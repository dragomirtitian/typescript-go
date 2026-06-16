/**
 * Shared-memory based RPC channel.
 *
 * Replaces pipe-based synchronous I/O (SyncRpcChannel) with shared memory +
 * futex signaling for dramatically lower latency per round-trip (~1-2us vs
 * ~10-50us for pipes) and zero-copy for large payloads.
 *
 * Memory layout:
 *   [0..63]    Control header (cache-line aligned)
 *   [64..N]    Request region (Node writes, Go reads)
 *   [N..2N]    Response region (Go writes, Node reads)
 *
 * Control header:
 *   offset 0:  uint32 state (futex word): 0=IDLE, 1=REQUEST_READY, 2=RESPONSE_READY
 *   offset 4:  uint32 request data length
 *   offset 8:  uint32 response data length
 *   offset 12: uint32 flags (bit 0 = CONTINUE)
 *   offset 16: uint32 region size
 */

import { type ChildProcess, spawn } from "node:child_process";
import {
    binHeaderSize,
    MSGPACK_BIN16,
    MSGPACK_BIN32,
    MSGPACK_BIN8,
    MSGPACK_FIXARRAY3,
    MSGPACK_UINT8,
    writeBinHeader,
} from "./node/msgpack.ts";

// ── State constants ─────────────────────────────────────────────────
const STATE_IDLE = 0;
const STATE_REQUEST_READY = 1;
const STATE_RESPONSE_READY = 2;

// ── Control header offsets (bytes) ──────────────────────────────────
const OFFSET_STATE = 0;
const OFFSET_REQ_LEN = 4;
const OFFSET_RES_LEN = 8;
const OFFSET_FLAGS = 12;
const OFFSET_SIZE = 16;
const OFFSET_GO_SPINNING = 20;
const CONTROL_SIZE = 64;

// ── Flags ───────────────────────────────────────────────────────────
const FLAG_CONTINUE = 1;

// ── Message types (same as syncChannel.ts) ──────────────────────────
const MSG_REQUEST = 1;
const MSG_CALL_RESPONSE = 2;
const MSG_CALL_ERROR = 3;
const MSG_RESPONSE = 4;
const MSG_ERROR = 5;
const MSG_CALL = 6;

// Shared empty buffer
const EMPTY_BUF = Buffer.alloc(0);

/** Interface for the native shared memory addon */
export interface ShmAddon {
    createSharedMemory(name: string, size: number): Buffer;
    destroySharedMemory(name: string): void;
    futexWait(name: string, byteOffset: number, expected: number, timeoutMs: number): boolean;
    futexWake(name: string, byteOffset: number): number;
    spinWaitFor(name: string, byteOffset: number, expected: number): number;
    spinWaitUntilChanged(name: string, byteOffset: number, current: number): number;
}

// ── Global cleanup tracking ─────────────────────────────────────────
const liveRegions = new Map<string, { addon: ShmAddon; child?: ChildProcess }>();

process.on("exit", () => {
    for (const [name, { addon, child }] of liveRegions) {
        try {
            child?.kill();
        }
        catch {}
        try {
            addon.destroySharedMemory(name);
        }
        catch {}
    }
    liveRegions.clear();
});

/**
 * ShmRpcChannel — shared-memory based drop-in replacement for SyncRpcChannel.
 *
 * Same public API:
 *   - constructor(exe, args, shmBuffer, shmSize, addon)
 *   - requestSync(method, payload): string
 *   - requestBinarySync(method, payload): Uint8Array
 *   - registerCallback(name, cb)
 *   - close()
 */
export class ShmRpcChannel {
    private child: ChildProcess;
    private shmName: string;
    private addon: ShmAddon;
    private callbacks = new Map<string, (name: string, payload: string) => string>();
    private methodBufCache = new Map<string, Buffer>();

    private controlView: DataView;
    private reqRegion: Buffer;
    private respRegion: Buffer;
    private regionSize: number;

    private _msgType = 0;
    private _msgName: Buffer = EMPTY_BUF;
    private _msgPayload: Buffer = EMPTY_BUF;

    constructor(exe: string, args: string[], shmBuffer: Buffer, shmSize: number, shmName: string, addon: ShmAddon) {
        this.shmName = shmName;
        this.addon = addon;

        // Set up region views
        this.regionSize = Math.floor((shmSize - CONTROL_SIZE) / 2);
        this.controlView = new DataView(shmBuffer.buffer, shmBuffer.byteOffset, CONTROL_SIZE);
        this.reqRegion = shmBuffer.subarray(CONTROL_SIZE, CONTROL_SIZE + this.regionSize);
        this.respRegion = shmBuffer.subarray(CONTROL_SIZE + this.regionSize);

        // Write region size into control header
        this.controlView.setUint32(OFFSET_SIZE, shmSize, true);

        if(process.env["TS_API_DEBUG"]) {
            this.child = spawn("dlv", ["exec", exe, "--headless", "--listen=:2345", "--api-version=2",  "--log-dest=/dev/null",  "--", ...args], {
                stdio: ["ignore", "ignore", "inherit"],
            });
        } else {
            this.child = spawn(exe, args, {
                stdio: ["ignore", "ignore", "inherit"],
            });
        }

        liveRegions.set(shmName, { addon, child: this.child });
        this.child.unref();
    }

    // ── Public API ──────────────────────────────────────────────────

    requestSync(method: string, payload: string): string {
        this.ensureOpen();
        const result = this.requestBytesSync(method, payload);
        return result.toString("utf-8");
    }

    requestBinarySync(method: string, payload: Uint8Array): Uint8Array {
        this.ensureOpen();
        const view = this.requestBytesSync(method, payload);
        // Must copy — the view points into respRegion which gets overwritten
        // by the next request. Callers (e.g. RemoteSourceFile) hold the result
        // for the lifetime of the source file.
        return Buffer.from(view);
    }

    registerCallback(name: string, callback: (name: string, payload: string) => string): void {
        this.callbacks.set(name, callback);
    }

    close(): void {
        try {
            liveRegions.delete(this.shmName);
            this.child.kill();
            this.addon.destroySharedMemory(this.shmName);
        }
        catch {}
    }

    // ── Core request loop ───────────────────────────────────────────

    private ensureOpen(): void {
        if (this.child.exitCode !== null) {
            throw new Error("ShmRpcChannel: child process has exited");
        }
    }

    private getMethodBuf(method: string): Buffer {
        let buf = this.methodBufCache.get(method);
        if (buf === undefined) {
            buf = Buffer.from(method, "utf-8");
            this.methodBufCache.set(method, buf);
        }
        return buf;
    }

    private requestBytesSync(method: string, payload: Buffer | Uint8Array | string): Buffer {
        const methodBuf = this.getMethodBuf(method);
        this.writeTuple(MSG_REQUEST, methodBuf, payload);

        for (;;) {
            this.readTuple();

            switch (this._msgType) {
                case MSG_RESPONSE: {
                    if (!methodBuf.equals(this._msgName)) {
                        throw new Error(
                            `name mismatch: expected \`${method}\`, got \`${this._msgName.toString("utf-8")}\``,
                        );
                    }
                    return this._msgPayload;
                }
                case MSG_ERROR: {
                    if (methodBuf.equals(this._msgName)) {
                        throw new Error(this._msgPayload.toString("utf-8"));
                    }
                    throw new Error(
                        `name mismatch: expected \`${method}\`, got \`${this._msgName.toString("utf-8")}\``,
                    );
                }
                case MSG_CALL: {
                    this.handleCall(this._msgName.toString("utf-8"), this._msgPayload);
                    break;
                }
                default:
                    throw new Error(`Invalid message type from child: ${this._msgType}`);
            }
        }
    }

    // ── Callback handling ───────────────────────────────────────────

    private handleCall(name: string, payload: Buffer): void {
        const cb = this.callbacks.get(name);
        const nameBuf = this.getMethodBuf(name);
        if (!cb) {
            const errMsg = `unknown callback: \`${name}\``;
            this.writeTuple(MSG_CALL_ERROR, nameBuf, errMsg);
            throw new Error(`no callback named \`${name}\` found`);
        }

        try {
            const result = cb(name, payload.toString("utf-8"));
            // Pass result as string — writeTuple encodes directly into reqRegion
            // without an intermediate Buffer.from() allocation.
            this.writeTuple(MSG_CALL_RESPONSE, nameBuf, result);
        }
        catch (e: unknown) {
            const errMsg = String(e instanceof Error ? e.message : e).trim();
            this.writeTuple(MSG_CALL_ERROR, nameBuf, errMsg);
            throw new Error(`Error calling callback \`${name}\`: ${errMsg}`);
        }
    }

    // ── Write: encode msgpack tuple into request region ─────────────

    private writeTuple(type: number, name: Buffer, payload: Buffer | Uint8Array | string): void {
        const nameLen = name.length;
        const payloadIsString = typeof payload === "string";
        const payloadLen = payloadIsString ? Buffer.byteLength(payload, "utf-8") : payload.length;
        const nameHdrSize = binHeaderSize(nameLen);
        const payloadHdrSize = binHeaderSize(payloadLen);
        const totalSize = 2 + nameHdrSize + nameLen + payloadHdrSize + payloadLen;

        // Encode the msgpack tuple
        const msgBuf = totalSize <= this.regionSize ? this.reqRegion : Buffer.allocUnsafe(totalSize);
        let off = 0;
        msgBuf[off++] = MSGPACK_FIXARRAY3;
        msgBuf[off++] = type;
        off = writeBinHeader(msgBuf, off, nameLen);
        name.copy(msgBuf, off);
        off += nameLen;
        off = writeBinHeader(msgBuf, off, payloadLen);
        if (payloadLen > 0) {
            if (payloadIsString) {
                msgBuf.write(payload, off, payloadLen, "utf-8");
            }
            else if (payload instanceof Buffer) {
                payload.copy(msgBuf, off);
            }
            else {
                msgBuf.set(payload, off);
            }
        }

        // Write to shared memory (chunked if needed)
        this.writeChunked(msgBuf, totalSize);
    }

    private writeChunked(data: Buffer, totalLen: number): void {
        let offset = 0;

        while (offset < totalLen) {
            const remaining = totalLen - offset;
            const chunkSize = Math.min(remaining, this.regionSize);
            const hasMore = remaining > this.regionSize;

            // Copy chunk into request region (skip if data IS reqRegion and offset is 0)
            if (data !== this.reqRegion || offset > 0) {
                data.copy(this.reqRegion, 0, offset, offset + chunkSize);
            }

            // Set length and flags
            this.controlView.setUint32(OFFSET_REQ_LEN, chunkSize, true);
            this.controlView.setUint32(OFFSET_FLAGS, hasMore ? FLAG_CONTINUE : 0, true);

            // Signal REQUEST_READY
            this.controlView.setUint32(OFFSET_STATE, STATE_REQUEST_READY, true);
            // Only call futexWake if Go is not spinning (i.e., it's in futex sleep)
            if (this.controlView.getUint32(OFFSET_GO_SPINNING, true) === 0) {
                this.addon.futexWake(this.shmName, OFFSET_STATE);
            }

            if (hasMore) {
                // Spin-wait (native, with CPU pause hints) for Go to acknowledge
                this.addon.spinWaitFor(this.shmName, OFFSET_STATE, STATE_IDLE);
            }

            offset += chunkSize;
        }
    }

    // ── Read: wait for response, decode msgpack tuple ───────────────

    private readTuple(): void {
        // Spin-wait (native, with CPU pause hints) for RESPONSE_READY
        this.addon.spinWaitFor(this.shmName, OFFSET_STATE, STATE_RESPONSE_READY);

        const respLen = this.controlView.getUint32(OFFSET_RES_LEN, true);
        const flags = this.controlView.getUint32(OFFSET_FLAGS, true);

        if (flags & FLAG_CONTINUE) {
            // Multi-chunk: must copy and accumulate (rare path)
            const data = this.readChunkedResponse(respLen);
            this.parseTupleFrom(data);
            return;
        }

        // Single-chunk (common case): parse directly from respRegion — zero-copy.
        // Data is stable until we send the next request.
        this.parseTupleFrom(this.respRegion);
    }

    private parseTupleFrom(data: Buffer | Uint8Array): void {
        let pos = 0;

        if (data[pos] !== MSGPACK_FIXARRAY3) {
            throw new Error(`Expected 0x93, got 0x${data[pos].toString(16)}`);
        }
        pos++;

        const tb = data[pos++];
        if (tb <= 0x7f) {
            this._msgType = tb;
        }
        else if (tb === MSGPACK_UINT8) {
            this._msgType = data[pos++];
        }
        else {
            throw new Error(`Expected fixint or uint8, got 0x${tb.toString(16)}`);
        }

        [this._msgName, pos] = this.readBinFrom(data, pos);
        [this._msgPayload, pos] = this.readBinFrom(data, pos);
    }

    private readBinFrom(data: Buffer | Uint8Array, pos: number): [Buffer, number] {
        const marker = data[pos++];
        let size: number;
        switch (marker) {
            case MSGPACK_BIN8:
                size = data[pos++];
                break;
            case MSGPACK_BIN16:
                size = (data[pos] << 8) | data[pos + 1];
                pos += 2;
                break;
            case MSGPACK_BIN32:
                size = (data[pos] << 24) | (data[pos + 1] << 16) | (data[pos + 2] << 8) | data[pos + 3];
                pos += 4;
                break;
            default:
                throw new Error(`Expected bin marker, got 0x${marker.toString(16)}`);
        }
        if (size === 0) return [EMPTY_BUF, pos];
        const result = Buffer.from(data.subarray(pos, pos + size));
        return [result, pos + size];
    }

    private readChunkedResponse(firstChunkLen: number): Buffer {
        const chunks: Buffer[] = [];
        chunks.push(Buffer.from(this.respRegion.subarray(0, firstChunkLen)));

        let hasMore = true;
        while (hasMore) {
            this.controlView.setUint32(OFFSET_STATE, STATE_IDLE, true);
            if (this.controlView.getUint32(OFFSET_GO_SPINNING, true) === 0) {
                this.addon.futexWake(this.shmName, OFFSET_STATE);
            }
            this.addon.spinWaitFor(this.shmName, OFFSET_STATE, STATE_RESPONSE_READY);
            const chunkLen = this.controlView.getUint32(OFFSET_RES_LEN, true);
            const chunkFlags = this.controlView.getUint32(OFFSET_FLAGS, true);
            chunks.push(Buffer.from(this.respRegion.subarray(0, chunkLen)));
            hasMore = !!(chunkFlags & FLAG_CONTINUE);
        }
        return Buffer.concat(chunks);
    }

}
