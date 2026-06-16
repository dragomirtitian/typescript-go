import * as fs from "node:fs";
import * as module from "node:module";
import * as path from "node:path";

import { fsCallbackNames } from "../fs.ts";
import {
    type ClientOptions,
    type ClientSocketOptions,
    type ClientSpawnOptions,
    isSpawnOptions,
    resolveExePath,
} from "../options.ts";
import { type ShmAddon, ShmRpcChannel } from "../shmChannel.ts";
import { SyncRpcChannel } from "../syncChannel.ts";

export type { ClientOptions, ClientSocketOptions, ClientSpawnOptions };

/** Try to load the shared memory native addon from the same directory as the tsgo exe.
 *  Set TS_API_TRANSPORT=pipe to force pipe-based channel (for benchmarking). */
function tryLoadShmAddon(exePath: string): ShmAddon | null {
    if (process.env["TS_API_TRANSPORT"] === "pipe") {
        return null;
    }
    try {
        const addonPath = path.join(path.dirname(exePath), "tsgo-shm.node");
        if (!fs.existsSync(addonPath)) {
            return null;
        }
        const require2 = module.createRequire(import.meta.url ?? __filename);
        return require2(addonPath);
    }
    catch(e) {
        return null;
    }
}

interface RpcChannel {
    requestSync(method: string, payload: string): string;
    requestBinarySync(method: string, payload: Uint8Array): Uint8Array;
    registerCallback(name: string, callback: (name: string, payload: string) => string): void;
    close(): void;
}

export class Client {
    private channel: RpcChannel;
    private encoder = new TextEncoder();

    constructor(options: ClientOptions) {
        if (!isSpawnOptions(options)) {
            throw new Error("Socket connections are not yet supported in the sync client");
        }

        const cwd = options.cwd ?? process.cwd();
        const args = [
            "--api",
            "--cwd",
            cwd,
        ];

        // Enable virtual FS callbacks for each provided FS function
        const enabledCallbacks: (typeof fsCallbackNames[number])[] = [];
        if (options.fs) {
            for (const name of fsCallbackNames) {
                if (options.fs[name]) {
                    enabledCallbacks.push(name);
                }
            }
        }
        if (enabledCallbacks.length > 0) {
            args.push(`--callbacks=${enabledCallbacks.join(",")}`);
        }

        const exe = resolveExePath(options);

        // Try shared memory transport, fall back to pipe-based transport
        const addon = tryLoadShmAddon(exe);
        let channel: RpcChannel;
        if (addon) {
            const shmName = `/tsgo-shm-${process.pid}-${Date.now()}`;
            const shmSize = 64 * 1024 * 1024; // 64MB
            const shmBuffer = addon.createSharedMemory(shmName, shmSize);
            channel = new ShmRpcChannel(exe, [...args, "--shm", shmName], shmBuffer, shmSize, shmName, addon);
        }
        else {
            channel = new SyncRpcChannel(exe, args);
        }
        this.channel = channel;

        if (options.fs) {
            for (const name of enabledCallbacks) {
                const callback = options.fs[name]!;
                channel.registerCallback(name, (_, arg) => {
                    const result = callback(JSON.parse(arg));
                    if (name === "readFile") {
                        // readFile has 3 returns: string (content), null (not found), undefined (fall back).
                        // Wrap in object to preserve null vs undefined distinction.
                        if (result === undefined) return "";
                        return JSON.stringify({ content: result });
                    }
                    return JSON.stringify(result) ?? "";
                });
            }
        }
    }

    apiRequest<T>(method: string, params?: unknown): T {
        const encodedPayload = JSON.stringify(params);
        const result = this.channel.requestSync(method, encodedPayload);
        if (result.length) {
            return JSON.parse(result) as T;
        }
        return undefined as unknown as T;
    }

    apiRequestBinary(method: string, params?: unknown): Uint8Array | undefined {
        const result = this.channel.requestBinarySync(method, this.encoder.encode(JSON.stringify(params)));
        if (result.length === 0) return undefined;
        return result;
    }

    echo(payload: string): string {
        return this.channel.requestSync("echo", payload);
    }

    echoBinary(payload: Uint8Array): Uint8Array {
        return this.channel.requestBinarySync("echo", payload);
    }

    close(): void {
        this.channel.close();
    }
}
