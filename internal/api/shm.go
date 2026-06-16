package api

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"unsafe"

	"github.com/microsoft/typescript-go/internal/json"
	"github.com/microsoft/typescript-go/internal/jsonrpc"
)

// Shared memory layout constants
const (
	shmControlSize      = 64 // Control header size (cache-line aligned)
	shmOffsetState      = 0  // uint32: futex word
	shmOffsetReqLen     = 4  // uint32: request data length
	shmOffsetResLen     = 8  // uint32: response data length
	shmOffsetFlags      = 12 // uint32: flags (bit 0 = CONTINUE)
	shmOffsetSize       = 16 // uint32: total region size
	shmOffsetGoSpinning = 20 // uint32: 1 = Go is spinning (no wake needed)

	// State values for the futex word
	shmStateIdle          = 0
	shmStateRequestReady  = 1
	shmStateResponseReady = 2

	// Flag bits
	shmFlagContinue = 1 // More chunks follow
)

// ShmProtocol implements the Protocol interface using shared memory + futex
// signaling instead of pipe-based I/O. It reads/writes msgpack tuples directly
// from/to mmap'd memory regions.
type ShmProtocol struct {
	data     []byte // full mmap'd region
	dataSize uint32 // total region size

	// Regions within the shared memory
	reqRegion  []byte // request data region (Node writes, Go reads)
	respRegion []byte // response data region (Go writes, Node reads)
}

// NewShmProtocol creates a protocol handler backed by the given shared memory region.
// The region must already be mmap'd and match the expected layout.
func NewShmProtocol(data []byte) (*ShmProtocol, error) {
	if len(data) < shmControlSize+2 {
		return nil, fmt.Errorf("shm region too small: %d bytes", len(data))
	}

	totalSize := uint32(len(data))
	regionSize := (totalSize - uint32(shmControlSize)) / 2

	p := &ShmProtocol{
		data:       data,
		dataSize:   totalSize,
		reqRegion:  data[shmControlSize : shmControlSize+regionSize],
		respRegion: data[shmControlSize+regionSize:],
	}

	// Write region size into control header so Node can verify
	binary.LittleEndian.PutUint32(data[shmOffsetSize:], totalSize)

	return p, nil
}

// statePtr returns a pointer to the futex/state word for atomic operations.
func (p *ShmProtocol) statePtr() *uint32 {
	return (*uint32)(unsafe.Pointer(&p.data[shmOffsetState]))
}

// goSpinningPtr returns a pointer to the "Go is spinning" flag.
func (p *ShmProtocol) goSpinningPtr() *uint32 {
	return (*uint32)(unsafe.Pointer(&p.data[shmOffsetGoSpinning]))
}

// spinIterations is the number of spin-loop iterations before falling back
// to a futex sleep. Tuned for typical request latency (~1-5us on modern CPUs,
// ~10000 iterations ≈ 30us of spinning).
const spinIterations = 10000

// ReadMessage implements Protocol. It waits for the Node process to write a
// request into the shared memory request region, then parses it.
func (p *ShmProtocol) ReadMessage() (*Message, error) {
	// Mark that Go is spinning — Node skips the futexWake syscall when this is set.
	atomic.StoreUint32(p.goSpinningPtr(), 1)

	for i := 0; i < spinIterations; i++ {
		if atomic.LoadUint32(p.statePtr()) == shmStateRequestReady {
			goto ready
		}
	}

	// Spin window expired — clear the flag before sleeping so Node knows to wake us.
	atomic.StoreUint32(p.goSpinningPtr(), 0)

	for {
		state := atomic.LoadUint32(p.statePtr())
		if state == shmStateRequestReady {
			break
		}
		if err := platformFutexWait(p.statePtr(), state, -1); err != nil {
			return nil, fmt.Errorf("shm futex_wait: %w", err)
		}
	}
ready:

	// Clear state so subsequent ReadMessage calls (e.g., from Call() during
	// callback handling) don't immediately re-read stale data.
	atomic.StoreUint32(p.statePtr(), shmStateIdle)

	// Read request — zero-copy for single-chunk (common case).
	// The data in reqRegion is stable until we write the response.
	reqLen := binary.LittleEndian.Uint32(p.data[shmOffsetReqLen:])
	flags := binary.LittleEndian.Uint32(p.data[shmOffsetFlags:])

	var msgType MessageType
	var method string
	var payload []byte
	var err error

	if flags&shmFlagContinue == 0 {
		// Single chunk: parse directly from the region.
		// Method name is copied (converted to string). Payload slice points
		// into reqRegion, so we must copy it — callback responses can overwrite
		// reqRegion while the handler is still processing the original params.
		msgType, method, payload, err = parseMsgpackTuple(p.reqRegion[:reqLen])
		if err == nil && len(payload) > 0 {
			payloadCopy := make([]byte, len(payload))
			copy(payloadCopy, payload)
			payload = payloadCopy
		}
	} else {
		// Multi-chunk: must copy (rare, only for oversized requests)
		reqData, readErr := p.readChunked(true)
		if readErr != nil {
			return nil, readErr
		}
		msgType, method, payload, err = parseMsgpackTuple(reqData)
	}
	if err != nil {
		return nil, err
	}

	// Convert to Message (same logic as MessagePackProtocol.ReadMessage)
	msg := &Message{}
	switch msgType {
	case MessageTypeRequest:
		id := jsonrpc.NewIDString(method)
		msg.ID = id
		msg.Method = method
		msg.Params = payload
	case MessageTypeCallResponse:
		id := jsonrpc.NewIDString(method)
		msg.ID = id
		msg.Result = payload
	case MessageTypeCallError:
		id := jsonrpc.NewIDString(method)
		msg.ID = id
		msg.Error = &jsonrpc.ResponseError{
			Code:    jsonrpc.CodeInternalError,
			Message: string(payload),
		}
	default:
		return nil, fmt.Errorf("unexpected message type: %d", msgType)
	}

	return msg, nil
}

// WriteResponse implements Protocol.
func (p *ShmProtocol) WriteResponse(id *jsonrpc.ID, result any) error {
	method := ""
	if id != nil {
		method = id.String()
	}

	var payload []byte
	var err error
	if raw, ok := result.(RawBinary); ok {
		payload = []byte(raw)
	} else {
		payload, err = json.Marshal(result)
		if err != nil {
			return err
		}
	}

	return p.writeAndSignal(MessageTypeResponse, method, payload)
}

// WriteError implements Protocol.
func (p *ShmProtocol) WriteError(id *jsonrpc.ID, respErr *jsonrpc.ResponseError) error {
	method := ""
	if id != nil {
		method = id.String()
	}
	return p.writeAndSignal(MessageTypeError, method, []byte(respErr.Message))
}

// WriteRequest implements Protocol (used for callbacks Go→Node).
func (p *ShmProtocol) WriteRequest(id *jsonrpc.ID, method string, params any) error {
	payload, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return p.writeAndSignal(MessageTypeCall, method, payload)
}

// WriteNotification implements Protocol.
func (p *ShmProtocol) WriteNotification(method string, params any) error {
	return p.WriteRequest(nil, method, params)
}

// writeAndSignal encodes a msgpack tuple into the response region and signals Node.
// For single-chunk responses (common case), it encodes directly into respRegion
// without allocating an intermediate buffer, and skips futexWake since Node is spin-waiting.
func (p *ShmProtocol) writeAndSignal(msgType MessageType, method string, payload []byte) error {
	methodBytes := []byte(method)
	headerSize := 2 + binHeaderSize(len(methodBytes)) + len(methodBytes) + binHeaderSize(len(payload))
	totalSize := headerSize + len(payload)

	if totalSize <= len(p.respRegion) {
		// Fast path: encode directly into respRegion — zero allocation
		off := 0
		p.respRegion[off] = msgpackFixedArray3
		off++
		p.respRegion[off] = byte(msgType)
		off++
		off = writeBinHeaderInto(p.respRegion, off, len(methodBytes))
		copy(p.respRegion[off:], methodBytes)
		off += len(methodBytes)
		off = writeBinHeaderInto(p.respRegion, off, len(payload))
		copy(p.respRegion[off:], payload)

		// Signal RESPONSE_READY — no futexWake needed, Node is spin-waiting
		binary.LittleEndian.PutUint32(p.data[shmOffsetResLen:], uint32(totalSize))
		binary.LittleEndian.PutUint32(p.data[shmOffsetFlags:], 0)
		atomic.StoreUint32(p.statePtr(), shmStateResponseReady)
		return nil
	}

	// Slow path: oversized response, use chunked transfer
	tuple := encodeMsgpackTuple(msgType, method, payload)
	return p.writeChunked(tuple)
}

// writeBinHeaderInto writes a msgpack bin header into buf at off, returns new offset.
func writeBinHeaderInto(buf []byte, off int, length int) int {
	if length < 256 {
		buf[off] = msgpackBin8
		buf[off+1] = byte(length)
		return off + 2
	}
	if length < 1<<16 {
		buf[off] = msgpackBin16
		buf[off+1] = byte(length >> 8)
		buf[off+2] = byte(length)
		return off + 3
	}
	buf[off] = msgpackBin32
	buf[off+1] = byte(length >> 24)
	buf[off+2] = byte(length >> 16)
	buf[off+3] = byte(length >> 8)
	buf[off+4] = byte(length)
	return off + 5
}

// writeChunked writes data to the response region, chunking if necessary.
func (p *ShmProtocol) writeChunked(data []byte) error {
	regionSize := len(p.respRegion)
	offset := 0

	for offset < len(data) {
		chunkSize := len(data) - offset
		hasMore := false
		if chunkSize > regionSize {
			chunkSize = regionSize
			hasMore = true
		}

		// Copy chunk into response region
		copy(p.respRegion[:chunkSize], data[offset:offset+chunkSize])

		// Set response length and flags
		binary.LittleEndian.PutUint32(p.data[shmOffsetResLen:], uint32(chunkSize))
		flags := uint32(0)
		if hasMore {
			flags |= shmFlagContinue
		}
		binary.LittleEndian.PutUint32(p.data[shmOffsetFlags:], flags)

		// Signal RESPONSE_READY
		atomic.StoreUint32(p.statePtr(), shmStateResponseReady)
		platformFutexWake(p.statePtr())

		if hasMore {
			// Wait for Node to acknowledge (set state back to IDLE)
			for {
				state := atomic.LoadUint32(p.statePtr())
				if state == shmStateIdle {
					break
				}
				if err := platformFutexWait(p.statePtr(), state, -1); err != nil {
					return fmt.Errorf("shm futex_wait (continue): %w", err)
				}
			}
		}

		offset += chunkSize
	}

	return nil
}

// readChunked reads data from the request region, handling continuation.
// isFirstChunk should be true for the initial read (state is already REQUEST_READY).
func (p *ShmProtocol) readChunked(isFirstChunk bool) ([]byte, error) {
	if !isFirstChunk {
		// Wait for REQUEST_READY
		for {
			state := atomic.LoadUint32(p.statePtr())
			if state == shmStateRequestReady {
				break
			}
			if err := platformFutexWait(p.statePtr(), state, -1); err != nil {
				return nil, fmt.Errorf("shm futex_wait (read continue): %w", err)
			}
		}
	}

	reqLen := binary.LittleEndian.Uint32(p.data[shmOffsetReqLen:])
	flags := binary.LittleEndian.Uint32(p.data[shmOffsetFlags:])

	if reqLen > uint32(len(p.reqRegion)) {
		return nil, fmt.Errorf("shm request length %d exceeds region size %d", reqLen, len(p.reqRegion))
	}

	// Copy first chunk
	result := make([]byte, reqLen)
	copy(result, p.reqRegion[:reqLen])

	// Handle continuation
	if flags&shmFlagContinue != 0 {
		// Acknowledge: set state to IDLE so Node sends next chunk
		atomic.StoreUint32(p.statePtr(), shmStateIdle)
		platformFutexWake(p.statePtr())

		// Read remaining chunks
		rest, err := p.readChunked(false)
		if err != nil {
			return nil, err
		}
		result = append(result, rest...)
	}

	return result, nil
}

// parseMsgpackTuple parses a [type, method, payload] tuple from raw bytes.
func parseMsgpackTuple(data []byte) (MessageType, string, []byte, error) {
	if len(data) < 3 {
		return 0, "", nil, fmt.Errorf("%w: shm tuple too short", ErrInvalidRequest)
	}

	pos := 0

	// Fixed 3-element array marker
	if data[pos] != msgpackFixedArray3 {
		return 0, "", nil, fmt.Errorf("%w: expected 0x93, got 0x%02x", ErrInvalidRequest, data[pos])
	}
	pos++

	// Message type
	var msgType MessageType
	if data[pos] <= 0x7F {
		msgType = MessageType(data[pos])
		pos++
	} else if data[pos] == msgpackU8 {
		pos++
		if pos >= len(data) {
			return 0, "", nil, io.ErrUnexpectedEOF
		}
		msgType = MessageType(data[pos])
		pos++
	} else {
		return 0, "", nil, fmt.Errorf("%w: unexpected type byte 0x%02x", ErrInvalidRequest, data[pos])
	}

	if !msgType.IsValid() {
		return 0, "", nil, fmt.Errorf("%w: unknown message type %d", ErrInvalidRequest, msgType)
	}

	// Read method (bin)
	methodBytes, newPos, err := parseBin(data, pos)
	if err != nil {
		return 0, "", nil, err
	}
	pos = newPos

	// Read payload (bin)
	payload, _, err := parseBin(data, pos)
	if err != nil {
		return 0, "", nil, err
	}

	return msgType, string(methodBytes), payload, nil
}

// parseBin reads a msgpack bin field from data at the given position.
func parseBin(data []byte, pos int) ([]byte, int, error) {
	if pos >= len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}

	marker := data[pos]
	pos++

	var size uint32
	switch marker {
	case msgpackBin8:
		if pos >= len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		size = uint32(data[pos])
		pos++
	case msgpackBin16:
		if pos+2 > len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		size = uint32(binary.BigEndian.Uint16(data[pos:]))
		pos += 2
	case msgpackBin32:
		if pos+4 > len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		size = binary.BigEndian.Uint32(data[pos:])
		pos += 4
	default:
		return nil, 0, fmt.Errorf("%w: expected bin marker, got 0x%02x", ErrInvalidRequest, marker)
	}

	end := pos + int(size)
	if end > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}

	return data[pos:end], end, nil
}

// encodeMsgpackTuple encodes a [type, method, payload] tuple into bytes.
func encodeMsgpackTuple(msgType MessageType, method string, payload []byte) []byte {
	methodBytes := []byte(method)
	// Calculate total size
	size := 1 + 1 + binHeaderSize(len(methodBytes)) + len(methodBytes) + binHeaderSize(len(payload)) + len(payload)

	buf := make([]byte, 0, size)
	buf = append(buf, msgpackFixedArray3)
	buf = append(buf, byte(msgType))
	buf = appendBinHeader(buf, len(methodBytes))
	buf = append(buf, methodBytes...)
	buf = appendBinHeader(buf, len(payload))
	buf = append(buf, payload...)
	return buf
}

func binHeaderSize(length int) int {
	if length < 256 {
		return 2
	}
	if length < 1<<16 {
		return 3
	}
	return 5
}

func appendBinHeader(buf []byte, length int) []byte {
	if length < 256 {
		return append(buf, msgpackBin8, byte(length))
	}
	if length < 1<<16 {
		buf = append(buf, msgpackBin16)
		buf = append(buf, byte(length>>8), byte(length))
		return buf
	}
	buf = append(buf, msgpackBin32)
	buf = append(buf, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	return buf
}

// Verify ShmProtocol implements Protocol
var _ Protocol = (*ShmProtocol)(nil)
