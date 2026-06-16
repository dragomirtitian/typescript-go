//go:build windows

package api

import (
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modkernel32         = windows.NewLazySystemDLL("kernel32.dll")
	procOpenFileMapping = modkernel32.NewProc("OpenFileMappingW")
	procMapViewOfFile   = modkernel32.NewProc("MapViewOfFile")
	procUnmapViewOfFile = modkernel32.NewProc("UnmapViewOfFile")
)

const fileMapAllAccess = 0xF001F

// Windows shm state: holds the event handle for cross-process signaling.
var winShmEvent windows.Handle

// platformShmOpen opens an existing named file mapping and the associated
// named event for cross-process signaling.
func platformShmOpen(name string, size int) ([]byte, error) {
	mappingName := "Local\\" + windowsShmName(name)
	namePtr, err := syscall.UTF16PtrFromString(mappingName)
	if err != nil {
		return nil, err
	}

	handle, _, err := procOpenFileMapping.Call(
		uintptr(fileMapAllAccess),
		0,
		uintptr(unsafe.Pointer(namePtr)),
	)
	if handle == 0 {
		return nil, fmt.Errorf("OpenFileMappingW(%s): %w", mappingName, err)
	}
	defer windows.CloseHandle(windows.Handle(handle))

	ptr, _, err := procMapViewOfFile.Call(
		handle,
		uintptr(fileMapAllAccess),
		0, 0,
		uintptr(size),
	)
	if ptr == 0 {
		return nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	// Open the named auto-reset event (created by Node)
	evtName := "Local\\" + windowsShmName(name) + "-evt"
	evtNamePtr, err := syscall.UTF16PtrFromString(evtName)
	if err != nil {
		procUnmapViewOfFile.Call(ptr)
		return nil, err
	}

	evt, err := windows.OpenEvent(windows.EVENT_ALL_ACCESS, false, evtNamePtr)
	if err != nil {
		procUnmapViewOfFile.Call(ptr)
		return nil, fmt.Errorf("OpenEvent(%s): %w", evtName, err)
	}
	winShmEvent = evt

	data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), size)
	return data, nil
}

// platformShmClose unmaps the shared memory and closes the event.
func platformShmClose(data []byte) error {
	if winShmEvent != 0 {
		windows.CloseHandle(winShmEvent)
		winShmEvent = 0
	}
	if len(data) == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&data[0])
	ret, _, err := procUnmapViewOfFile.Call(uintptr(ptr))
	if ret == 0 {
		return fmt.Errorf("UnmapViewOfFile: %w", err)
	}
	return nil
}

// platformFutexWait waits on the named event (cross-process doorbell).
// The caller must re-check the state value after waking.
func platformFutexWait(addr *uint32, expected uint32, timeoutMs int) error {
	// Quick check: if value already changed, no need to wait
	if *addr != expected {
		return nil
	}

	var timeout uint32
	if timeoutMs >= 0 {
		timeout = uint32(timeoutMs)
	} else {
		timeout = windows.INFINITE
	}

	_, err := windows.WaitForSingleObject(winShmEvent, timeout)
	if err != nil && err != windows.ERROR_TIMEOUT {
		return fmt.Errorf("WaitForSingleObject: %w", err)
	}
	return nil
}

// platformFutexWake signals the named event to wake the other process.
func platformFutexWake(addr *uint32) {
	windows.SetEvent(winShmEvent)
}

// windowsShmName converts "/tsgo-shm-1234-5678" → "tsgo-shm-1234-5678"
func windowsShmName(name string) string {
	result := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			if i > 0 {
				result = append(result, '_')
			}
		} else {
			result = append(result, name[i])
		}
	}
	return string(result)
}
