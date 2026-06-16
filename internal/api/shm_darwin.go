//go:build darwin

package api

import (
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// macOS __ulock constants (private but stable since macOS 10.12)
const (
	ulockCompareAndWait = 1
	ulockWake           = 1
	ulfNoErrno          = 0x01000000
)

// SYS___ulock_wait and SYS___ulock_wake syscall numbers
const (
	sysUlockWait = 515
	sysUlockWake = 516
)

// platformShmOpen opens an existing POSIX shared memory region.
func platformShmOpen(name string, size int) ([]byte, error) {
	fd, err := unix.ShmOpen(name, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("shm_open %s: %w", name, err)
	}
	defer unix.Close(fd)

	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap shm: %w", err)
	}

	return data, nil
}

// platformShmClose unmaps a shared memory region.
func platformShmClose(data []byte) error {
	return unix.Munmap(data)
}

// platformFutexWait blocks until *addr != expected or timeout.
func platformFutexWait(addr *uint32, expected uint32, timeoutMs int) error {
	var timeoutUs uint32
	if timeoutMs >= 0 {
		timeoutUs = uint32(timeoutMs) * 1000
	}
	// timeout of 0 means infinite for __ulock_wait

	_, _, errno := syscall.Syscall6(
		uintptr(sysUlockWait),
		uintptr(ulockCompareAndWait|ulfNoErrno),
		uintptr(unsafe.Pointer(addr)),
		uintptr(expected),
		uintptr(timeoutUs),
		0, 0,
	)

	if errno != 0 {
		// With ULF_NO_ERRNO, errors are returned as negative return values
		// but Go's Syscall6 still reports them as errno
		if errno == syscall.ETIMEDOUT {
			return nil
		}
		if errno == syscall.EAGAIN || errno == syscall.EINTR {
			return nil
		}
		return fmt.Errorf("__ulock_wait errno: %v", errno)
	}
	return nil
}

// platformFutexWake wakes one waiter.
func platformFutexWake(addr *uint32) {
	syscall.Syscall6(
		uintptr(sysUlockWake),
		uintptr(ulockWake|ulfNoErrno),
		uintptr(unsafe.Pointer(addr)),
		0, 0, 0, 0,
	)
}
