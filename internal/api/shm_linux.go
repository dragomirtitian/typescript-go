//go:build linux

package api

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const shmDirectory = "/dev/shm"

// Linux futex operation constants (not exported by x/sys/unix).
// We intentionally do NOT use FUTEX_PRIVATE_FLAG here because
// the futex word lives in cross-process shared memory.
const (
	futexWait = 0
	futexWake = 1
)

// platformShmOpen opens an existing shared memory region by name.
// The region must already exist (created by Node).
func platformShmOpen(name string, size int) ([]byte, error) {
	// On Linux, POSIX shared memory is backed by files in /dev/shm/
	// The name starts with "/" per POSIX convention
	path := shmDirectory + name

	fd, err := unix.Open(path, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open shm %s: %w", path, err)
	}
	defer unix.Close(fd)

	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap shm: %w", err)
	}

	return data, nil
}

// platformShmClose unmaps a shared memory region (does not unlink).
func platformShmClose(data []byte) error {
	return unix.Munmap(data)
}

// platformFutexWait blocks until *addr != expected or timeout.
// timeout < 0 means wait indefinitely.
func platformFutexWait(addr *uint32, expected uint32, timeoutMs int) error {
	var ts *unix.Timespec
	if timeoutMs >= 0 {
		t := unix.NsecToTimespec(int64(time.Duration(timeoutMs) * time.Millisecond))
		ts = &t
	}

	_, _, errno := syscall.Syscall6(
		unix.SYS_FUTEX,
		uintptr(unsafe.Pointer(addr)),
		uintptr(futexWait),
		uintptr(expected),
		uintptr(unsafe.Pointer(ts)),
		0, 0,
	)

	if errno != 0 && errno != syscall.EAGAIN && errno != syscall.EINTR {
		if errno == syscall.ETIMEDOUT {
			return nil // timeout is not an error
		}
		return fmt.Errorf("futex_wait errno: %v", errno)
	}
	return nil
}

// platformFutexWake wakes one waiter on the futex word.
func platformFutexWake(addr *uint32) {
	syscall.Syscall6(
		unix.SYS_FUTEX,
		uintptr(unsafe.Pointer(addr)),
		uintptr(futexWake),
		1, // wake at most 1
		0, 0, 0,
	)
}
