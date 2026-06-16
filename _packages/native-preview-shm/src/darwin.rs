use napi::Result;
use std::ffi::CString;

pub fn create_shm(name: &str, size: usize) -> Result<*mut u8> {
    let c_name = CString::new(name).map_err(|e| napi::Error::from_reason(e.to_string()))?;

    unsafe {
        let fd = libc::shm_open(
            c_name.as_ptr(),
            libc::O_CREAT | libc::O_RDWR,
            0o600,
        );
        if fd < 0 {
            return Err(napi::Error::from_reason(format!(
                "shm_open failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        if libc::ftruncate(fd, size as libc::off_t) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            libc::shm_unlink(c_name.as_ptr());
            return Err(napi::Error::from_reason(format!("ftruncate failed: {}", err)));
        }

        let ptr = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );

        libc::close(fd);

        if ptr == libc::MAP_FAILED {
            libc::shm_unlink(c_name.as_ptr());
            return Err(napi::Error::from_reason(format!(
                "mmap failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        Ok(ptr as *mut u8)
    }
}

pub fn destroy_shm(name: &str, ptr: *mut u8, size: usize) -> Result<()> {
    let c_name = CString::new(name).map_err(|e| napi::Error::from_reason(e.to_string()))?;
    unsafe {
        libc::munmap(ptr as *mut libc::c_void, size);
        libc::shm_unlink(c_name.as_ptr());
    }
    Ok(())
}

// macOS __ulock syscall numbers (private but stable, used by libdispatch)
const UL_COMPARE_AND_WAIT: u32 = 1;
const UL_WAKE: u32 = 1;
const ULF_WAKE_ALL: u32 = 0x00000100;
const ULF_NO_ERRNO: u32 = 0x01000000;

// __ulock_wait and __ulock_wake are available since macOS 10.12
extern "C" {
    fn __ulock_wait(operation: u32, addr: *const u32, value: u64, timeout_us: u32) -> i32;
    fn __ulock_wake(operation: u32, addr: *const u32, wake_value: u64) -> i32;
}

pub fn futex_wait(addr: *const u32, expected: u32, timeout_ms: i32) -> Result<bool> {
    let timeout_us = if timeout_ms >= 0 {
        (timeout_ms as u32) * 1000
    } else {
        0 // 0 means infinite for __ulock_wait
    };

    let ret = unsafe {
        __ulock_wait(
            UL_COMPARE_AND_WAIT | ULF_NO_ERRNO,
            addr,
            expected as u64,
            timeout_us,
        )
    };

    if ret < 0 {
        let errno = -ret;
        if errno == libc::ETIMEDOUT {
            return Ok(false);
        }
        // EFAULT/EAGAIN means value already changed — normal wakeup
        if errno == libc::EAGAIN || errno == libc::EINTR {
            return Ok(true);
        }
        return Err(napi::Error::from_reason(format!(
            "__ulock_wait failed: {}",
            std::io::Error::from_raw_os_error(errno)
        )));
    }
    Ok(true)
}

pub fn futex_wake(addr: *const u32) -> Result<u32> {
    let ret = unsafe {
        __ulock_wake(UL_WAKE | ULF_NO_ERRNO, addr, 0)
    };

    if ret < 0 {
        let errno = -ret;
        // ENOENT means no waiters — not an error
        if errno == libc::ENOENT {
            return Ok(0);
        }
        return Err(napi::Error::from_reason(format!(
            "__ulock_wake failed: {}",
            std::io::Error::from_raw_os_error(errno)
        )));
    }
    Ok(1)
}
