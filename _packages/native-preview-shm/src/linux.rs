use napi::Result;
use std::ffi::CString;

pub fn create_shm(name: &str, size: usize) -> Result<*mut u8> {
    let c_name = CString::new(name).map_err(|e| napi::Error::from_reason(e.to_string()))?;

    unsafe {
        // Create or open the shared memory object
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

        // Set the size
        if libc::ftruncate(fd, size as libc::off_t) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            libc::shm_unlink(c_name.as_ptr());
            return Err(napi::Error::from_reason(format!("ftruncate failed: {}", err)));
        }

        // Map it into our address space
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

pub fn futex_wait(addr: *const u32, expected: u32, timeout_ms: i32) -> Result<bool> {
    unsafe {
        let ts;
        let ts_ptr = if timeout_ms >= 0 {
            ts = libc::timespec {
                tv_sec: (timeout_ms / 1000) as libc::time_t,
                tv_nsec: ((timeout_ms % 1000) as libc::c_long) * 1_000_000,
            };
            &ts as *const libc::timespec
        } else {
            std::ptr::null()
        };

        let ret = libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAIT,
            expected,
            ts_ptr,
            std::ptr::null::<u32>(),
            0u32,
        );

        if ret == -1 {
            let errno = *libc::__errno_location();
            if errno == libc::ETIMEDOUT {
                return Ok(false);
            }
            // EAGAIN means value changed before we slept — that's a normal wakeup
            if errno == libc::EAGAIN {
                return Ok(true);
            }
            if errno == libc::EINTR {
                return Ok(true);
            }
            return Err(napi::Error::from_reason(format!(
                "futex_wait failed: {}",
                std::io::Error::from_raw_os_error(errno)
            )));
        }
        Ok(true)
    }
}

pub fn futex_wake(addr: *const u32) -> Result<u32> {
    unsafe {
        let ret = libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAKE,
            1i32, // wake at most 1 waiter
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        );
        if ret < 0 {
            return Err(napi::Error::from_reason(format!(
                "futex_wake failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(ret as u32)
    }
}
