#[macro_use]
extern crate napi_derive;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use darwin as platform;
#[cfg(target_os = "windows")]
use windows as platform;

use napi::{bindgen_prelude::*, JsBuffer };
use std::sync::Mutex;

/// Tracks all live shared memory regions for cleanup on process exit.
static LIVE_REGIONS: Mutex<Vec<ShmRegion>> = Mutex::new(Vec::new());

struct ShmRegion {
    name: String,
    ptr: *mut u8,
    size: usize,
}

unsafe impl Send for ShmRegion {}

/// Create a named shared memory region and return a Buffer backed by it.
/// The Buffer directly points to the mmap'd memory — zero-copy access.
#[napi]
pub fn create_shared_memory(env: Env, name: String, size: u32) -> Result<JsBuffer> {
    let size = size as usize;
    let ptr = platform::create_shm(&name, size)?;

    // Zero the control header (first 64 bytes)
    unsafe {
        std::ptr::write_bytes(ptr, 0, 64.min(size));
    }

    // Track for cleanup
    LIVE_REGIONS.lock().unwrap().push(ShmRegion {
        name: name.clone(),
        ptr,
        size,
    });

    // Create a Buffer backed by the mmap'd memory.
    // The Buffer does NOT own the memory — we handle cleanup in destroy_shared_memory.
    let buf = unsafe {
        env.create_buffer_with_borrowed_data(ptr, size, ptr as *mut std::ffi::c_void, |_hint, _env| {
            // Release hint callback — intentionally a no-op.
            // Memory is owned by the mmap and freed in destroy_shared_memory.
        })?
    };

    Ok(buf.into_raw())
}

/// Unmap and unlink a shared memory region by name.
#[napi]
pub fn destroy_shared_memory(name: String) -> Result<()> {
    let mut regions = LIVE_REGIONS.lock().unwrap();
    if let Some(idx) = regions.iter().position(|r| r.name == name) {
        let region = regions.remove(idx);
        platform::destroy_shm(&region.name, region.ptr, region.size)?;
    }
    Ok(())
}

/// Atomically wait until the uint32 at `byte_offset` in the named shm region
/// is no longer equal to `expected`. Blocks the calling thread.
/// Returns true if woken normally, false on timeout.
/// timeout_ms = -1 means wait indefinitely.
///
/// NOTE: We look up the raw mmap'd pointer by name instead of using a Buffer
/// parameter, because napi-rs copies Buffer data into a Vec<u8> when receiving
/// it as a function argument — the copy lives on a different page, so the
/// kernel's futex key would never match the Go process's waiter.
#[napi]
pub fn futex_wait(name: String, byte_offset: u32, expected: u32, timeout_ms: i32) -> Result<bool> {
    let regions = LIVE_REGIONS.lock().unwrap();
    let region = regions.iter().find(|r| r.name == name)
        .ok_or_else(|| Error::from_reason(format!("futex_wait: no shm region named '{}'", name)))?;
    if (byte_offset as usize) + 4 > region.size {
        return Err(Error::from_reason("byte_offset out of bounds"));
    }
    let ptr = unsafe { region.ptr.add(byte_offset as usize) as *const u32 };
    // Drop the lock before blocking
    drop(regions);
    platform::futex_wait(ptr, expected, timeout_ms)
}

/// Wake one thread waiting on the futex word at `byte_offset` in the named shm region.
/// Returns the number of waiters woken (0 or 1).
#[napi]
pub fn futex_wake(name: String, byte_offset: u32) -> Result<u32> {
    let regions = LIVE_REGIONS.lock().unwrap();
    let region = regions.iter().find(|r| r.name == name)
        .ok_or_else(|| Error::from_reason(format!("futex_wake: no shm region named '{}'", name)))?;
    if (byte_offset as usize) + 4 > region.size {
        return Err(Error::from_reason("byte_offset out of bounds"));
    }
    let ptr = unsafe { region.ptr.add(byte_offset as usize) as *const u32 };
    drop(regions);
    platform::futex_wake(ptr)
}

/// Spin-wait until the uint32 at `byte_offset` equals `expected`.
/// Uses CPU pause/yield hints to reduce power consumption and avoid
/// starving hyperthreads. Returns the final value read.
/// This replaces a JS-side spin loop with a native one that has proper
/// CPU hints — one napi call instead of thousands of DataView reads.
#[napi]
pub fn spin_wait_for(name: String, byte_offset: u32, expected: u32) -> Result<u32> {
    let regions = LIVE_REGIONS.lock().unwrap();
    let region = regions.iter().find(|r| r.name == name)
        .ok_or_else(|| Error::from_reason(format!("spin_wait_for: no shm region named '{}'", name)))?;
    if (byte_offset as usize) + 4 > region.size {
        return Err(Error::from_reason("byte_offset out of bounds"));
    }
    let ptr = unsafe { region.ptr.add(byte_offset as usize) as *const u32 };
    drop(regions);

    loop {
        let val = unsafe { std::ptr::read_volatile(ptr) };
        if val == expected {
            return Ok(val);
        }
        std::hint::spin_loop();
    }
}

/// Spin-wait until the uint32 at `byte_offset` is NOT equal to `current`.
/// Returns the new value. Useful for sequence-counter based signaling.
#[napi]
pub fn spin_wait_until_changed(name: String, byte_offset: u32, current: u32) -> Result<u32> {
    let regions = LIVE_REGIONS.lock().unwrap();
    let region = regions.iter().find(|r| r.name == name)
        .ok_or_else(|| Error::from_reason(format!("spin_wait_until_changed: no shm region named '{}'", name)))?;
    if (byte_offset as usize) + 4 > region.size {
        return Err(Error::from_reason("byte_offset out of bounds"));
    }
    let ptr = unsafe { region.ptr.add(byte_offset as usize) as *const u32 };
    drop(regions);

    loop {
        let val = unsafe { std::ptr::read_volatile(ptr) };
        if val != current {
            return Ok(val);
        }
        std::hint::spin_loop();
    }
}
