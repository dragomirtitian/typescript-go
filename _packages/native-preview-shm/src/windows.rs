use napi::Result;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::sync::Mutex;

use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_OBJECT_0};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, MapViewOfFile, UnmapViewOfFile, FILE_MAP_ALL_ACCESS, PAGE_READWRITE,
};
use windows_sys::Win32::System::Threading::{CreateEventW, SetEvent, WaitForSingleObject, INFINITE};

fn to_wide(s: &str) -> Vec<u16> {
    OsStr::new(s).encode_wide().chain(std::iter::once(0)).collect()
}

/// On Windows, WaitOnAddress is same-process only. For cross-process signaling
/// we use a single named auto-reset Event as a "doorbell": either side signals
/// it after updating the shared state word, and the waiter wakes to re-check.
static EVENT_MAP: Mutex<Option<HashMap<usize, HANDLE>>> = Mutex::new(None);

fn event_map_insert(ptr: usize, handle: HANDLE) {
    let mut guard = EVENT_MAP.lock().unwrap();
    let map = guard.get_or_insert_with(HashMap::new);
    map.insert(ptr, handle);
}

fn event_map_get(ptr: usize) -> Option<HANDLE> {
    let guard = EVENT_MAP.lock().unwrap();
    guard.as_ref().and_then(|m| m.get(&ptr).copied())
}

fn event_map_remove(ptr: usize) -> Option<HANDLE> {
    let mut guard = EVENT_MAP.lock().unwrap();
    guard.as_mut().and_then(|m| m.remove(&ptr))
}

pub fn create_shm(name: &str, size: usize) -> Result<*mut u8> {
    let mapping_name = format!("Local\\{}", name.replace('/', "_"));
    let wide_name = to_wide(&mapping_name);

    unsafe {
        let handle = CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            std::ptr::null(),
            PAGE_READWRITE,
            (size >> 32) as u32,
            size as u32,
            wide_name.as_ptr(),
        );
        if handle == 0 {
            return Err(napi::Error::from_reason(format!(
                "CreateFileMappingW failed: error {}",
                GetLastError()
            )));
        }

        let mapped = MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, size);
        CloseHandle(handle);

        let ptr = mapped.Value as *mut u8;
        if ptr.is_null() {
            return Err(napi::Error::from_reason(format!(
                "MapViewOfFile failed: error {}",
                GetLastError()
            )));
        }

        // Create a named auto-reset event for cross-process doorbell signaling.
        // Both Node and Go open the same named event.
        let evt_name = to_wide(&format!("Local\\{}-evt", name.replace('/', "_")));
        let evt = CreateEventW(
            std::ptr::null(),
            0, // bManualReset = FALSE (auto-reset)
            0, // bInitialState = FALSE (non-signaled)
            evt_name.as_ptr(),
        );
        if evt == 0 {
            UnmapViewOfFile(mapped);
            return Err(napi::Error::from_reason(format!(
                "CreateEventW failed: error {}",
                GetLastError()
            )));
        }

        // Associate the event handle with this buffer's base pointer
        event_map_insert(ptr as usize, evt);

        Ok(ptr)
    }
}

pub fn destroy_shm(_name: &str, ptr: *mut u8, _size: usize) -> Result<()> {
    if let Some(evt) = event_map_remove(ptr as usize) {
        unsafe { CloseHandle(evt); }
    }
    use windows_sys::Win32::System::Memory::MEMORY_MAPPED_VIEW_ADDRESS;
    let addr = MEMORY_MAPPED_VIEW_ADDRESS { Value: ptr as *mut std::ffi::c_void };
    unsafe { UnmapViewOfFile(addr); }
    Ok(())
}

pub fn futex_wait(addr: *const u32, expected: u32, timeout_ms: i32) -> Result<bool> {
    // First check if value already changed (avoid syscall)
    let current = unsafe { std::ptr::read_volatile(addr) };
    if current != expected {
        return Ok(true);
    }

    // Find the event handle from the buffer base pointer.
    // The addr is within the shm region; the base is at offset 0 of the control header.
    // Since we always pass byte_offset=0 (the state word is at the start), addr IS the base.
    let base = addr as usize;
    let evt = event_map_get(base)
        .ok_or_else(|| napi::Error::from_reason("futex_wait: no event for this shm region"))?;

    let timeout = if timeout_ms >= 0 { timeout_ms as u32 } else { INFINITE };
    let ret = unsafe { WaitForSingleObject(evt, timeout) };

    Ok(ret == WAIT_OBJECT_0)
}

pub fn futex_wake(addr: *const u32) -> Result<u32> {
    let base = addr as usize;
    let evt = event_map_get(base)
        .ok_or_else(|| napi::Error::from_reason("futex_wake: no event for this shm region"))?;

    unsafe { SetEvent(evt); }
    Ok(1)
}
