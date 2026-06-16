extern crate napi_build;

fn main() {
    // Skip napi_build::setup() when cross-compiling for Windows from Linux —
    // it tries to find libnode.dll which doesn't exist on the host.
    // The addon will resolve node symbols via delayed loading at runtime.
    let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let host = std::env::var("HOST").unwrap_or_default();
    if target == "windows" && !host.contains("windows") {
        return;
    }
    napi_build::setup();
}
