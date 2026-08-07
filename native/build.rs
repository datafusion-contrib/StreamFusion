fn main() {
    // The `mimalloc` feature rebinds the C allocator for everything linked INTO this library —
    // librdkafka's per-message op calloc/free (unreachable from a Rust #[global_allocator]) and the
    // Rust side's own allocations — by aliasing the libc allocation symbols to mimalloc's at link
    // time. The binding is resolved inside the library only: nothing is exported, no malloc zone is
    // registered, and the hosting process (a Flink JVM) is untouched — the failure mode that made
    // the process-wide override benchmark-grade only. strdup/strndup must be aliased along with
    // malloc/free: leaving them to libc would hand out libc-owned pointers that mimalloc's free
    // would later reject. The deallocation half of that same hazard cannot be closed by aliasing —
    // libc APIs like realpath(3) allocate INTERNALLY with the system allocator and hand the buffer
    // out — so `free` and `realloc` alias to checked shims (mimalloc_shim.c) that route each
    // pointer to the allocator that owns it.
    if std::env::var_os("CARGO_FEATURE_MIMALLOC").is_some() {
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        println!("cargo:rerun-if-changed=mimalloc_shim.c");
        cc::Build::new()
            .file("mimalloc_shim.c")
            .compile("mimalloc_shim");
        let symbols = [
            ("malloc", "mi_malloc"),
            ("calloc", "mi_calloc"),
            ("realloc", "sf_realloc"),
            ("free", "sf_free"),
            ("strdup", "mi_strdup"),
            ("strndup", "mi_strndup"),
            ("posix_memalign", "mi_posix_memalign"),
            ("aligned_alloc", "mi_aligned_alloc"),
        ];
        for (symbol, implementation) in symbols {
            match target_os.as_str() {
                "macos" => {
                    println!("cargo:rustc-link-arg=-Wl,-alias,_{implementation},_{symbol}");
                }
                "linux" => {
                    println!("cargo:rustc-link-arg=-Wl,--defsym={symbol}={implementation}");
                }
                other => {
                    panic!("the mimalloc feature has no link-alias mapping for target OS {other}");
                }
            }
        }
    }
}
