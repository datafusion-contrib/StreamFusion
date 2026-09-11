/// Retains the allocator and checked-free shim in the calling library.
#[macro_export]
macro_rules! link_allocator {
    () => {
        /// Anchors libmimalloc-sys's object file into the link: nothing references the crate by symbol
        /// until build.rs's link aliases resolve, so without this the archive member they alias into would
        /// be dropped. See the `mimalloc` feature in Cargo.toml.
        #[cfg(feature = "mimalloc")]
        #[used]
        pub(crate) static FORCE_LINK_MIMALLOC: unsafe extern "C" fn(*mut std::os::raw::c_void) =
            libmimalloc_sys::mi_free;

        // The checked free/realloc implementations live together in a small C archive built by build.rs.
        // A --defsym alias does not make the static linker extract that archive member on every linker,
        // so retain an ordinary Rust reference as well. This matters for the RocksDB-enabled core DSO,
        // whose larger static link otherwise leaves sf_realloc undefined on Linux.
        #[cfg(feature = "mimalloc")]
        extern "C" {
            fn sf_realloc(
                pointer: *mut std::os::raw::c_void,
                size: usize,
            ) -> *mut std::os::raw::c_void;
        }

        #[cfg(feature = "mimalloc")]
        #[used]
        pub(crate) static FORCE_LINK_MIMALLOC_SHIM:
            unsafe extern "C" fn(*mut std::os::raw::c_void, usize) -> *mut std::os::raw::c_void =
            sf_realloc;
    };
}
