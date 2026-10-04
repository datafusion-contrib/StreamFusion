use std::sync::OnceLock;

// The released binding defines a strong, always-enabled hook. Linker wrapping lets
// us control it without replacing the dependency or exporting a duplicate symbol.
#[no_mangle]
pub extern "C" fn __wrap_RocksDbIOUringEnable() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("SF_ROCKSDB_IO_URING") {
        Ok(value) => value == "true",
        Err(std::env::VarError::NotPresent) => true,
        Err(std::env::VarError::NotUnicode(_)) => false,
    })
}
