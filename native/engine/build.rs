fn main() {
    streamfusion_native_build::configure();
    if std::env::var_os("CARGO_FEATURE_ROCKSDB_IO_URING").is_some()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux")
    {
        println!("cargo:rustc-link-arg=-Wl,--wrap=RocksDbIOUringEnable");
    }
}
