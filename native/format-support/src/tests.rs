use super::*;

// A panic escaping a skip-mode decode must not leave the thread's panics silenced: the marker
// resets on unwind, so the next unexpected failure still reaches the panic hook.
#[test]
fn skip_mode_silencing_resets_after_an_escaping_panic() {
    let escaped = std::panic::catch_unwind(|| silence_expected_decode_panics(|| panic!("boom")));
    assert!(escaped.is_err());
    assert!(!IN_SKIP_DECODE.with(std::cell::Cell::get));
}

// The driver-init handshake fills the vtable only for an ABI version this library speaks — anything
// else is refused, which the connector treats as "stay on the JVM-mediated decode" — and fills only
// the requested version's prefix: a version-1 caller's struct ends at the version-1 fields.
#[test]
fn format_driver_init_gates_on_version() {
    extern "C" fn sentinel(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
        99
    }
    extern "C" fn error_sentinel(_: i64, _: *mut i32) -> *const u8 {
        std::ptr::null()
    }
    let mut driver = FormatDriver {
        decode_body_batch: sentinel,
        decode_last_error: error_sentinel,
    };
    assert_ne!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_2 + 1, &mut driver),
        0
    );
    assert_eq!(driver.decode_body_batch as usize, sentinel as usize);
    assert_eq!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1, &mut driver),
        0
    );
    assert_ne!(driver.decode_body_batch as usize, sentinel as usize);
    assert_eq!(driver.decode_last_error as usize, error_sentinel as usize);
    assert_eq!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_2, &mut driver),
        0
    );
    assert_ne!(driver.decode_last_error as usize, error_sentinel as usize);
    assert_ne!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1, std::ptr::null_mut()),
        0
    );
}
