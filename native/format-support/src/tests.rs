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

// A keyed table's slab carries the keys beside the values: the decode input becomes `[key, body]`,
// nulls kept, and the keys are always copied out of the JVM's buffer while the values are copied
// only on request.
#[test]
fn record_slab_pairs_keys_with_values() {
    // Layout: key lengths, value lengths, keys, values — each region at its own offset.
    let key_lengths = [2_i32, -1, 1];
    let value_lengths = [3_i32, 0, -1];
    let mut slab = Vec::new();
    slab.extend(key_lengths.iter().flat_map(|l| l.to_ne_bytes()));
    slab.extend(value_lengths.iter().flat_map(|l| l.to_ne_bytes()));
    slab.extend_from_slice(b"k1k");
    slab.extend_from_slice(b"abc");
    let mut layout = [-1_i64; LAYOUT_LEN];
    layout[LAYOUT_KEY_LENGTHS] = 0;
    layout[LAYOUT_VALUE_LENGTHS] = 12;
    layout[LAYOUT_KEYS] = 24;
    layout[LAYOUT_KEYS_LEN] = 3;
    layout[LAYOUT_VALUES] = 27;
    layout[LAYOUT_VALUES_LEN] = 3;
    let slab_address = slab.as_ptr() as usize;

    let records = unsafe { RecordSlab::new(slab_address, slab.len(), layout, 3) }.records(false);

    assert_eq!(records.schema().field(0).name(), "key");
    let keys = records
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        vec![Some(b"k1".as_slice()), None, Some(b"k".as_slice())]
    );
    let bodies = records
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(
        bodies.iter().collect::<Vec<_>>(),
        vec![Some(b"abc".as_slice()), Some(b"".as_slice()), None]
    );
    let slab_bytes = slab_address..slab_address + slab.len();
    assert!(!slab_bytes.contains(&(keys.values().as_ptr() as usize)));
    assert!(slab_bytes.contains(&(bodies.values().as_ptr() as usize)));
}
