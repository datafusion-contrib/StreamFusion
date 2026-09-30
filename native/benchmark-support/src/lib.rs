//! Untimed allocation and Arrow buffer diagnostics for production-operation benchmarks.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;

use arrow::array::{ArrayData, ArrayRef};

#[derive(Clone, Copy, Debug, Default)]
pub struct Allocations {
    pub calls: usize,
    pub requested_bytes: usize,
}

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static COUNTS: Cell<Allocations> = const { Cell::new(Allocations { calls: 0, requested_bytes: 0 }) };
}

/// Counts Rust allocation requests on the measured thread, delegating to the system allocator.
/// Install only in benchmark executables; setup and Criterion's timed iterations remain uncounted.
pub struct CountingAllocator;

fn record(bytes: usize) {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            let _ = COUNTS.try_with(|counts| {
                let previous = counts.get();
                counts.set(Allocations {
                    calls: previous.calls + 1,
                    requested_bytes: previous.requested_bytes + bytes,
                });
            });
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        System.realloc(pointer, layout, size)
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
    }
}

/// Measures one call after fixture construction and warmup. Reallocation counts its full request;
/// this is allocation traffic, not peak memory, copied bytes, or native C/C++ allocation traffic.
pub fn measure<T>(run: impl FnOnce() -> T) -> (T, Allocations) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(false));
        }
    }
    ACTIVE.with(|active| assert!(!active.get(), "allocation probes cannot nest"));
    COUNTS.with(|counts| counts.set(Allocations::default()));
    ACTIVE.with(|active| active.set(true));
    let reset = Reset;
    let output = run();
    drop(reset);
    (output, COUNTS.with(Cell::get))
}

fn buffers(data: &ArrayData, ranges: &mut BTreeSet<(usize, usize)>) {
    for buffer in data
        .buffers()
        .iter()
        .chain(data.nulls().map(|n| n.buffer()))
    {
        if !buffer.is_empty() {
            ranges.insert((buffer.as_ptr() as usize, buffer.len()));
        }
    }
    for child in data.child_data() {
        buffers(child, ranges);
    }
}

/// Reports output buffer lengths, counting identical ranges once and recognizing input slices.
/// New buffers may contain computed values; their size alone does not establish a payload copy.
pub fn output_buffers(input: &[ArrayRef], output: &[ArrayRef]) -> (usize, usize) {
    let mut inputs = BTreeSet::new();
    let mut outputs = BTreeSet::new();
    for array in input {
        buffers(&array.to_data(), &mut inputs);
    }
    for array in output {
        buffers(&array.to_data(), &mut outputs);
    }
    let (mut shared, mut new) = (0, 0);
    for (address, length) in outputs {
        if inputs.iter().any(|&(base, bytes)| {
            address >= base && address.saturating_add(length) <= base.saturating_add(bytes)
        }) {
            shared += length;
        } else {
            new += length;
        }
    }
    (shared, new)
}

pub fn report(label: &str, input: &[ArrayRef], output: &[ArrayRef], allocations: Allocations) {
    let (shared, new) = output_buffers(input, output);
    let label = format!("\"{}\"", label.replace('"', "\"\""));
    eprintln!(
        "NATIVE_ALLOCATION,{label},{},{},{shared},{new}",
        allocations.calls, allocations.requested_bytes
    );
}

pub fn header() {
    eprintln!("NATIVE_ALLOCATION_HEADER,case,allocation_calls,requested_bytes,shared_output_buffer_bytes,new_output_buffer_bytes");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    #[test]
    fn buffers_distinguish_nested_shared_slices_from_fresh_values() {
        use arrow::array::{Int64Array, StructArray};
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let slice = input.slice(1, 2);
        let nested: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Int64, false)),
            slice.clone(),
        )]));
        assert_eq!(
            output_buffers(&[input.clone()], &[slice.clone(), nested]),
            (16, 0)
        );
        let copy: ArrayRef = Arc::new(Int64Array::from(vec![2, 3]));
        assert_eq!(output_buffers(&[input], &[copy]), (0, 16));
    }

    #[test]
    fn scopes_reset_after_panics_and_count_reallocations() {
        let (_, allocations) = measure(|| {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&[1; 16]);
            bytes.reserve_exact(32);
            std::hint::black_box(bytes)
        });
        assert_eq!(allocations.calls, 2);
        assert!(allocations.requested_bytes >= 64);
        assert!(std::panic::catch_unwind(|| measure(|| panic!("probe"))).is_err());
        let (_, empty) = measure(|| 1);
        assert_eq!(empty.calls, 0);
    }
}
