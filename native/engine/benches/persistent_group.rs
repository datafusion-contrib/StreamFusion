use criterion::{criterion_group, criterion_main, Criterion};
use streamfusion_benchmark_support::{header, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
const OPTIONS: &str = include_str!("fixtures/rocks-options.json");
#[path = "persistent_state/group.rs"]
mod group;

fn register(c: &mut Criterion) {
    header();
    group::group(c);
}

criterion_group!(benches, register);
criterion_main!(benches);
