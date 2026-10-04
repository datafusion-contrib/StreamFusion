use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use jni::objects::{GlobalRef, JValue};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_parquet::bench::{encode, HostDecoder};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn input(vm: &jni::JavaVM, bytes: &[u8], filesystem: bool) -> GlobalRef {
    vm.get_env()
        .unwrap()
        .with_local_frame(16, |env| -> jni::errors::Result<GlobalRef> {
            let encoded = env.byte_array_from_slice(bytes)?;
            let object = env.new_object(
                "tech/streamfusion/parquet/ParquetBenchmarkInput",
                "([BZ)V",
                &[
                    JValue::Object(encoded.as_ref()),
                    JValue::Bool(filesystem.into()),
                ],
            )?;
            env.new_global_ref(object)
        })
        .expect("construct host input")
}

fn counter(vm: &jni::JavaVM, input: &GlobalRef, method: &str) -> i64 {
    vm.get_env()
        .unwrap()
        .call_method(input.as_obj(), method, "()J", &[])
        .unwrap()
        .j()
        .unwrap()
}

fn run(c: &mut Criterion) {
    header();
    let classpath = std::env::var("SF_NATIVE_BENCH_CLASSPATH")
        .expect("Run bin/bench-native.py to prepare the JVM classpath");
    let property = format!("-Djava.class.path={classpath}");
    let args = jni::InitArgsBuilder::new()
        .version(jni::JNIVersion::V8)
        .option(&property)
        .option("-Xms128m")
        .option("-Xmx256m")
        .build()
        .unwrap();
    let vm = jni::JavaVM::new(args).unwrap();
    let _thread = vm.attach_current_thread().unwrap();
    streamfusion_bridge::capture_jvm_raw(vm.get_java_vm_pointer());
    let mut group = c.benchmark_group("format/parquet/host_reader");
    for rows in [16, 1024, 16384] {
        for width in [8, 264] {
            let text = format!("é\0{}", "x".repeat(width));
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                (
                    "name",
                    Arc::new(StringArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
                    )) as ArrayRef,
                ),
            ])
            .unwrap();
            let bytes = encode(&batch, None);
            for filesystem in [false, true] {
                let host = input(&vm, &bytes, filesystem);
                for batch_rows in [64, 4096] {
                    group.throughput(Throughput::Elements(rows as u64));
                    for phase in ["open_close", "decode"] {
                        let label = format!("{phase}/{rows}/bytes={width}/filesystem={filesystem}/batch={batch_rows}");
                        let mut output = Vec::new();
                        if phase == "decode" {
                            let mut decoder = HostDecoder::new(
                                &vm,
                                host.as_obj(),
                                bytes.len(),
                                batch.schema().as_ref(),
                                batch_rows,
                            );
                            while let Some(batch) = decoder.next_batch() {
                                output.push(batch);
                            }
                            assert_eq!(concat_batches(&batch.schema(), &output).unwrap(), batch);
                        }
                        let mut prepared = (phase == "decode").then(|| {
                            HostDecoder::new(
                                &vm,
                                host.as_obj(),
                                bytes.len(),
                                batch.schema().as_ref(),
                                batch_rows,
                            )
                        });
                        let before_reads = counter(&vm, &host, "reads");
                        let before_bytes = counter(&vm, &host, "bytesRead");
                        let (_, allocations) = measure(|| {
                            if let Some(mut decoder) = prepared.take() {
                                while let Some(batch) = decoder.next_batch() {
                                    std::hint::black_box(batch);
                                }
                            } else {
                                std::hint::black_box(HostDecoder::new(
                                    &vm,
                                    host.as_obj(),
                                    bytes.len(),
                                    batch.schema().as_ref(),
                                    batch_rows,
                                ));
                            }
                        });
                        let reads = counter(&vm, &host, "reads") - before_reads;
                        let read_bytes = counter(&vm, &host, "bytesRead") - before_bytes;
                        assert!(reads > 0 && read_bytes > 0, "host callback must execute");
                        println!("HOST_READS,{label},{reads},{read_bytes}");
                        let columns: Vec<_> = output
                            .iter()
                            .flat_map(|b| b.columns().iter().cloned())
                            .collect();
                        report(&label, &[], &columns, allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            if phase == "open_close" {
                                b.iter(|| {
                                    std::hint::black_box(HostDecoder::new(
                                        &vm,
                                        host.as_obj(),
                                        bytes.len(),
                                        batch.schema().as_ref(),
                                        batch_rows,
                                    ));
                                });
                            } else {
                                b.iter_batched(
                                    || {
                                        HostDecoder::new(
                                            &vm,
                                            host.as_obj(),
                                            bytes.len(),
                                            batch.schema().as_ref(),
                                            batch_rows,
                                        )
                                    },
                                    |mut decoder| {
                                        while let Some(batch) = decoder.next_batch() {
                                            std::hint::black_box(batch);
                                        }
                                    },
                                    BatchSize::SmallInput,
                                );
                            }
                        });
                    }
                }
                vm.get_env()
                    .unwrap()
                    .call_method(host.as_obj(), "close", "()V", &[])
                    .unwrap();
            }
        }
    }
    group.finish();
}

criterion_group!(benches, run);
criterion_main!(benches);
