# Doris sink constraints and disk-backed candidates

These complete-job measurements retain Doris reads, SQL planning/execution, JNI, intrinsic state
work, Arrow output serialization, Stream Load, commit and query visibility. Cluster boot and input
preparation are outside timing. Every trial validates its output and admitted native execution.
[results.json](results.json) retains all 192 warmup/measured trials, ranges, build hashes and host
configuration; scenario directories retain CSVs, configuration and representative plans, and compressed logs retain
unfavorable trials. There are two warmups and six measured trials per engine and workload.

## Controls and resources

All builds use Flink 2.2.1, connector 26.3.0, Doris 4.1.4.1, Java 17 and release Rust with mimalloc.
The shared host is Linux/WSL2, Core i7-12650H, 16 logical CPUs and about 9.7 GiB RAM. Jobs use
parallelism two, a 2 GiB JVM heap, a fresh metric-reporting MiniCluster per job and the existing
256 MiB task off-heap configuration. RocksDB cases configure the same 32 MiB per-slot pool on
stock and native backends. Compilation and other tests do not overlap measured jobs.

Constraint controls use the same core: stock Flink; the previous admission behavior (native Doris
source/SQL followed by stock constraints and sink); and the columnar constraint/sink path. Modes
rotate within the same warm JVM. DROP makes every seventh input key null at the sink boundary;
the SQL filter still selects even input keys. TRIM_PAD feeds overlong Unicode strings through a
VARCHAR(12) declaration against a physical STRING column, ensuring enforcement actually runs.
Nullable-length failure parity is tested separately against the released executor; this positive
benchmark uses empty strings for those inputs. Payloads also contain integers and nullable decimals.

Materialization uses the same updating GROUP BY and forced MAP strategy as the existing Doris
benchmark. The scalar cases have 1,000 or 100,000 keys; the nested case adds timestamp, array with
null elements, and ROW payloads at 1,000 keys. Stock and current native modes rotate within one JVM.
**Previous materialization is the actual baseline implementation at `62681aaeea8e8ef98768a014c57b004aeca93243`,
with only the benchmark test copied in**, running matching Java and its own release DSO in a separate
JVM. It retains live candidates in Rust RAM while using the RocksDB snapshot wrapper. The current
path stores candidates and matching indexes in RocksDB. Separate builds cannot share one JVM;
previous-build runs are not interleaved with current-build trials. Plans and library hashes make
this distinction explicit. No periodic checkpoints are enabled; recovery, rescaling, TTL and old
checkpoint migration are validated separately.

## Results

Median seconds, with the complete measured range in brackets:

| Workload | Input rows | Stock Flink | Previous | Current columnar |
| --- | ---: | ---: | ---: | ---: |
| NOT NULL DROP | 100,000 | 0.334 [0.261–0.407] | 0.330 [0.294–0.361] | 0.308 [0.297–0.337] |
| NOT NULL DROP | 1,000,000 | 0.525 [0.488–0.566] | 0.432 [0.421–0.457] | 0.404 [0.383–0.411] |
| TRIM_PAD | 100,000 | 0.363 [0.234–0.386] | 0.314 [0.293–0.350] | 0.316 [0.277–0.378] |
| TRIM_PAD | 1,000,000 | 0.522 [0.513–0.542] | 0.432 [0.420–0.460] | 0.408 [0.397–0.434] |
| RocksDB MAP, 1k keys | 100,000 | 0.575 [0.560–0.611] | 0.368 [0.341–0.404] | 0.356 [0.340–0.377] |
| RocksDB MAP, 1k keys | 1,000,000 | 2.980 [2.516–3.311] | 0.587 [0.556–0.606] | 0.795 [0.620–0.891] |
| RocksDB MAP, 100k keys | 1,000,000 | 4.166 [3.250–5.077] | 0.800 [0.775–0.835] | 1.822 [1.202–2.188] |
| RocksDB MAP, nested | 1,000,000 | 3.644 [3.537–7.124] | 0.992 [0.945–1.034] | 1.434 [1.303–4.112] |

At one million rows, DROP and TRIM_PAD improve their stock medians by 1.30× and 1.28×, and their
previous medians by 1.07× and 1.06×. The 100k cases overlap substantially; TRIM_PAD's current median
is slightly slower than previous there. These are startup-inclusive shared-host jobs, not isolated
constraint throughput or sustained-stream measurements.

Disk-backed materialization improves stock medians by 2.29–3.75× at one million rows. It costs
1.35–2.28× the previous RAM-backed candidate implementation in those cases: point reads/writes and
persistent candidate/index maintenance replace resident maps. This is a capacity and persistence
change, not a speedup over retaining candidates in RAM. A separate capacity regression keeps
10,000 candidates under one sink key with a 16 KiB operator budget, checkpoints/restores the native
database files and retracts the visible candidate; the same candidates exhaust the heap budget.
The benchmark itself does not establish performance for state larger than RAM or checkpoint upload.
Every slow trial remains in the results, including the nested native outlier and high-cardinality
warmup trend.

The measured current DSO precedes two defensive changes: bounded legacy-key lookup at recovery
and reservation of disk fast-path payload scratch. They do not change the queries, candidate layout
or mutation algorithms. Final correctness gates run against the final build.

## Reproduction

Build release Rust and run the existing pinned Doris fixture with Java 17:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --lib --release --features mimalloc
SF_DORIS_NATIVE_BUILD_SKIP=true bin/doris-suite.sh benchmark \
  -Dsf.doris.benchmark.constraints=drop \
  -Dsf.doris.benchmark.rows=100000,1000000 \
  -Dsf.doris.benchmark.warmups=2 -Dsf.doris.benchmark.trials=6
```

Use `constraints=trim` for TRIM_PAD. For current disk-backed materialization use
`materialize=true`, `rocks=true`, `strategy=MAP`, `cardinality=1000` or `100000`, and
`modes=stock,arrow`; add `complex=true` for the nested workload. Run the baseline checkout's own
Java/release library with this test harness copied in, `modes=arrow` and `modeLabel=previous` for
the actual previous materializer control. Keep each native library in a separate directory and
select it with `java.library.path`; never overwrite a library mapped by a running JVM.
