# Direct protobuf-to-Arrow decoding

**Applies to:** native Kafka protobuf sources

The protobuf source owns an Apache-2.0-derived wire decoder instead of routing through a general
conversion crate. It reads message bytes directly into Arrow builders; there is no
`DynamicMessage`, Java object, or row-at-a-time Arrow conversion in the data path. Owning this code
also lets StreamFusion match Flink's table semantics for unsigned values, enums, bytes, presence,
defaults, oneofs, nested messages, repeated fields, maps, and well-known messages.

## One-time schema compilation

When a task opens its decoder, StreamFusion recursively compiles the portable protobuf descriptor
into a task-local execution plan. The plan contains the tag lookup and field operation for every
top-level and nested message, including repeated-message elements and message-valued maps. It is
reused for every input batch; descriptor traversal and nested tag-table construction never occur in
the batch loop.

This is an in-process Rust execution plan, not a generated job-specific shared library. Users do not
need a Rust toolchain on a TaskManager or an additional artifact. The generated Java class is
consumed during planning, when StreamFusion captures its descriptor and message name in the physical
source plan.

There is no message-name-specific decoder. Nexmark and user messages use the same production path.

## Sparse column completion

The original profile was dominated by flushing every schema field after every message, including
all children of absent nested branches. The decoder now tracks the last row in which each field was
present. A value remains pending until that field next appears or the batch finishes, and missing row
ranges are appended in bulk. This preserves protobuf's last-value-wins behavior while avoiding work
for fields that are absent on the wire.

The same mechanism applies recursively to nested messages, repeated fields, and maps. Oneof groups
additionally clear an earlier member when a later member occurs in the same wire message. Schemas
without oneofs use a scan loop with no oneof bookkeeping.

The hot field operations are kept inline, and field-local sparse state is stored beside its compiled
operation so decoding does not bounce between parallel metadata arrays. String and byte values
borrow their input slice until completion; ASCII strings take the simple ASCII validation path while
non-ASCII strings retain full UTF-8 validation.

## Measured result

The release benchmark uses 8,192 pre-serialized Nexmark messages, a one-second warmup, five
three-second measurements, and reports the best run. It times bytes to each engine's destination
representation, including StreamFusion's Arrow C Data import/export around every batch.

The general production decoder improved from **6.021 M rows/s (166.1 ns/row)** before this work to
**7.595 M rows/s (131.7 ns/row)**, a **26.1% throughput increase** and **20.7% lower per-row cost**.
In the final matched run, Flink's generated Java decoder reached **6.280 M rows/s (159.2 ns/row)**,
so the arbitrary-schema StreamFusion path was **1.21x Flink**.

Correctness is covered by the ported decoder/config tests, StreamFusion's native unit suite,
Docker-backed Kafka SQL parity tests, and the untouched upstream Flink protobuf decode/SQL suite.
The tests include reordered and unknown fields, malformed input, sparse nested data, maps, repeated
messages, projections, default handling, and multiple oneof members in the same wire message.
