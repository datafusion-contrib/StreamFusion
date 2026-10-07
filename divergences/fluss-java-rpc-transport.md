# Fluss transport uses the released Java connection and broker Arrow RPCs

The requested integration follows Fluss's existing wire protocol through its released
1.0.0 Java connection, metadata router and RPC gateways. It does not incorporate
fluss-rust, introduce a Rust Fluss client, modify the broker, or rely on unpublished
upstream builds. The Flink enumerator and split serializers remain upstream-owned.

The batch data plane deliberately uses version-specific Java implementation APIs and
schema-less Arrow IPC framing rather than waiting for a stable public Arrow polling
API. The shaded Arrow types remain schema metadata only; unshaded vectors borrow
addressable RPC bodies and enter StreamFusion's existing C Data Interface bridge.
Allocator ownership and native alignment behavior follow the existing Comet-inspired
bridge; compressed data and under-aligned native imports still allocate.

This choice permits append-only and primary-key log reads, including their aligned
change-type sidecar, and append-only production on a released broker today. It also
requires a narrow admission whitelist and exact connector-version pin. Unsupported
options and primary-key writes retain the stock connector. Initial primary-key
snapshots and remote range pruning are not optimized.

The integration remains optional and off by default. A future move to the stable
public Arrow polling contract is tracked by [issue #25](https://github.com/datafusion-contrib/StreamFusion/issues/25);
that issue is not closed by this lower-level transport. Coverage and measured limits
live in [the connector documentation](../docs/connectors/fluss.md).

The direct receive optimization also accesses the released connection's private
`rpcClient` and the Netty client's private `bootstrap` fields. It wraps the original
channel initializer, preserving its handshake, authentication, idle and RPC handlers,
then changes only the length-field decoder's accumulator. Direct buffers are allocated
from the four-byte frame length; retained response slices prevent reuse/overwriting of
a shared allocation. This mirrors the released heap-preferring frame allocation
strategy while enabling Arrow foreign-allocation ownership. Availability is checked
at planning time and unavailable access falls back to stock. This is deliberately
coupled to released SDK 1.0.0 and is not a promise of private-API compatibility with
other releases. Diagnostic switches permit the generic direct or heap receiver.

The LZ4 adapter invokes the released block input/output streams rather than copying
Fluss compressor code. It removes whole-vector heap staging through bounded scratch
and Arrow output; protocol framing and the stock reader remain interoperable. This
local adapter and the transport hook add no unpublished dependency or broker change.

Immediate shutdown reuses that bootstrap access to obtain its public
event-loop group. The last connection lease requests zero Netty quiet time before
closing the released Java connection, then waits for network termination. This
avoids the SDK's fixed quiet period without a fork or private shutdown-field changes.
It applies to StreamFusion-owned RPC connections; stock primary-key writers and
snapshot connections retain their SDK lifecycle. This follows Kafka's explicit
network-thread shutdown after pending writes are handled, while preserving Arrow's
independent buffer lifetime across connection close. The connector's regression
tests retain fetched vectors through event-loop termination and release them afterward.
