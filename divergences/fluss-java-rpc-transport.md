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
