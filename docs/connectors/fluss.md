# Fluss

**Status:** experimental. Unlike [Kafka](kafka.md), which is production-grade and documented in
depth, the Fluss native path has not seen the same hardening — expect rough edges and treat
fallback to stock Flink as the normal, safe outcome.

## Source only

The native source (fluss-rs' log scanner) replaces a Fluss **log-table** scan. There is no native
sink; writes always go through Flink's own Fluss connector. Coordination — split assignment,
startup-offset resolution, partition discovery, snapshot leases, and checkpointing — stays on the
JVM by design; only the scan itself runs natively.

Falls back to Flink on:

- **Primary-key tables** — they read a changelog the native log scanner does not carry (append-only
  log tables only).
- **Datalake-enabled tables** — reads go through the lake, not the log.
- Pushdown the native reader can't honor (a single-row filter, a modification/row-count scan, a
  pushed-down `LIMIT`, pushed partition filters, an empty projection), and metadata/computed
  columns.
- A column type outside the verified whitelist — notably **TIMESTAMP_LTZ**, **BINARY**, and nested
  **ARRAY**/**MAP** (plain TIMESTAMP and nested ROWs of whitelisted scalars are covered).
- `table.log.format` other than `ARROW`.
- Client config the native client can't mirror — an unrecognized `client.*` option, a known option
  with no fluss-rs equivalent, a `client.security.protocol` other than PLAINTEXT or SASL, or a SASL
  mechanism other than PLAIN.

See [Deployment](../deployment.md) for the JARs a Fluss source needs.
