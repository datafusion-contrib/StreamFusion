# Parquet column-chunk streaming

**Applies to:** the native Parquet sink

The native encoder uses parquet-rs' lower-level row-group and column-writer APIs. When a row group
closes, each encoded column chunk is appended immediately through a bounded one-MiB JNI adapter to
Flink's recoverable output stream. The previous high-level `ArrowWriter` path first accumulated the
completed chunks in the output `SharedBuffer`, then drained them after the write call returned. Its
cleared `Vec` retained row-group-sized capacity while the encoder built the next row group.

This keeps Parquet's normal row-group boundary while reducing the additional bridge high-water mark
from a compressed row group to one MiB. It also lets Flink's object-store stream begin forming and
uploading multipart pieces while later column chunks are appended. Flink still decides multipart
size, local staging, rolling, checkpoint recovery, and final publication.

This does not eliminate row-group memory: parquet-rs still retains encoded and not-yet-encoded data
inside the active column writers until that row group closes. It eliminates the second row-group-
sized output copy and releases completed column chunks to Flink one by one.

The change is memory- and latency-oriented; no throughput claim is recorded without a release-build
benchmark.
