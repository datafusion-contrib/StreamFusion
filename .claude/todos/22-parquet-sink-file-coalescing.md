# Parquet sink file coalescing

**Status:** done. The sink holds one `ArrowWriter` open across batches via a native writer handle
(`createParquetWriter` / `parquetWriterWrite` / `closeParquetWriter`) and rolls a file only on a row
target (`DEFAULT_TARGET_ROWS = 1M`) or on checkpoint. Output file count now tracks total size, not
batch size. Exactly-once unchanged (checkpoint closes the open file into the recorded set; commit on
completion; idempotent commit on recovery). Tests: roll-threshold + the existing SQL round-trip
(proves no rows lost). Below is the original plan, kept for context.

**Source:** the columnar-copy profiling (ticket 20/21). The native sink wrote **one Parquet
file per incoming batch**, so the output file count was tied to the upstream batch size — small
batches produce thousands of tiny files (footer/metadata/syscall overhead each, and poor output
for downstream readers).

## Why
The first columnar-copy benchmark measured 0.45× partly because 1024-row read batches became
~4,900 tiny output files. Raising the read batch size to 8192 was a blunt workaround (it couples
output layout to an unrelated read knob and risks large per-batch memory). The right fix is to
decouple output file size from batch size in the sink itself, the way Flink's filesystem sink does
(it rolls files on size / checkpoint).

## What to build
- Buffer incoming Arrow batches in the sink and write a Parquet file only when a target is reached
  (a row-count or byte-size threshold), plus a final flush. One `ArrowWriter` can take multiple
  batches before `close()`, so a file naturally holds many batches.
- Keep the exactly-once contract: a file is in-progress until the checkpoint that recorded it
  completes (the current two-phase commit). Coalescing changes *when* a file is finalized for
  writing, not the commit protocol — make sure a checkpoint flushes the current open file so its
  rows are durable in the recorded set.
- Pick a sensible default target (e.g. 128 MB or a few hundred k rows) and keep it simple.

## Acceptance criteria
- Output file count is roughly `total_size / target`, independent of the read batch size.
- Exactly-once still holds (parity + recovery tests stay green).
- The columnar-copy benchmark improves or holds at the 8192-batch level without needing a large
  read batch size, and the read batch size can return to a memory-safe default.
