package io.github.jordepic.streamfusion;

/** Entry point to the native data plane. Holds the methods backed by the Rust library. */
public final class Native {

  static {
    System.loadLibrary("streamfusion");
  }

  private Native() {}

  /** Version reported by the loaded native library, proving the JVM↔Rust bridge is live. */
  public static native String version();

  /**
   * Awaits a trivial async computation on the native runtime, proving the blocking bridge a JVM
   * thread uses to drive native plan execution.
   */
  public static native long blockingAnswer();

  /**
   * Sums an int32 column the JVM has exported through the Arrow C Data Interface.
   *
   * @param arrayAddress address of the producer-allocated {@code ArrowArray} C struct
   * @param schemaAddress address of the producer-allocated {@code ArrowSchema} C struct
   */
  public static native long sumInt(long arrayAddress, long schemaAddress);

  /**
   * Imports an int32 column the JVM exported and exports an equal column back into the
   * consumer-allocated C structs, exercising both directions of the boundary.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   */
  public static native void roundTrip(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /**
   * Applies the first stateless operator, a projection that doubles an int32 column, to a batch the
   * JVM exported, writing the produced column back into the consumer-allocated C structs.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   */
  public static native void doubleColumn(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /**
   * Runs a filter as a full plan over a batch the JVM exported, keeping rows whose int32 column
   * exceeds {@code threshold}, and writes the surviving column into the consumer-allocated C
   * structs. Native execution is async, so this drives the plan to completion on the native
   * runtime.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   * @param threshold rows are kept when the column value is strictly greater than this
   */
  public static native void filterGreaterThan(
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      int threshold);

  /**
   * Compiles a general predicate expression into a reusable handle. The predicate is the encoded
   * tree (pre-order parallel arrays — see the expression encoder): {@code kinds} tags each node
   * (0=input ref, 1=long literal, 2=double literal, 3=string literal, 4=bool literal, 6=call,
   * 7/8/9=int/smallint/tinyint literal — narrow integer literals whose value still rides in the
   * long pool but keep their declared width so arithmetic matches the host), {@code payload} carries
   * the column index / op code / literal-pool index, and {@code childCounts} the operand count of
   * each call; literals are drawn from {@code longs}/{@code doubles}/{@code strings} by index. The
   * handle compiles the plan once (against the first batch's schema) and reuses it, and must be
   * released with {@link #closeFilterExpression(long)}.
   *
   * <p>Call op codes: 0=+, 1=-, 2=*, 10=&gt;, 11=&gt;=, 12=&lt;, 13=&lt;=, 14==, 15=&lt;&gt;, 20=AND,
   * 21=OR, 22=NOT.
   */
  public static native long createFilterExpression(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings);

  /**
   * Filters a batch the JVM exported through a compiled predicate handle, writing the surviving rows
   * into the consumer-allocated output C structs. A null predicate result drops the row, as SQL
   * {@code WHERE} requires.
   *
   * @param handle a handle from {@link #createFilterExpression}
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   */
  public static native void filterExpression(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  /** Releases a compiled predicate handle and its native state. */
  public static native void closeFilterExpression(long handle);

  /**
   * Runs a batch the JVM exported through the stateless windowing table function, writing the
   * fanned-out batch (input columns, one copy per window for hopping/cumulative, plus appended
   * {@code window_start}/{@code window_end}/{@code window_time}) into the consumer-allocated output C
   * structs. Stateless — there is no handle to create or release.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   * @param timeColumn index of the event-time column the window is assigned over
   * @param windowMillis window size in millis (the max size for cumulative)
   * @param slideMillis slide in millis (the size for tumbling, the step for cumulative)
   * @param cumulative whether the window is cumulative (nested windows sharing a start)
   * @param proctime whether to assign by the processing-time clock instead of the time column
   * @param proctimeNowMillis the processing-time clock (epoch millis) to assign every row by when
   *     {@code proctime} is set; ignored otherwise
   */
  public static native void assignWindows(
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      int timeColumn,
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      boolean proctime,
      long proctimeNowMillis);

  /**
   * Stateless GROUPING SETS / CUBE / ROLLUP expansion: fans each input row out to {@code
   * numExpandRows} output rows, one per grouping set. For output column {@code c} and expand row
   * {@code r}, {@code copyIndices[r*numOutCols + c]} is the input column to copy (an {@code InputRef}
   * cell) or {@code -1} for a literal — the expand-id column ({@code expandIdIndex}) takes the per-row
   * grouping id {@code expandIdValues[r]}, every other literal cell is a typed NULL (a grouped-out
   * key). The {@code $row_kind$} tag rides through, so the expansion is changelog-transparent.
   *
   * @param expandIdIsLong whether the expand-id column is BIGINT (Int64) rather than INT (Int32)
   */
  public static native void expand(
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      int numExpandRows,
      int numOutCols,
      int expandIdIndex,
      boolean expandIdIsLong,
      int[] copyIndices,
      long[] expandIdValues);

  /**
   * Stateless INNER UNNEST of an ARRAY column: fans each input row out to one output row per element
   * of {@code arrayCol}, the input columns repeated and the element appended ({@code [input cols..,
   * element]}). A null/empty array yields no rows (INNER); a null element rides through as a null
   * row. The {@code $row_kind$} tag rides through (repeated per element), so it is
   * changelog-transparent.
   */
  public static native void unnest(
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      int arrayCol,
      boolean withOrdinality,
      boolean isLeft,
      boolean isMultiset);

  /**
   * Creates a buffering local half of a two-phase non-windowed {@code GROUP BY} and returns an opaque
   * handle. It accumulates rows by key across batches in memory (each aggregate folds its {@code
   * valueColumns} entry, or {@code -1} for COUNT(*), read as {@code valueTypes}; SUM kind 0, MIN 1,
   * MAX 2, COUNT 3) and emits one partial row per key ({@code [key0.., partial0..]}, no {@code
   * $row_kind$} — insert-only) when flushed. The buffer is transient (drained before each checkpoint
   * by the operator), so there is no snapshot/restore — the global half keeps the durable state.
   * Released with {@link #closeLocalGroupAggregator(long)}.
   */
  public static native long createLocalGroupAggregator(
      int[] aggregateKinds, int[] valueTypes, int[] valueColumns, int[] keyColumns);

  /** Folds a batch into the buffered per-key accumulators; emits nothing. */
  public static native void updateLocalGroupAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Emits the buffered partials (one row per key) and clears the buffer. */
  public static native void flushLocalGroupAggregator(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native void closeLocalGroupAggregator(long handle);

  /**
   * Compiles an encoded Calc — an optional condition tree plus the projection trees, sharing one set
   * of pools, with each tree's root in {@code projectionRoots}/{@code conditionRoot} — into a
   * reusable handle. Released with {@link #closeCalcExpression(long)}.
   *
   * @param projectionRoots the pre-order node index of each projection tree's root
   * @param conditionRoot the condition tree's root index, or -1 if there is no condition
   * @param outputNames the output column names, in order
   */
  public static native long createCalcExpression(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      int[] projectionRoots,
      int conditionRoot,
      String[] outputNames);

  /**
   * Runs a batch the JVM exported through a compiled Calc handle — filtering by the condition, then
   * projecting — writing the output batch into the consumer-allocated output C structs.
   *
   * @param handle a handle from {@link #createCalcExpression}
   */
  public static native void calcExpression(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  /** Releases a compiled Calc handle and its native state. */
  public static native void closeCalcExpression(long handle);

  /**
   * Writes an Arrow batch the JVM exported to a Parquet file at {@code path}, encoding it in its
   * columnar form directly rather than through the host's row-to-Parquet path. The core of the
   * native columnar sink.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param path filesystem path of the Parquet file to write
   */
  public static native void writeParquet(long inArrayAddress, long inSchemaAddress, String path);

  /**
   * Opens a Parquet file for writing and returns an opaque handle. Batches are appended with {@link
   * #parquetWriterWrite}; the file is finalized (footer written) and the handle released by {@link
   * #closeParquetWriter}. One open writer takes many batches before it is closed, so the sink rolls
   * a file on its own row/size target rather than once per batch.
   *
   * @param path filesystem path of the Parquet file to write
   */
  public static native long createParquetWriter(String path);

  /**
   * Appends an Arrow batch the JVM exported to the open file behind {@code handle}.
   *
   * @param handle a handle from {@link #createParquetWriter}
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   */
  public static native void parquetWriterWrite(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Finalizes the Parquet file (writes its footer) and releases the writer handle. */
  public static native void closeParquetWriter(long handle);

  /**
   * Opens one Parquet split — the row groups of {@code path} starting within {@code [rangeStart,
   * rangeStart + rangeLength)} — and returns an opaque handle. Flink's file source enumerates the
   * directory and assigns each subtask file byte ranges; the handle yields batches one at a time via
   * {@link #nextBatch} and must be released with {@link #closeSource}.
   *
   * @param path the Parquet file to read
   * @param projection output column names, in the order the plan expects (honoring projection
   *     pushdown); an empty array emits every column as read
   * @param rangeStart first byte of the assigned split
   * @param rangeLength length of the assigned split in bytes
   */
  public static native long openParquet(
      String path, String[] projection, long rangeStart, long rangeLength);

  /**
   * Exports the next Arrow batch from a source handle into the consumer-allocated C structs.
   *
   * @param handle a handle from a native file source (e.g. {@link #openParquet})
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   * @return true if a batch was produced, false once the split is exhausted
   */
  public static native boolean nextBatch(long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases a native file source handle (Parquet, ORC, …). */
  public static native void closeSource(long handle);

  /**
   * Opens one ORC split — the stripes of {@code path} starting within {@code [rangeStart, rangeStart +
   * rangeLength)} — and returns an opaque handle. ORC is self-describing, so the reader derives the
   * schema from the file. The handle yields batches one at a time via {@link #nextBatch} and must be
   * released with {@link #closeSource}.
   *
   * @param path the ORC file to read
   * @param projection output column names, in the order the plan expects (honoring projection
   *     pushdown); an empty array emits every column as read
   * @param rangeStart first byte of the assigned split
   * @param rangeLength length of the assigned split in bytes
   */
  public static native long openOrc(
      String path, String[] projection, long rangeStart, long rangeLength);

  /**
   * Splits a batch the JVM exported by a consistent hash of the {@code keyColumns} into up to {@code
   * numPartitions} sub-batches (every row with a given key in one partition), returning a handle to
   * pull them with {@link #nextSplit}; released with {@link #closeSplit}. The columnar shuffle's
   * by-key routing.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param keyColumns indices of the key columns to hash
   * @param numPartitions number of partitions (downstream channels) to split into
   */
  public static native long splitByKey(
      long inArrayAddress, long inSchemaAddress, int[] keyColumns, int numPartitions);

  /**
   * Exports the next sub-batch of a split into the consumer-allocated C structs and returns its
   * partition index, or -1 once the split is exhausted.
   */
  public static native int nextSplit(long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases a split handle. */
  public static native void closeSplit(long handle);

  /**
   * Imports a whole multi-column batch the JVM exported and exports an equal batch back into the
   * consumer-allocated C structs, exercising batch transfer beyond a single column.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   */
  public static native void echoBatch(
      long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /**
   * Runs an event-time tumbling-window sum over a batch the JVM exported. The input batch has a
   * {@code ts} column (event time in millis) and a {@code value} column; the result has a {@code
   * window_start} column and a {@code total} column, one row per window.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param windowMillis width of each tumbling window in milliseconds
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   */
  public static native void tumblingSum(
      long inArrayAddress,
      long inSchemaAddress,
      long windowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /**
   * Creates a stateful tumbling-window aggregator and returns an opaque handle. The handle owns
   * native state that persists across calls and must be released with {@link
   * #closeTumblingAggregator(long)}.
   *
   * @param windowMillis window size in milliseconds
   * @param slideMillis window slide in milliseconds (equal to the size for a tumbling window)
   * @param valueTypes one value-column type per aggregate (0=bigint, 1=double, 2=int, 4=smallint,
   *     5=tinyint, 6=float), positionally matching {@code aggregateKinds} so each aggregate reads its
   *     own value column
   * @param aggregateKinds one code per aggregate: 0=SUM, 1=MIN, 2=MAX, 3=COUNT, 4=AVG
   * @param memoryBudgetBytes managed-memory budget bounding the open-window state (negative for
   *     unaccounted); exceeding it throws {@link NativeMemoryLimitException} from the violating call
   */
  public static native long createTumblingAggregator(
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      long memoryBudgetBytes);

  /**
   * Folds a batch (columns {@code ts} and {@code value}) into the aggregator's open windows.
   * Produces no output; closed windows are emitted by {@link #flushTumblingAggregator}.
   */
  public static native void updateTumblingAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Window-attached local half: folds a batch whose rows carry explicit {@code window_start}/{@code
   * window_end} columns (epoch millis) — an upstream window aggregate's output being re-aggregated per
   * window (Nexmark q5) — into the open windows, folding each row into the single window it names.
   */
  public static native void updateAttachedTumblingAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Emits the windows the watermark has closed as a batch (columns {@code window_start} and {@code
   * total}) and drops them from state.
   */
  public static native void flushTumblingAggregator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases an aggregator handle and its native state. */
  public static native void closeTumblingAggregator(long handle);

  /**
   * Local two-phase half: merges a batch of partials ({@code key}, {@code partial}, {@code
   * slice_end}) into the aggregator's windows.
   */
  public static native void updatePartialTumblingAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Local two-phase half: emits the partial state of windows the watermark has closed as a batch
   * ({@code key}, {@code partial}, {@code slice_end}).
   */
  public static native void flushPartialTumblingAggregator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Serializes an aggregator's open windows so they can be stored in a checkpoint. */
  public static native byte[] snapshotTumblingAggregator(long handle);

  /**
   * Rebuilds an aggregator from a snapshot and returns a fresh handle.
   *
   * @param windowMillis window size, supplied again since it is configuration, not state
   * @param slideMillis window slide (equal to the size for a tumbling window)
   * @param valueTypes value-column type per aggregate (see {@link #createTumblingAggregator})
   * @param aggregateKinds aggregate codes (see {@link #createTumblingAggregator})
   * @param snapshot bytes produced by {@link #snapshotTumblingAggregator(long)}
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator}); the
   *     restored state is accounted immediately, so a snapshot that no longer fits fails here
   */
  public static native long restoreTumblingAggregator(
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a columnar event-time OVER aggregator (RANGE between unbounded preceding and current
   * row): it buffers input batches and, on a watermark, emits the completed rows with the running
   * aggregate(s) appended. Released with {@link #closeOverAggregator}.
   *
   * @param valueTypes value column type per aggregate (see {@link #createTumblingAggregator}); empty
   *     for window-function OVER with no value argument
   * @param aggregateKinds aggregate codes (see {@link #createTumblingAggregator})
   * @param rtColumn rowtime column index in the input batch
   * @param valueColumns value column index per aggregate (each aggregate reads its own); empty for
   *     window-function OVER
   * @param keyColumns PARTITION BY column indices in the input batch (empty for no partition)
   * @param frameKind frame shape: 0 = RANGE unbounded preceding, 1 = bounded ROWS, 2 = bounded RANGE
   * @param frameOffset n preceding rows (ROWS) or the preceding interval in millis (RANGE); 0 when
   *     unbounded
   * @param proctime whether the order is processing time (arrival order, eager emit) vs a rowtime
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createOverAggregator(
      int[] valueTypes,
      int[] aggregateKinds,
      int rtColumn,
      int[] valueColumns,
      int[] keyColumns,
      int frameKind,
      long frameOffset,
      boolean proctime,
      long memoryBudgetBytes);

  /** Buffers an input batch; its rows are emitted later when a watermark completes them (rowtime). */
  public static native void pushOverAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Proctime OVER: folds a batch in arrival order and exports its rows immediately (no watermark),
   * each with the running aggregate(s) appended, into the consumer-allocated C structs.
   */
  public static native void pushProctimeOverAggregator(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /**
   * Exports the rows the watermark has completed — the input columns with the running aggregate(s)
   * appended — into the consumer-allocated C structs (an empty batch if none are complete).
   */
  public static native void flushOverAggregator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases an OVER aggregator handle. */
  public static native void closeOverAggregator(long handle);

  /** Serializes an OVER aggregator's running state and buffered rows for a checkpoint. */
  public static native byte[] snapshotOverAggregator(long handle);

  /** Rebuilds an OVER aggregator from a snapshot and returns a fresh handle. */
  public static native long restoreOverAggregator(
      int[] valueTypes,
      int[] aggregateKinds,
      int rtColumn,
      int[] valueColumns,
      int[] keyColumns,
      int frameKind,
      long frameOffset,
      boolean proctime,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates an event-time sorter over the given rowtime column and returns an opaque handle. Each
   * input batch is buffered; on a watermark the sorter emits the rows whose rowtime is at or before
   * it, ascending by rowtime (stable for ties), and keeps the rest. Released with {@link
   * #closeTemporalSorter}.
   */
  public static native long createTemporalSorter(int rtColumn, long memoryBudgetBytes);

  /** Buffers an input batch; rows are emitted in rowtime order as watermarks complete them. */
  public static native void pushTemporalSorter(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Exports the rows the watermark has completed, sorted ascending by rowtime. */
  public static native void flushTemporalSorter(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases the event-time sorter and its buffered rows. */
  public static native void closeTemporalSorter(long handle);

  /** Serializes the sorter's buffered rows for a checkpoint. */
  public static native byte[] snapshotTemporalSorter(long handle);

  /** Rebuilds an event-time sorter from a snapshot and returns a fresh handle. */
  public static native long restoreTemporalSorter(
      int rtColumn, byte[] snapshot, long memoryBudgetBytes);

  /**
   * Creates a keep-first deduplicator over the partition-key columns and rowtime column, and returns
   * an opaque handle. Each input batch is buffered; on a watermark the deduplicator emits each key's
   * minimum-rowtime row (insert-only) once the watermark reaches that rowtime, and drops every later
   * row for the key. Released with {@link #closeKeepFirstDeduplicator}.
   */
  public static native long createKeepFirstDeduplicator(
      int[] partitionColumns, int rtColumn, long memoryBudgetBytes);

  /** Buffers an input batch; each key's first row is emitted on the watermark that reaches it. */
  public static native void pushKeepFirstDeduplicator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached. */
  public static native void flushKeepFirstDeduplicator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases the deduplicator and its per-key state. */
  public static native void closeKeepFirstDeduplicator(long handle);

  /** Serializes the deduplicator's pending candidates, emitted keys, and watermark for a checkpoint. */
  public static native byte[] snapshotKeepFirstDeduplicator(long handle);

  /** Rebuilds a keep-first deduplicator from a snapshot and returns a fresh handle. */
  public static native long restoreKeepFirstDeduplicator(
      int[] partitionColumns, int rtColumn, byte[] snapshot, long memoryBudgetBytes);

  /**
   * Creates an eager (push→emit) deduplicator and returns an opaque handle. Per partition key, keep-
   * last keeps the winning row and emits a retract changelog eagerly per input row (first row {@code
   * +I}; a replacement emits {@code -U}(previous, gated on {@code generateUpdateBefore})/{@code +U}
   * (new)); keep-first emits the first row per key ({@code +I}, insert-only) and drops the rest. A
   * rowtime order ({@code rowtimeOrdered}) reads the rowtime column and keeps the max-rowtime row;
   * proctime uses arrival order (no rowtime read). Released with {@link #closeKeepLastDeduplicator}.
   */
  public static native long createKeepLastDeduplicator(
      int[] partitionColumns,
      int rtColumn,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst,
      long memoryBudgetBytes);

  /** Folds an input batch and returns the changelog (or insert-only rows) it produces. */
  public static native void pushKeepLastDeduplicator(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Releases the deduplicator and its per-key state. */
  public static native void closeKeepLastDeduplicator(long handle);

  /** Serializes the deduplicator's per-key stored rows for a checkpoint. */
  public static native byte[] snapshotKeepLastDeduplicator(long handle);

  /** Rebuilds an eager deduplicator from a snapshot and returns a fresh handle. */
  public static native long restoreKeepLastDeduplicator(
      int[] partitionColumns,
      int rtColumn,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a window-rank ranker (window Top-N / window deduplication) over the attached
   * {@code window_start}/{@code window_end} columns and returns an opaque handle. Within each window
   * and partition key it keeps the {@code limit} rows ordered by the sort columns, emitting them
   * (with the rank number appended when {@code outputRankNumber}) once a watermark closes the window.
   * Released with {@link #closeWindowRanker}.
   */
  public static native long createWindowRanker(
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      long memoryBudgetBytes);

  /** Buffers an input batch; each window's top-N rows are emitted when a watermark closes it. */
  public static native void pushWindowRanker(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Exports the top-N rows of every window the watermark has closed. */
  public static native void flushWindowRanker(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases the window-rank ranker and its per-window state. */
  public static native void closeWindowRanker(long handle);

  /** Serializes the ranker's per-window buffers and watermark for a checkpoint. */
  public static native byte[] snapshotWindowRanker(long handle);

  /** Rebuilds a window-rank ranker from a snapshot and returns a fresh handle. */
  public static native long restoreWindowRanker(
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a non-windowed {@code GROUP BY} aggregator and returns an opaque handle. Each input batch
   * folds into per-key state and the aggregator exports the changelog rows it produces, with the row
   * kinds carried on the {@code $row_kind$} column. Released with {@link #closeGroupAggregator}.
   *
   * @param aggregateKinds aggregate codes (see {@link #createTumblingAggregator})
   * @param valueTypes per-aggregate value-column types (see {@link #createTumblingAggregator})
   * @param valueColumns per-aggregate value-column index in the input batch ({@code -1} for COUNT(*))
   * @param keyColumns grouping-key column indices in the input batch (empty for global aggregation)
   * @param generateUpdateBefore whether to emit an UPDATE_BEFORE row before each UPDATE_AFTER
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createGroupAggregator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      boolean generateUpdateBefore,
      long memoryBudgetBytes);

  /**
   * Folds an input batch into per-key state, exporting the changelog rows it produces (grouping keys,
   * aggregate results, then the {@code $row_kind$} byte column) into the consumer-allocated C structs.
   */
  public static native void updateGroupAggregator(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Releases a {@code GROUP BY} aggregator handle. */
  public static native void closeGroupAggregator(long handle);

  /** Serializes a {@code GROUP BY} aggregator's per-key state for a checkpoint. */
  public static native byte[] snapshotGroupAggregator(long handle);

  /** Rebuilds a {@code GROUP BY} aggregator from a snapshot and returns a fresh handle. */
  public static native long restoreGroupAggregator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      boolean generateUpdateBefore,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a changelog normalizer (keep-last per unique key) and returns an opaque handle. Each
   * input changelog batch folds into per-key state and the normalizer exports the normalized
   * changelog (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE on the {@code $row_kind$} column). Released
   * with {@link #closeChangelogNormalizer}.
   *
   * @param keyColumns unique-key column indices in the input batch
   * @param generateUpdateBefore whether to emit an UPDATE_BEFORE row before each UPDATE_AFTER
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createChangelogNormalizer(
      int[] keyColumns, boolean generateUpdateBefore, long memoryBudgetBytes);

  /**
   * Folds an input changelog batch into per-key keep-last state, exporting the normalized changelog
   * (the input columns then the {@code $row_kind$} byte column) into the consumer-allocated C structs.
   */
  public static native void pushChangelogNormalizer(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Serializes a changelog normalizer's per-key state for a checkpoint. */
  public static native byte[] snapshotChangelogNormalizer(long handle);

  /** Rebuilds a changelog normalizer from a snapshot and returns a fresh handle. */
  public static native long restoreChangelogNormalizer(
      int[] keyColumns, boolean generateUpdateBefore, byte[] snapshot, long memoryBudgetBytes);

  /** Releases a changelog normalizer handle. */
  public static native void closeChangelogNormalizer(long handle);

  /**
   * Creates the single format-dispatched message decoder shared by every ingest path, released with
   * {@link #closeDecoder}. It turns a batch of one binary column of raw message bodies into a typed
   * batch — the format-decode core both the shallow and native Kafka paths feed bytes into. Stateless,
   * so no snapshot/restore.
   *
   * @param format 0 = JSON, 2 = CSV, 3 = raw, 6 = debezium-json, 7 = ogg-json, 8 = maxwell-json,
   *     9 = canal-json (all decoded against the schema C structs; the CDC formats append a
   *     {@code $row_kind$} byte), 1 = Confluent Avro, 4 = bare Avro
   * @param schemaArrayAddress address of an exported (empty) {@code ArrowArray} of the target schema
   * @param schemaAddress address of the matching exported {@code ArrowSchema}
   * @param avroSchema writer-schema JSON for Avro (ignored for JSON; pass "")
   * @param readerAvroSchema reader-schema JSON projecting the Avro writer record to a subset of fields
   *     via Avro resolution (the query's columns); pass "" for no projection / non-Avro
   * @param schemaId Confluent schema id the Avro writer schema is registered under (ignored for JSON)
   */
  public static native long createDecoder(
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      String readerAvroSchema,
      int schemaId);

  /**
   * Creates a protobuf message decoder (Flink's {@code protobuf} format: bare message bytes, no
   * Confluent framing), returning a {@link MessageDecoder} handle released with {@link #closeDecoder}.
   * The Arrow batch schema is derived from the descriptor (no schema C-structs, unlike JSON).
   *
   * @param descriptor an encoded protobuf {@code FileDescriptorSet} the JVM serialized off the
   *     generated message class (the message's {@code .proto} file + its transitive dependencies)
   * @param messageName the fully-qualified message type to decode each body as
   * @param schemaArrayAddress address of an exported (empty) {@code ArrowArray} of the projected output
   *     schema, used to prune the descriptor to the read fields; 0 for no projection (decode in full)
   * @param schemaAddress address of the matching exported {@code ArrowSchema}, or 0
   */
  public static native long createProtobufDecoder(
      byte[] descriptor, String messageName, long schemaArrayAddress, long schemaAddress);

  /** Decodes one binary-column body batch into a typed batch, exported into the output C structs. */
  public static native void decodeInto(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress);

  /**
   * Benchmark-only: decode a body batch and return the decoded row count without exporting the result,
   * so the shallow path terminates with Arrow in Rust (symmetric with the native consumer).
   */
  public static native long decodeCount(long handle, long inArrayAddress, long inSchemaAddress);

  /** Releases a message decoder handle. */
  public static native void closeDecoder(long handle);

  /**
   * Benchmark-only: consume an entire topic with a native rdkafka consumer and decode it to typed
   * Arrow entirely in native code (payloads go from librdkafka straight into an Arrow builder — no JVM
   * heap byte[] and no per-record JNI crossing), returning the decoded row count. The JVM times this
   * one call to compare native consume+decode against the shallow path. Not the production source —
   * no enumerator/offset/config-fidelity work (see the native-source todo).
   *
   * @param brokers bootstrap servers
   * @param topic topic to consume from the beginning
   * @param schemaArrayAddress address of an exported (empty) {@code ArrowArray} of the target schema
   * @param schemaAddress address of the matching exported {@code ArrowSchema}
   * @param maxMessages stop after consuming this many messages
   */
  public static native long benchmarkKafkaConsume(
      String brokers, String topic, long schemaArrayAddress, long schemaAddress, long maxMessages);

  /**
   * Benchmark-only: drive the production split reader over a topic and count decoded rows entirely in
   * Rust (no per-batch export to the JVM), as the source would feed a downstream native operator.
   * Returns the row count.
   */
  public static native long benchmarkNativeConsume(
      String[] configKeys,
      String[] configValues,
      String topic,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      int schemaId,
      long maxMessages);

  /**
   * Benchmark-only: the serial counterpart to {@link #benchmarkNativeConsume} — same rdkafka consume and
   * decoder, but decode runs inline on the consume thread (no decode thread). Returns the row count.
   */
  public static native long benchmarkNativeConsumeSerial(
      String[] configKeys,
      String[] configValues,
      String topic,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      int schemaId,
      long maxMessages);

  /**
   * Benchmark-only: measure librdkafka's raw delivery rate — batch-consume and count messages with no
   * decode, to compare the consumer alone against the Java client's poll. Returns the message count.
   */
  public static native long benchmarkConsumeOnly(
      String[] configKeys, String[] configValues, String topic, long maxMessages);

  /**
   * Opens a native Kafka split reader for one subtask and returns an opaque handle, released with
   * {@link #closeKafkaConsumer}. One rdkafka consumer multiplexes the subtask's partitions; splits are
   * added later with {@link #assignKafkaSplits} as the enumerator assigns them. The reader manually
   * assigns + seeks (never subscribe/rebalance), mirroring Flink's {@code KafkaPartitionSplitReader}.
   *
   * @param configKeys translated librdkafka config keys (from {@code KafkaConfigTranslator})
   * @param configValues values index-aligned with {@code configKeys}, applied verbatim
   * @param format decoder: 0 = JSON (against the schema C structs), 1 = Confluent Avro, 4 = bare Avro,
   *     5 = protobuf — the same codes the shallow decode path uses
   * @param schemaArrayAddress address of an exported (empty) {@code ArrowArray} of the decoder's schema
   * @param schemaAddress address of the matching exported {@code ArrowSchema}
   * @param avroSchema writer-schema JSON for Avro (ignored for JSON/protobuf; pass "")
   * @param readerAvroSchema narrowed reader-schema JSON for a projected bare-Avro decode (else "")
   * @param schemaId Confluent schema id the Avro writer schema is registered under (ignored for JSON)
   * @param descriptor encoded protobuf {@code FileDescriptorSet} for format 5 (else {@code null})
   * @param messageName fully-qualified protobuf message type for format 5 (else "")
   */
  public static native long openKafkaConsumer(
      String[] configKeys,
      String[] configValues,
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      String readerAvroSchema,
      int schemaId,
      byte[] descriptor,
      String messageName);

  /**
   * Adds splits to the reader and re-assigns the consumer: each new partition seeks to its start
   * offset, each already-assigned one keeps its tracked position. Index-aligned arrays.
   *
   * @param handle reader handle from {@link #openKafkaConsumer}
   * @param topics split topics
   * @param partitions split partition ids
   * @param startOffsets offset to assign+seek each new split to (its checkpointed resume position)
   */
  public static native void assignKafkaSplits(
      long handle, String[] topics, long[] partitions, long[] startOffsets);

  /**
   * Removes finished splits (reached their bounded stopping offset) from the consumer's assignment so
   * it no longer fetches or blocks on them. Index-aligned {@code topics}/{@code partitions}.
   */
  public static native void unassignKafkaSplits(long handle, String[] topics, long[] partitions);

  /**
   * Polls one cycle, decoding one Arrow batch per partition that had messages. Returns the number of
   * per-partition batches now pending; drain each with {@link #drainKafkaSplit}.
   *
   * @param handle reader handle from {@link #openKafkaConsumer}
   * @param maxRecords cap on messages per poll (the native batch size; Java's {@code max.poll.records})
   * @param timeoutMillis poll timeout; returns 0 if nothing arrives within it
   */
  public static native int pollKafkaBatch(long handle, int maxRecords, long timeoutMillis);

  /**
   * Drains one pending per-partition batch: exports typed Arrow into the consumer C structs, writes
   * {@code [partition, nextOffset]} into {@code splitMeta}, and the topic into {@code outTopic[0]} so
   * the JVM can form the split id and advance that split's checkpoint offset. Returns the decoded row
   * count. Call it {@link #pollKafkaBatch}'s return-value times.
   *
   * @param handle reader handle from {@link #openKafkaConsumer}
   * @param splitMeta output {@code long[2]}: the partition id and the next offset for that split
   * @param outTopic output {@code String[1]}: the topic of the drained split
   * @param outArrayAddress address of a consumer {@code ArrowArray} to receive the decoded batch
   * @param outSchemaAddress address of the matching {@code ArrowSchema}
   */
  public static native int drainKafkaSplit(
      long handle, long[] splitMeta, String[] outTopic, long outArrayAddress, long outSchemaAddress);

  /** Releases a native Kafka split reader, closing the rdkafka consumer's connections. */
  public static native void closeKafkaConsumer(long handle);

  /**
   * Creates an event-time INNER interval joiner and returns an opaque handle. It buffers both inputs
   * per equi-join key and emits a matched pair when the second of its two rows arrives. The JVM owns
   * the handle across calls and must release it with {@link #closeIntervalJoiner}.
   *
   * @param leftKeys equi-join key column indices in the left input batch
   * @param rightKeys equi-join key column indices in the right input batch
   * @param leftTime rowtime column index in the left input batch
   * @param rightTime rowtime column index in the right input batch
   * @param lowerMillis inclusive lower bound on {@code left.rt - right.rt}
   * @param upperMillis inclusive upper bound on {@code left.rt - right.rt}
   * @param joinType 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL (outer pads unmatched rows at eviction)
   * @param leftSchemaAddress C Data Interface address of the left input's (data-only) Arrow schema
   * @param rightSchemaAddress C Data Interface address of the right input's (data-only) Arrow schema
   * @param predKinds residual non-equi predicate over the joined {@code [left.., right..]} row (empty
   *     ⇒ none), ANDed with the interval bounds; same encoding {@link #createFilterExpression} takes
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createIntervalJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long lowerMillis,
      long upperMillis,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      long memoryBudgetBytes);

  /**
   * Pushes a left batch, exporting the matched pairs (left columns then right columns). For a
   * proctime join {@code proctime} is set and every row's time is stamped with {@code
   * proctimeNowMillis} (the operator clock) instead of read from the time column.
   */
  public static native void pushLeftIntervalJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      boolean proctime,
      long proctimeNowMillis);

  /** Pushes a right batch, exporting the matched pairs (left columns then right columns). */
  public static native void pushRightIntervalJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      boolean proctime,
      long proctimeNowMillis);

  /**
   * Advances the combined watermark, evicting rows no future arrival can match, and exporting the
   * null-padded rows for evicted outer rows that never matched (empty for an INNER join).
   */
  public static native void advanceIntervalJoiner(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases an interval joiner handle. */
  public static native void closeIntervalJoiner(long handle);

  /** Serializes an interval joiner's buffered rows for a checkpoint. */
  public static native byte[] snapshotIntervalJoiner(long handle);

  /** Rebuilds an interval joiner from a snapshot and returns a fresh handle. */
  public static native long restoreIntervalJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long lowerMillis,
      long upperMillis,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates an event-time temporal-table joiner ({@code FOR SYSTEM_TIME AS OF probe.rowtime}) and
   * returns an opaque handle. The right input is a versioned changelog keyed by the equi-join key; a
   * probe row is buffered until the watermark passes its time, then joined against the build version
   * valid at that time. The JVM owns the handle and must release it with {@link #closeTemporalJoiner}.
   *
   * @param leftKeys equi-join key column indices in the probe (left) input batch
   * @param rightKeys equi-join key column indices in the build (right) input batch
   * @param leftTime rowtime column index in the probe input batch
   * @param rightTime rowtime column index in the build input batch
   * @param joinType 0=INNER or 1=LEFT (a LEFT join null-pads a probe row with no valid build version)
   * @param leftSchemaAddress C Data Interface address of the left input's (data-only) Arrow schema
   * @param rightSchemaAddress C Data Interface address of the right input's (data-only) Arrow schema
   * @param predKinds residual non-equi predicate over the joined {@code [left.., right..]} row (empty
   *     ⇒ none); same encoding {@link #createFilterExpression} takes
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createTemporalJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      long memoryBudgetBytes);

  /** Buffers a probe-side (left) batch (no output until a watermark). */
  public static native void pushLeftTemporalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Folds a build-side (right) changelog batch into the versioned state (no output until a watermark). */
  public static native void pushRightTemporalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Advances the watermark, exporting the joined rows ({@code [left.., right..]} with a trailing
   * {@code $row_kind$}) for the buffered probe rows it has passed.
   */
  public static native void advanceTemporalJoiner(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases a temporal joiner handle. */
  public static native void closeTemporalJoiner(long handle);

  /** Serializes a temporal joiner's buffered probe rows and versioned build state for a checkpoint. */
  public static native byte[] snapshotTemporalJoiner(long handle);

  /** Rebuilds a temporal joiner from a snapshot and returns a fresh handle. */
  public static native long restoreTemporalJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a regular (non-windowed) updating joiner and returns an opaque handle. It keeps a
   * per-side keyed multiset of live rows and, on each input row, emits the join changelog against the
   * other side (carrying the input row's kind from the trailing {@code $row_kind$} column). For
   * LEFT/RIGHT/FULL outer and SEMI/ANTI it also tracks a per-row match-degree on the outer side to
   * emit/retract null-padded (outer) or bare (semi/anti) rows. The JVM owns the handle and must
   * release it with {@link #closeUpdatingJoiner}.
   *
   * @param leftKeys equi-join key column indices in the left input batch
   * @param rightKeys equi-join key column indices in the right input batch
   * @param joinType 0=INNER, 1=LEFT, 2=RIGHT, 3=FULL, 4=SEMI, 5=ANTI
   * @param leftSchemaAddress C Data Interface address of the left input's (data-only) Arrow schema
   * @param rightSchemaAddress C Data Interface address of the right input's (data-only) Arrow schema
   * @param predKinds residual non-equi predicate, encoded over the joined {@code [left.., right..]}
   *     row (empty {@code predKinds} ⇒ no predicate); the {@code pred*} arrays are the same encoding
   *     {@link #createFilterExpression} consumes
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createUpdatingJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      long memoryBudgetBytes);

  /** Pushes a left batch, exporting the join changelog (left columns, right columns, row kind). */
  public static native void pushLeftUpdatingJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Pushes a right batch, exporting the join changelog (left columns, right columns, row kind). */
  public static native void pushRightUpdatingJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Releases an updating joiner handle. */
  public static native void closeUpdatingJoiner(long handle);

  /** Serializes an updating joiner's per-side state for a checkpoint. */
  public static native byte[] snapshotUpdatingJoiner(long handle);

  /** Rebuilds an updating joiner from a snapshot and returns a fresh handle. */
  public static native long restoreUpdatingJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates an append-only streaming Top-N ranker ({@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY
   * …) <= limit}, rank number not projected). Per partition it keeps the top {@code limit} rows by
   * the order keys and emits an INSERT for a row entering the top-N and a DELETE for one displaced.
   * The JVM owns the handle and must release it with {@link #closeTopNRanker}.
   *
   * @param partitionColumns PARTITION BY column indices (empty for a single global partition)
   * @param sortIndices ORDER BY column indices, in order
   * @param sortAscending per sort column, 1 if ascending else 0
   * @param sortNullsFirst per sort column, 1 if nulls sort first else 0
   * @param limit the rank bound N
   * @param outputRankNumber whether the rank column is projected (the operator then emits the
   *     shift cascade and appends the rank); false for the plain Top-N and the global LIMIT
   * @param retracting whether the input is a changelog (use the retracting ranker, which keeps the
   *     full buffer to promote on delete) rather than insert-only (the append-only bounded ranker)
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createTopNRanker(
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      long memoryBudgetBytes);

  /** Pushes an input batch, exporting the top-N changelog (input columns plus the row kind). */
  public static native void pushTopNRanker(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Releases a Top-N ranker handle. */
  public static native void closeTopNRanker(long handle);

  /** Serializes a Top-N ranker's bounded per-partition buffers for a checkpoint. */
  public static native byte[] snapshotTopNRanker(long handle);

  /** Rebuilds a Top-N ranker from a snapshot and returns a fresh handle. */
  public static native long restoreTopNRanker(
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates an event-time INNER window joiner and returns an opaque handle. It buffers both inputs
   * (whose rows carry matching {@code window_start}/{@code window_end} columns assigned upstream) and
   * joins them per window when the watermark closes it. The JVM owns the handle across calls and must
   * release it with {@link #closeWindowJoiner}.
   *
   * @param leftKeys equi-join key column indices in the left input batch
   * @param rightKeys equi-join key column indices in the right input batch
   * @param leftWindowStart window-start column index in the left input batch
   * @param leftWindowEnd window-end column index in the left input batch
   * @param rightWindowStart window-start column index in the right input batch
   * @param rightWindowEnd window-end column index in the right input batch
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createWindowJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      long memoryBudgetBytes);

  /** Buffers a left batch; its rows are joined when a watermark closes their window. */
  public static native void pushLeftWindowJoiner(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Buffers a right batch. */
  public static native void pushRightWindowJoiner(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Exports the INNER matches (left columns then right columns) of every window the watermark has
   * closed into the consumer-allocated C structs, then evicts those windows (empty batch if none).
   */
  public static native void flushWindowJoiner(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases a window joiner handle. */
  public static native void closeWindowJoiner(long handle);

  /** Serializes a window joiner's buffered rows for a checkpoint. */
  public static native byte[] snapshotWindowJoiner(long handle);

  /** Rebuilds a window joiner from a snapshot and returns a fresh handle. */
  public static native long restoreWindowJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a stateful cumulative-window aggregator and returns an opaque handle. Cumulative windows
   * are nested windows of {@code stepMillis} growing up to {@code maxSizeMillis}, all sharing a
   * start. It shares the aligned-window engine — {@link #updateTumblingAggregator}, {@link
   * #flushTumblingAggregator}, {@link #snapshotTumblingAggregator}, {@link
   * #closeTumblingAggregator} all apply to the returned handle; only the window assignment differs.
   *
   * @param maxSizeMillis the full (maximum) window size in milliseconds
   * @param stepMillis the step between successive cumulative window ends
   * @param valueTypes value-column type per aggregate (see {@link #createTumblingAggregator})
   * @param aggregateKinds one code per aggregate: 0=SUM, 1=MIN, 2=MAX, 3=COUNT, 4=AVG
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createCumulativeAggregator(
      long maxSizeMillis,
      long stepMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      long memoryBudgetBytes);

  /** Rebuilds a cumulative-window aggregator from a snapshot and returns a fresh handle. */
  public static native long restoreCumulativeAggregator(
      long maxSizeMillis,
      long stepMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      byte[] snapshot,
      long memoryBudgetBytes);

  /**
   * Creates a stateful session-window aggregator and returns an opaque handle, released with {@link
   * #closeSessionAggregator(long)}. Sessions are dynamic per-key windows that merge on the gap, so
   * there is no fixed size or slide.
   *
   * @param gapMillis the inactivity gap in milliseconds that separates sessions
   * @param valueTypes value-column type per aggregate (see {@link #createTumblingAggregator})
   * @param aggregateKinds one code per aggregate: 0=SUM, 1=MIN, 2=MAX, 3=COUNT, 4=AVG
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createSessionAggregator(
      long gapMillis, int[] valueTypes, int[] aggregateKinds, long memoryBudgetBytes);

  /**
   * Folds a batch (columns {@code ts}, {@code value}, optional {@code key}) into the aggregator's
   * sessions, merging any the new elements bridge. Closed sessions are emitted by {@link
   * #flushSessionAggregator}.
   */
  public static native void updateSessionAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /**
   * Emits the sessions the watermark has closed as a batch (columns {@code key}, {@code
   * window_start}, {@code window_end}, {@code result0..}) and drops them from state.
   */
  public static native void flushSessionAggregator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  /** Releases a session aggregator handle and its native state. */
  public static native void closeSessionAggregator(long handle);

  /** Serializes a session aggregator's open sessions so they can be stored in a checkpoint. */
  public static native byte[] snapshotSessionAggregator(long handle);

  /**
   * Rebuilds a session aggregator from a snapshot and returns a fresh handle.
   *
   * @param gapMillis the inactivity gap, supplied again since it is configuration, not state
   * @param valueTypes value-column type per aggregate (see {@link #createSessionAggregator})
   * @param aggregateKinds aggregate codes (see {@link #createSessionAggregator})
   * @param snapshot bytes produced by {@link #snapshotSessionAggregator(long)}
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long restoreSessionAggregator(
      long gapMillis, int[] valueTypes, int[] aggregateKinds, byte[] snapshot, long memoryBudgetBytes);
}
