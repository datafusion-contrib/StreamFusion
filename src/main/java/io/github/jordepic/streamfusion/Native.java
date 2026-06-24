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
   * Opens a directory of Parquet files for reading and returns an opaque handle. The handle yields
   * batches one at a time via {@link #nextBatch} and must be released with {@link #closeParquet}.
   *
   * @param directory directory of Parquet files to read
   * @param projection output column names, in the order the plan expects (honoring projection
   *     pushdown); an empty array emits every column as read
   * @param subtask this reader's subtask index; it reads the sorted files congruent to it modulo
   *     {@code numSubtasks}, so a parallel read covers every file once
   * @param numSubtasks the source parallelism
   */
  public static native long openParquet(
      String directory, String[] projection, int subtask, int numSubtasks);

  /**
   * Exports the next Arrow batch from a source handle into the consumer-allocated C structs.
   *
   * @param handle a handle from {@link #openParquet}
   * @param outArrayAddress address of the consumer-allocated output {@code ArrowArray} C struct
   * @param outSchemaAddress address of the consumer-allocated output {@code ArrowSchema} C struct
   * @return true if a batch was produced, false once the directory is exhausted
   */
  public static native boolean nextBatch(long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases a Parquet source handle. */
  public static native void closeParquet(long handle);

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
   */
  public static native long createTumblingAggregator(
      long windowMillis, long slideMillis, int[] valueTypes, int[] aggregateKinds);

  /**
   * Folds a batch (columns {@code ts} and {@code value}) into the aggregator's open windows.
   * Produces no output; closed windows are emitted by {@link #flushTumblingAggregator}.
   */
  public static native void updateTumblingAggregator(
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
   */
  public static native long restoreTumblingAggregator(
      long windowMillis, long slideMillis, int[] valueTypes, int[] aggregateKinds, byte[] snapshot);

  /**
   * Creates a columnar event-time OVER aggregator (RANGE between unbounded preceding and current
   * row): it buffers input batches and, on a watermark, emits the completed rows with the running
   * aggregate(s) appended. Released with {@link #closeOverAggregator}.
   *
   * @param valueType value column type (see {@link #createTumblingAggregator})
   * @param aggregateKinds aggregate codes (see {@link #createTumblingAggregator})
   * @param rtColumn rowtime column index in the input batch
   * @param valueColumn value column index in the input batch
   * @param keyColumns PARTITION BY column indices in the input batch (empty for no partition)
   */
  public static native long createOverAggregator(
      int valueType, int[] aggregateKinds, int rtColumn, int valueColumn, int[] keyColumns);

  /** Buffers an input batch; its rows are emitted later when a watermark completes them. */
  public static native void pushOverAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

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
      int valueType,
      int[] aggregateKinds,
      int rtColumn,
      int valueColumn,
      int[] keyColumns,
      byte[] snapshot);

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
   */
  public static native long createGroupAggregator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      boolean generateUpdateBefore);

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
      boolean generateUpdateBefore,
      byte[] snapshot);

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
   */
  public static native long createIntervalJoiner(
      int[] leftKeys, int[] rightKeys, int leftTime, int rightTime, long lowerMillis, long upperMillis);

  /** Pushes a left batch, exporting the matched pairs (left columns then right columns). */
  public static native void pushLeftIntervalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Pushes a right batch, exporting the matched pairs (left columns then right columns). */
  public static native void pushRightIntervalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Advances the combined watermark, evicting rows no future arrival can match. */
  public static native void advanceIntervalJoiner(long handle, long watermarkMillis);

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
      byte[] snapshot);

  /**
   * Creates a regular (non-windowed) INNER updating joiner and returns an opaque handle. It keeps a
   * per-side keyed multiset of live rows and, on each input row, emits the join changelog against the
   * other side (carrying the input row's kind from the trailing {@code $row_kind$} column). The JVM
   * owns the handle and must release it with {@link #closeUpdatingJoiner}.
   *
   * @param leftKeys equi-join key column indices in the left input batch
   * @param rightKeys equi-join key column indices in the right input batch
   */
  public static native long createUpdatingJoiner(int[] leftKeys, int[] rightKeys);

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
  public static native long restoreUpdatingJoiner(int[] leftKeys, int[] rightKeys, byte[] snapshot);

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
   */
  public static native long createWindowJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd);

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
      byte[] snapshot);

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
   */
  public static native long createCumulativeAggregator(
      long maxSizeMillis, long stepMillis, int[] valueTypes, int[] aggregateKinds);

  /** Rebuilds a cumulative-window aggregator from a snapshot and returns a fresh handle. */
  public static native long restoreCumulativeAggregator(
      long maxSizeMillis, long stepMillis, int[] valueTypes, int[] aggregateKinds, byte[] snapshot);

  /**
   * Creates a stateful session-window aggregator and returns an opaque handle, released with {@link
   * #closeSessionAggregator(long)}. Sessions are dynamic per-key windows that merge on the gap, so
   * there is no fixed size or slide.
   *
   * @param gapMillis the inactivity gap in milliseconds that separates sessions
   * @param valueTypes value-column type per aggregate (see {@link #createTumblingAggregator})
   * @param aggregateKinds one code per aggregate: 0=SUM, 1=MIN, 2=MAX, 3=COUNT, 4=AVG
   */
  public static native long createSessionAggregator(
      long gapMillis, int[] valueTypes, int[] aggregateKinds);

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
   */
  public static native long restoreSessionAggregator(
      long gapMillis, int[] valueTypes, int[] aggregateKinds, byte[] snapshot);
}
