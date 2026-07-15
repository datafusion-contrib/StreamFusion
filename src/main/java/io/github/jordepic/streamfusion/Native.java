package io.github.jordepic.streamfusion;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;

/** Entry point to the native data plane. Holds the methods backed by the Rust library. */
public final class Native {

  private static final String LIBRARY_NAME = "streamfusion";
  private static final String NATIVE_RESOURCE_PREFIX =
      "/io/github/jordepic/streamfusion/native/";

  static {
    loadLibrary();
  }

  private Native() {}

  private static void loadLibrary() {
    try {
      System.loadLibrary(LIBRARY_NAME);
    } catch (UnsatisfiedLinkError libraryPathFailure) {
      loadBundledLibrary(libraryPathFailure);
    }
  }

  private static void loadBundledLibrary(UnsatisfiedLinkError libraryPathFailure) {
    for (String resourcePath : bundledLibraryResourcePaths()) {
      try (InputStream stream = Native.class.getResourceAsStream(resourcePath)) {
        if (stream == null) {
          continue;
        }

        String libraryFileName = System.mapLibraryName(LIBRARY_NAME);
        String suffix = libraryFileName.substring(libraryFileName.lastIndexOf('.'));
        Path extractedLibrary = Files.createTempFile("streamfusion-", suffix);
        try {
          Files.copy(stream, extractedLibrary, StandardCopyOption.REPLACE_EXISTING);
          System.load(extractedLibrary.toAbsolutePath().toString());
          return;
        } finally {
          extractedLibrary.toFile().deleteOnExit();
        }
      } catch (IOException e) {
        UnsatisfiedLinkError error =
            new UnsatisfiedLinkError("Unable to extract bundled StreamFusion native library: " + e);
        error.initCause(e);
        throw error;
      }
    }

    UnsatisfiedLinkError error =
        new UnsatisfiedLinkError(
            "No bundled StreamFusion library for "
                + nativePlatform()
                + "/"
                + nativeArchitecture()
                + ". Tried "
                + String.join(", ", bundledLibraryResourcePaths()));
    error.initCause(libraryPathFailure);
    throw error;
  }

  static String bundledLibraryResourcePath() {
    return NATIVE_RESOURCE_PREFIX
        + nativePlatform()
        + "/"
        + nativeArchitecture()
        + "/"
        + System.mapLibraryName(LIBRARY_NAME);
  }

  static List<String> bundledLibraryResourcePaths() {
    return List.of(
        bundledLibraryResourcePath(), NATIVE_RESOURCE_PREFIX + System.mapLibraryName(LIBRARY_NAME));
  }

  static String nativePlatform() {
    return platformName(System.getProperty("os.name"));
  }

  static String nativeArchitecture() {
    return architectureName(System.getProperty("os.arch"));
  }

  static String platformName(String osName) {
    String normalized = osName.toLowerCase(Locale.ROOT);
    if (normalized.contains("linux")) {
      return "linux";
    }
    if (normalized.contains("mac") || normalized.contains("darwin")) {
      return "darwin";
    }
    throw new UnsupportedOperationException("Unsupported StreamFusion operating system: " + osName);
  }

  static String architectureName(String architecture) {
    String normalized = architecture.toLowerCase(Locale.ROOT);
    if (normalized.equals("amd64") || normalized.equals("x86_64") || normalized.equals("x64")) {
      return "x86_64";
    }
    if (normalized.equals("aarch64") || normalized.equals("arm64")) {
      return "aarch64";
    }
    throw new UnsupportedOperationException(
        "Unsupported StreamFusion architecture: " + architecture);
  }

  /** Version reported by the loaded native library, proving the JVM↔Rust bridge is live. */
  public static native String version();

  /**
   * The native side's live-handle breakdown by type (e.g. {@code SessionAggregator=1}), empty once
   * every handle has been closed. The test harness asserts this drains to empty after each test, so
   * a missing close call fails the test naming the leaking type instead of slowly growing RSS.
   */
  public static native String liveNativeHandles();

  // A stateful operator's tracked native state footprint in bytes (zero when unaccounted), one
  // getter per handle type. Handles are not thread-safe, so operators sample these on the task
  // thread after each batch and publish the value to their metric gauges (ManagedMemoryBudget).
  public static native long tumblingAggregatorStateBytes(long handle);

  public static native long sessionAggregatorStateBytes(long handle);

  public static native long groupAggregatorStateBytes(long handle);

  public static native long groupAggregatorStagingBytes(long handle);

  public static native long groupAggregatorStagedKeys(long handle);

  public static native long localGroupAggregatorStateBytes(long handle);

  public static native long overAggregatorStateBytes(long handle);

  public static native long temporalSorterStateBytes(long handle);

  public static native long keepFirstDeduplicatorStateBytes(long handle);

  public static native long keepLastDeduplicatorStateBytes(long handle);

  public static native long keepLastDeduplicatorStagingBytes(long handle);

  public static native long keepLastDeduplicatorStagedKeys(long handle);

  public static native long updatingJoinerStagingBytes(long handle);

  public static native long updatingJoinerStagedKeys(long handle);

  public static native long windowRankerStateBytes(long handle);

  public static native long intervalJoinerStateBytes(long handle);

  public static native long temporalJoinerStateBytes(long handle);

  public static native long updatingJoinerStateBytes(long handle);

  public static native long topNRankerStateBytes(long handle);

  public static native long topNRankerStagingBytes(long handle);

  public static native long topNRankerStagedPartitions(long handle);

  public static native long changelogNormalizerStateBytes(long handle);

  public static native long changelogNormalizerStagingBytes(long handle);

  public static native long changelogNormalizerStagedKeys(long handle);

  public static native long windowJoinerStateBytes(long handle);

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
   * Computes Flink's {@code BinaryRowData.hashCode()} for every Arrow row projected to the supplied
   * key columns. Timestamp precision is carried separately because Arrow's timestamp type has no
   * precision parameter; use {@code -1} for every non-timestamp key column.
   */
  public static native int[] flinkBinaryRowHashes(
      long arrayAddress,
      long schemaAddress,
      int[] keyColumns,
      int[] timestampPrecisions);

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
   * MAX 2, COUNT 3, COUNT(DISTINCT) 7, SUM(DISTINCT) 9) and emits one partial row per key ({@code
   * [key0.., partial0.., distinct-view0..]}, no {@code $row_kind$} — insert-only) when flushed. Each
   * {@code distinctViewSources} entry names the aggregate whose per-bundle distinct set backs the
   * corresponding trailing view column (the bundle's (value, count) entries as a list of structs, the
   * wire form of Flink's serialized MapView partial). The buffer is transient (drained before each
   * checkpoint by the operator), so there is no snapshot/restore — the global half keeps the durable
   * state. Released with {@link #closeLocalGroupAggregator(long)}.
   */
  public static native long createLocalGroupAggregator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] filterColumns,
      int[] keyColumns,
      int[] distinctViewSources,
      long memoryBudgetBytes);

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
   * Splits a batch the JVM exported using Flink's BinaryRow key hash and key-group assignment into up
   * to {@code numPartitions} sub-batches (every row with a given key in one partition), returning a
   * handle to pull them with {@link #nextSplit}; released with {@link #closeSplit}. The columnar
   * shuffle's by-key routing.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param keyColumns indices of the key columns to hash
   * @param timestampPrecisions logical timestamp precision per key column ({@code -1} for non-timestamp keys)
   * @param maxParallelism number of Flink key groups
   * @param numPartitions number of partitions (downstream channels) to split into
   */
  public static native long splitByKey(
      long inArrayAddress,
      long inSchemaAddress,
      int[] keyColumns,
      int[] timestampPrecisions,
      int maxParallelism,
      int numPartitions);

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

  /** Lists the non-empty Flink key groups in a fixed-window raw keyed-state checkpoint. */
  public static native int[] tumblingAggregatorSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one fixed-window key group for a raw keyed-state checkpoint. */
  public static native byte[] snapshotTumblingAggregatorKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Restores a tumbling, hopping, or cumulative window aggregator from assigned raw key groups. */
  public static native long restoreTumblingAggregatorPartitions(
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] valueTypes,
      int[] aggregateKinds,
      byte[][] snapshots,
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

  /** Lists the non-empty Flink key groups in an OVER raw keyed-state checkpoint. */
  public static native int[] overAggregatorSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one OVER key group for a raw keyed-state checkpoint. */
  public static native byte[] snapshotOverAggregatorKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

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

  /** Restores an OVER aggregator from raw keyed-state partitions assigned to this task. */
  public static native long restoreOverAggregatorPartitions(
      int[] valueTypes,
      int[] aggregateKinds,
      int rtColumn,
      int[] valueColumns,
      int[] keyColumns,
      int frameKind,
      long frameOffset,
      boolean proctime,
      byte[][] snapshots,
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
      int[] partitionColumns, int[] keyTimestampPrecisions, int rtColumn, long memoryBudgetBytes);

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

  /** Lists the non-empty Flink key groups in a keep-first deduplication raw keyed-state checkpoint. */
  public static native int[] keepFirstDeduplicatorSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one keep-first deduplication key group for a raw keyed-state checkpoint. */
  public static native byte[] snapshotKeepFirstDeduplicatorKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Restores a keep-first deduplicator from raw keyed-state partitions assigned to this subtask. */
  public static native long restoreKeepFirstDeduplicatorPartitions(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rtColumn,
      byte[][] snapshots,
      long memoryBudgetBytes);

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
      int[] keyTimestampPrecisions,
      int rtColumn,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      long memoryBudgetBytes);

  /** Folds an input batch and returns the changelog (or insert-only rows) it produces. */
  public static native void pushKeepLastDeduplicator(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  public static native void flushKeepLastDeduplicator(
      long handle, long outArrayAddress, long outSchemaAddress);

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
      boolean miniBatch,
      byte[] snapshot,
      long memoryBudgetBytes);

  /** Lists the non-empty Flink key groups in a keep-last deduplication raw keyed-state checkpoint. */
  public static native int[] keepLastDeduplicatorSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one keep-last deduplication key group for a raw keyed-state checkpoint. */
  public static native byte[] snapshotKeepLastDeduplicatorKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Restores a keep-last deduplicator from the raw keyed-state partitions assigned to this subtask. */
  public static native long restoreKeepLastDeduplicatorPartitions(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rtColumn,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      byte[][] snapshots,
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

  /** Lists the non-empty Flink key groups in a window-rank raw keyed-state checkpoint. */
  public static native int[] windowRankerSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one window-rank key group for a raw keyed-state checkpoint. */
  public static native byte[] snapshotWindowRankerKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Restores a window ranker from raw keyed-state partitions assigned to this subtask. */
  public static native long restoreWindowRankerPartitions(
      int windowStartColumn,
      int windowEndColumn,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      byte[][] snapshots,
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
   * @param keyTimestampPrecisions pre-order logical key type descriptors (timestamp precision or
   *     {@code -1}); this lets the native BinaryRow codec preserve nested timestamp layout
   * @param countColumns per-aggregate two-phase AVG count-partial column ({@code -1} otherwise): the
   *     value column is then the local's pre-summed sum partial, and each row bumps the count by this
   *     column instead of by one
   * @param distinctViewColumns per-aggregate two-phase distinct-view column ({@code -1} otherwise):
   *     the column carries a local bundle's distinct (value, count) entries as a list of structs,
   *     folded into the per-key distinct set with multiplicities instead of one value per row
   * @param recordCountColumn the count1 partial column of a retracting two-phase merge ({@code -1}
   *     otherwise): each row bumps the key's record count by this column instead of ±1, so a key
   *     whose merged count reaches zero is deleted ({@code -D})
   * @param generateUpdateBefore whether to emit an UPDATE_BEFORE row before each UPDATE_AFTER
   * @param memoryBudgetBytes managed-memory budget (see {@link #createTumblingAggregator})
   */
  public static native long createGroupAggregator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long memoryBudgetBytes);

  /**
   * Folds an input batch into per-key state, exporting the changelog rows it produces (grouping keys,
   * aggregate results, then the {@code $row_kind$} byte column) into the consumer-allocated C structs.
   */
  public static native void updateGroupAggregator(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Flushes the group changes staged across one logical mini-batch. */
  public static native void flushGroupAggregator(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases a {@code GROUP BY} aggregator handle. */
  public static native void closeGroupAggregator(long handle);

  /** Serializes a {@code GROUP BY} aggregator's per-key state for a checkpoint. */
  public static native byte[] snapshotGroupAggregator(long handle);

  /** Lists the non-empty Flink key groups in the group aggregator's raw keyed-state checkpoint. */
  public static native int[] groupAggregatorSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes the state of one group returned by {@link #groupAggregatorSnapshotKeyGroups}. */
  public static native byte[] snapshotGroupAggregatorKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Rebuilds a {@code GROUP BY} aggregator from a snapshot and returns a fresh handle. */
  public static native long restoreGroupAggregator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      boolean miniBatch,
      byte[] snapshot,
      long memoryBudgetBytes);

  /** Rebuilds a group aggregator from every raw keyed-state partition assigned to this subtask. */
  public static native long restoreGroupAggregatorPartitions(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      boolean miniBatch,
      byte[][] snapshots,
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
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long memoryBudgetBytes);

  /**
   * Folds an input changelog batch into per-key keep-last state, exporting the normalized changelog
   * (the input columns then the {@code $row_kind$} byte column) into the consumer-allocated C structs.
   */
  public static native void pushChangelogNormalizer(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Finalizes the current logical mini-batch into one normalized changelog per touched key. */
  public static native void flushChangelogNormalizer(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Serializes a changelog normalizer's per-key state for a checkpoint. */
  public static native byte[] snapshotChangelogNormalizer(long handle);

  /** Rebuilds a changelog normalizer from a snapshot and returns a fresh handle. */
  public static native long restoreChangelogNormalizer(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      boolean miniBatch,
      byte[] snapshot,
      long memoryBudgetBytes);

  /** Lists the non-empty Flink key groups in a normalizer raw keyed-state checkpoint. */
  public static native int[] changelogNormalizerSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one normalizer key-group payload for a raw keyed-state checkpoint. */
  public static native byte[] snapshotChangelogNormalizerKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Restores a normalizer from all raw keyed-state partitions assigned to this subtask. */
  public static native long restoreChangelogNormalizerPartitions(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      boolean miniBatch,
      byte[][] snapshots,
      long memoryBudgetBytes);

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
   * @param avroSchema writer-schema JSON for Avro (ignored for JSON; pass ""). For Confluent Avro an
   *     empty string starts an empty schema store, fed by id at runtime via {@link
   *     #registerAvroSchema} — the registry-driven path
   * @param readerAvroSchema reader-schema JSON projecting the Avro writer record to a subset of fields
   *     via Avro resolution (the query's columns); pass "" for no projection / non-Avro
   * @param schemaId Confluent schema id the Avro writer schema is registered under (ignored for JSON)
   * @param skipParseErrors Flink's {@code ignore-parse-errors}: an undecodable message contributes no
   *     rows instead of failing the decode (honored by the JSON-decoded formats — plain JSON and the
   *     CDC envelopes — and by CSV, which reproduces Flink's per-field skip granularity natively;
   *     other formats are only routed with it off)
   * @param formatOptions decode-relevant format options as {@code key=value} lines (the CSV
   *     delimiter/quote/escape/comments/null-literal knobs — see {@code KafkaTables}); "" for
   *     defaults. Only planner-vetted options reach here: anything unsupported already fell back.
   */
  public static native long createDecoder(
      int format,
      long schemaArrayAddress,
      long schemaAddress,
      String avroSchema,
      String readerAvroSchema,
      int schemaId,
      boolean skipParseErrors,
      String formatOptions);

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

  /**
   * Registers a writer schema under a Confluent schema id on an existing Confluent-Avro decoder
   * (format 1). The decode operator calls this the first time a batch carries an id it hasn't seen,
   * with the schema fetched from the schema registry — so the decoder's store follows the topic's
   * schema evolution the way Flink's own {@code avro-confluent} deserializer does.
   */
  public static native void registerAvroSchema(long handle, int schemaId, String schema);

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

  /** Serializes every non-empty interval-join key group once, framed by key-group id. */
  public static native byte[][] snapshotIntervalJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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

  /** Restores an interval joiner from raw keyed-state partitions assigned to this task. */
  public static native long restoreIntervalJoinerPartitions(
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
      byte[][] snapshots,
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

  /** Serializes every non-empty temporal-join key group once, framed by key-group id. */
  public static native byte[][] snapshotTemporalJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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

  /** Restores a temporal joiner from raw keyed-state partitions assigned to this task. */
  public static native long restoreTemporalJoinerPartitions(
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
      byte[][] snapshots,
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
      boolean miniBatch,
      long memoryBudgetBytes);

  /** Pushes a left batch, exporting the join changelog (left columns, right columns, row kind). */
  public static native void pushLeftUpdatingJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Pushes a right batch, exporting the join changelog (left columns, right columns, row kind). */
  public static native void pushRightUpdatingJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  public static native void flushUpdatingJoiner(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases an updating joiner handle. */
  public static native void closeUpdatingJoiner(long handle);

  /** Serializes an updating joiner's per-side state for a checkpoint. */
  public static native byte[] snapshotUpdatingJoiner(long handle);

  /** Serializes every non-empty updating-join key group once, framed by key-group id. */
  public static native byte[][] snapshotUpdatingJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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
      boolean miniBatch,
      byte[] snapshot,
      long memoryBudgetBytes);

  /** Restores an updating joiner from raw keyed-state partitions assigned to this task. */
  public static native long restoreUpdatingJoinerPartitions(
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
      boolean miniBatch,
      byte[][] snapshots,
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
   * @param netDiff mini-batch mode (append-only ranker only): emit the net logical-bundle rank
   *     diff — old top-N vs new top-N per touched partition — instead of the per-record shift
   *     cascade; the collapsed changelog is unchanged (see divergences/20)
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
      boolean netDiff,
      long memoryBudgetBytes);

  /** Pushes an input batch, exporting the top-N changelog (input columns plus the row kind). */
  public static native void pushTopNRanker(
      long handle, long inArrayAddress, long inSchemaAddress, long outArrayAddress, long outSchemaAddress);

  /** Flushes the net append-only Top-N changes staged across one logical mini-batch. */
  public static native void flushTopNRanker(
      long handle, long outArrayAddress, long outSchemaAddress);

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
      boolean netDiff,
      byte[] snapshot,
      long memoryBudgetBytes);

  /** Serializes every non-empty Top-N key group once; each payload starts with its key-group id. */
  public static native byte[][] snapshotTopNRankerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Restores a Top-N ranker from raw keyed-state partitions assigned to this subtask. */
  public static native long restoreTopNRankerPartitions(
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      boolean netDiff,
      byte[][] snapshots,
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

  /** Serializes every non-empty window-join key group once, framed by key-group id. */
  public static native byte[][] snapshotWindowJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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

  /** Restores a window joiner from raw keyed-state partitions assigned to this task. */
  public static native long restoreWindowJoinerPartitions(
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
      byte[][] snapshots,
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

  /** Lists the non-empty Flink key groups in a session-window raw keyed-state checkpoint. */
  public static native int[] sessionAggregatorSnapshotKeyGroups(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Serializes one session-window key group for a raw keyed-state checkpoint. */
  public static native byte[] snapshotSessionAggregatorKeyGroup(
      long handle, int keyGroup, int maxParallelism, int[] timestampPrecisions);

  /** Restores a session-window aggregator from raw keyed-state partitions assigned to this task. */
  public static native long restoreSessionAggregatorPartitions(
      long gapMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      byte[][] snapshots,
      long memoryBudgetBytes);
}
