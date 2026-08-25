package tech.streamfusion;

import tech.streamfusion.format.FormatCodes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import org.slf4j.LoggerFactory;

/** Entry point to the native data plane. Holds the methods backed by the Rust library. */
public final class Native {

  private static final String LIBRARY_NAME = "streamfusion";
  private static final String NATIVE_RESOURCE_PREFIX =
      "/tech/streamfusion/native/";

  static {
    loadLibrary();
    verifyLoadedVersion();
  }

  private Native() {}

  /**
   * {@code System.loadLibrary} above searches java.library.path before the bundled JAR resource,
   * so a leftover library from another StreamFusion release would silently win; its snapshot and
   * plan wire formats may differ, which corrupts state instead of failing. Comparing the loaded
   * library's build stamp against the JAR's turns that into an immediate, explained failure. In
   * development mode ({@code -Dstreamfusion.native.development=true}) a locally built library may
   * legitimately differ from the JAR stamp, so a mismatch only warns.
   */
  private static void verifyLoadedVersion() {
    String loaded;
    try {
      loaded = version();
    } catch (UnsatisfiedLinkError versionUnavailable) {
      loaded = null;
    }
    String mismatch = BuildVersion.mismatch(LIBRARY_NAME, loaded, BuildVersion.jarVersion());
    if (mismatch == null) {
      return;
    }
    if (BuildVersion.developmentMode()) {
      LoggerFactory.getLogger(Native.class).warn(mismatch);
      return;
    }
    throw new UnsatisfiedLinkError(mismatch);
  }

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
   * Panics on the native side, on purpose. Exists so the boundary's panic containment is provable:
   * without it the only way to observe the difference between a contained panic and a process abort
   * is to crash a real job. Throws {@link NativeException}; the JVM survives and stays usable.
   */
  static native void panicForTest();

  /**
   * The native side's live-handle breakdown by type (e.g. {@code SessionAggregator=1}), empty once
   * every handle has been closed. The test harness asserts this drains to empty after each test, so
   * a missing close call fails the test naming the leaking type instead of slowly growing RSS.
   */
  public static native String liveNativeHandles();

  // A stateful operator's tracked native state footprint in bytes (zero when unaccounted), one
  // getter per handle type. Handles are not thread-safe, so operators sample these on the task
  // thread after each batch and publish the value to their metric gauges (NativeMemoryBudget).
  public static native long tumblingAggregatorStateBytes(long handle);

  /** Cumulative fixed-window rows rejected behind the aggregator watermark. */
  public static native long tumblingAggregatorLateDrops(long handle);

  public static native long sessionAggregatorStateBytes(long handle);

  public static native long sessionAggregatorLateDrops(long handle);

  public static native long groupAggregatorStateBytes(long handle);

  public static native long groupAggregatorStagingBytes(long handle);

  public static native long groupAggregatorStagedKeys(long handle);

  public static native long localGroupAggregatorStateBytes(long handle);

  public static native long overAggregatorStateBytes(long handle);

  /** Cumulative rowtime rows rejected behind the OVER operator's watermark. */
  public static native long overAggregatorLateDrops(long handle);

  public static native long temporalSorterStateBytes(long handle);

  public static native long keepFirstDeduplicatorStateBytes(long handle);

  public static native long keepLastDeduplicatorStateBytes(long handle);

  public static native long keepLastDeduplicatorStagingBytes(long handle);

  public static native long keepLastDeduplicatorStagedKeys(long handle);

  public static native long updatingJoinerStagingBytes(long handle);

  public static native long updatingJoinerStagedKeys(long handle);

  public static native long updatingJoinerStagedRecords(long handle, boolean left);

  public static native long windowRankerStateBytes(long handle);

  public static native long windowRankerLateDrops(long handle);

  public static native long intervalJoinerStateBytes(long handle);

  public static native long temporalJoinerStateBytes(long handle);

  public static native long updatingJoinerStateBytes(long handle);

  public static native long topNRankerStateBytes(long handle);

  public static native long topNRankerCacheSize(long handle);

  public static native long topNRankerStagingBytes(long handle);

  public static native long topNRankerStagedPartitions(long handle);

  public static native long changelogNormalizerStateBytes(long handle);

  public static native long changelogNormalizerStagingBytes(long handle);

  public static native long changelogNormalizerStagedKeys(long handle);

  public static native long windowJoinerStateBytes(long handle);

  public static native long windowJoinerLateDrops(long handle, boolean left);

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

  /** Number of distinct keys in the current local aggregate bundle. */
  public static native long localGroupAggregatorStagedKeys(long handle);

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

  /** Runs a later Calc batch using the input schema cached by the handle's first call. */
  public static native void calcExpressionArray(
      long handle, long inArrayAddress, long outArrayAddress, long outSchemaAddress);

  /** Releases a compiled Calc handle and its native state. */
  public static native void closeCalcExpression(long handle);

  /**
   * Splits a batch the JVM exported using Flink's BinaryRow key hash into one order-preserving
   * sub-batch per non-empty destination channel, returning a handle to pull them with {@link
   * #nextSplit}; released with {@link #closeSplit}.
   *
   * @param inArrayAddress address of the input {@code ArrowArray} C struct
   * @param inSchemaAddress address of the input {@code ArrowSchema} C struct
   * @param keyColumns indices of the key columns to hash
   * @param timestampPrecisions logical timestamp precision per key column ({@code -1} for non-timestamp keys)
   * @param maxParallelism number of Flink key groups
   * @param parallelism number of downstream channels
   */
  public static native long splitByKey(
      long inArrayAddress,
      long inSchemaAddress,
      int[] keyColumns,
      int[] timestampPrecisions,
      int maxParallelism,
      int parallelism,
      boolean recoverable);

  /**
   * Exports the next sub-batch of a split into the consumer-allocated C structs and returns its key
   * group, or -1 once the split is exhausted.
   */
  public static native int nextSplit(long handle, long outArrayAddress, long outSchemaAddress);

  /** Non-empty key groups in the current fragment's parent batch. */
  public static native int[] currentSplitKeyGroups(long handle);

  /** Original parent-batch row positions for the fragment returned by the latest {@link #nextSplit}. */
  public static native int[] currentSplitOrdinals(long handle);

  /** Releases a split handle. */
  public static native void closeSplit(long handle);

  /**
   * Concatenates several exported batches — row subsets of one exchange edge, so they share a
   * schema — into a single batch exported back into the consumer-allocated C structs. The merge
   * step of the post-exchange coalescer, undoing the fragmentation {@link #splitByKey} introduced.
   */
  public static native void concatBatches(
      long[] inArrayAddresses,
      long[] inSchemaAddresses,
      long outArrayAddress,
      long outSchemaAddress);

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
   * @param memoryBudgetBytes task off-heap budget bounding the open-window state (negative for
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

  /**
   * Local two-phase half's barrier flush: emits every open window's partial state, watermark
   * untouched, so the local operator crosses the checkpoint stateless and later rows are not
   * spuriously late.
   */
  public static native void drainPartialTumblingAggregator(
      long handle, long outArrayAddress, long outSchemaAddress);

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
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator}); the
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
  public static native byte[][] snapshotTumblingAggregatorPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}). Flink bounds OVER
   *     state three ways by shape, all replicated natively: the rowtime frames and the proctime
   *     bounded-ROWS frame keep ONE per-key processing-time cleanup deadline at 1.5x the retention
   *     ({@code <= 1} disables it — Flink's literal {@code minRetentionTime > 1}); the proctime
   *     unbounded fold puts a per-value TTL on its accumulator ({@code > 0} enables); and the
   *     bounded-RANGE rowtime frame takes no retention at all
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Buffers an input batch; its rows are emitted later when a watermark completes them (rowtime).
   * {@code nowMillis} is the operator's processing-time reading — the cleanup-deadline clock.
   */
  public static native void pushOverAggregator(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  /**
   * Proctime OVER: folds a batch in arrival order and exports its rows immediately (no watermark),
   * each with the running aggregate(s) appended, into the consumer-allocated C structs. {@code
   * nowMillis} is the operator's processing-time reading — the retention clock.
   */
  public static native void pushProctimeOverAggregator(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /**
   * Exports the rows the watermark has completed — the input columns with the running aggregate(s)
   * appended — into the consumer-allocated C structs (an empty batch if none are complete). {@code
   * nowMillis} is the operator's processing-time reading — the cleanup-deadline clock.
   */
  public static native void flushOverAggregator(
      long handle,
      long watermarkMillis,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /** Releases an OVER aggregator handle. */
  public static native void closeOverAggregator(long handle);

  /** Serializes every non-empty OVER key group once, framed by key-group id. */
  public static native byte[][] snapshotOverAggregatorPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Restores an OVER aggregator from raw keyed-state partitions assigned to this task. {@code
   * nowMillis} stamps keys restored from a snapshot that carries no retention stamps (a
   * pre-retention writer) from the restore clock — Flink's enable-TTL migration.
   */
  public static native long restoreOverAggregatorPartitions(
      int[] valueTypes,
      int[] aggregateKinds,
      int rtColumn,
      int[] valueColumns,
      int[] keyColumns,
      int frameKind,
      long frameOffset,
      boolean proctime,
      long stateTtlMillis,
      long nowMillis,
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
   *
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code 0} disables
   *     expiry. Only the emitted markers expire — the buffered candidates mirror Flink's
   *     deliberately un-TTL'd timer state. The marker is written once, when the key fires, and
   *     never refreshed, so an emitted key expires a fixed retention after firing and can then
   *     emit a second first row.
   */
  public static native long createKeepFirstDeduplicator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rtColumn,
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Buffers an input batch; each key's first row is emitted on the watermark that reaches it.
   * {@code nowMillis} is the operator's processing-time reading — the state-TTL clock.
   */
  public static native void pushKeepFirstDeduplicator(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  /**
   * Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached.
   * Firing stamps the key's emitted marker with {@code nowMillis} — the marker's single TTL'd
   * write.
   */
  public static native void flushKeepFirstDeduplicator(
      long handle, long watermarkMillis, long nowMillis, long outArrayAddress, long outSchemaAddress);

  /**
   * Rows dropped as late (rowtime already below the watermark) over the handle's lifetime,
   * cumulative across pushes; feeds Flink's {@code numLateRecordsDropped} counter. Serves memory-
   * and persistent handles alike (the late filter precedes the backend split).
   */
  public static native long keepFirstDeduplicatorLateDrops(long handle);

  /** Releases the deduplicator and its per-key state. */
  public static native void closeKeepFirstDeduplicator(long handle);

  /** Lists the non-empty Flink key groups in a keep-first deduplication raw keyed-state checkpoint. */
  public static native byte[][] snapshotKeepFirstDeduplicatorPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Restores a keep-first deduplicator from raw keyed-state partitions assigned to this subtask.
   * {@code nowMillis} stamps markers restored from a snapshot that carries no TTL timestamps (a
   * pre-TTL writer), granting them a full retention from the restore — Flink's enable-TTL
   * migration.
   */
  public static native long restoreKeepFirstDeduplicatorPartitions(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rtColumn,
      long stateTtlMillis,
      long nowMillis,
      byte[][] snapshots,
      long memoryBudgetBytes);

  /**
   * Creates an eager (push→emit) deduplicator and returns an opaque handle. Per partition key, keep-
   * last keeps the winning row and emits a retract changelog eagerly per input row (first row {@code
   * +I}; a replacement emits {@code -U}(previous, gated on {@code generateUpdateBefore})/{@code +U}
   * (new)); keep-first emits the first row per key ({@code +I}, insert-only) and drops the rest. A
   * rowtime order ({@code rowtimeOrdered}) reads the rowtime column and keeps the max-rowtime row;
   * proctime uses arrival order (no rowtime read). Released with {@link #closeKeepLastDeduplicator}.
   *
   * @param generateInsert Flink's insert-sensitivity ({@code
   *     table.exec.deduplicate.insert-update-after-sensitive-enabled}, default true). With it and
   *     {@code generateUpdateBefore} both false, every emission is a bare {@code +U} — a fresh
   *     key's first row included — and the proctime identical-row suppression is disabled
   * @param compactChanges mini-batch compact-changes ({@code
   *     table.exec.deduplicate.mini-batch.compact-changes-enabled}, rowtime only): the flush nets
   *     each key's bundle to one transition instead of the default full kept chain
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code 0} disables
   *     expiry. A key expires {@code stateTtlMillis} after its last write and then reads as absent,
   *     and the proctime keep-last identical-row suppression is disabled — Flink's TTL'd emission
   */
  public static native long createKeepLastDeduplicator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rtColumn,
      boolean generateUpdateBefore,
      boolean generateInsert,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      boolean compactChanges,
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Folds an input batch and returns the changelog (or insert-only rows) it produces. {@code
   * nowMillis} is the operator's processing-time reading — the state-TTL clock.
   */
  public static native void pushKeepLastDeduplicator(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushKeepLastDeduplicator(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases the deduplicator and its per-key state. */
  public static native void closeKeepLastDeduplicator(long handle);

  /** Lists the non-empty Flink key groups in a keep-last deduplication raw keyed-state checkpoint. */
  public static native byte[][] snapshotKeepLastDeduplicatorPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Restores a keep-last deduplicator from the raw keyed-state partitions assigned to this subtask.
   * {@code nowMillis} stamps keys restored from a snapshot that carries no TTL timestamps (a
   * pre-TTL writer), granting them a full retention from the restore — Flink's enable-TTL
   * migration.
   */
  public static native long restoreKeepLastDeduplicatorPartitions(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rtColumn,
      boolean generateUpdateBefore,
      boolean generateInsert,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      boolean compactChanges,
      long stateTtlMillis,
      long nowMillis,
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

  /** Serializes every non-empty window-rank key group once, framed by key-group id. */
  public static native byte[][] snapshotWindowRankerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code 0} disables
   *     expiry. A group expires {@code stateTtlMillis} after its last write and then reads as
   *     absent, and the unchanged-result suppression is disabled — Flink's TTL'd emission
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Folds an input batch into per-key state, exporting the changelog rows it produces (grouping keys,
   * aggregate results, then the {@code $row_kind$} byte column) into the consumer-allocated C structs.
   * {@code nowMillis} is the operator's processing-time reading — the state-TTL clock.
   */
  public static native void updateGroupAggregator(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /** Flushes the group changes staged across one logical mini-batch. */
  public static native void flushGroupAggregator(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases a {@code GROUP BY} aggregator handle. */
  public static native void closeGroupAggregator(long handle);

  /** Lists the non-empty Flink key groups in the group aggregator's raw keyed-state checkpoint. */
  public static native byte[][] snapshotGroupAggregatorPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Rebuilds a group aggregator from every raw keyed-state partition assigned to this subtask.
   * {@code nowMillis} stamps groups restored from a snapshot that carries no TTL timestamps (a
   * pre-TTL writer), granting them a full retention from the restore — Flink's enable-TTL
   * migration.
   */
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
      long stateTtlMillis,
      long nowMillis,
      byte[][] snapshots,
      long memoryBudgetBytes);

  /** Whether this native build carries direct Rust RocksDB state. */
  public static native boolean rocksdbStateAvailable();

  /**
   * Creates the slot's shared RocksDB block cache and write-buffer manager (Flink's memory-control
   * split of the given budget); every native store opened with the returned handle is bounded by
   * it. Released through Flink's shared-resource lease, after every store under it closed.
   */
  public static native long createRocksDBSharedResources(long totalBytes, double writeBufferRatio);

  public static native void releaseRocksDBSharedResources(long handle);

  /** Whether this aggregate state shape can be encoded in the Rust RocksDB store. */
  public static native boolean rocksdbGroupAggregatorSupported(
      int[] aggregateKinds, int[] valueTypes);

  /** Creates a group aggregator backed directly by a Rust-owned RocksDB instance. */
  public static native long createRocksDBGroupAggregator(
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
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  /** Materializes direct RocksDB aggregate state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBGroupAggregatorPartitions(long handle);

  public static native void updateRocksDBGroupAggregator(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushRocksDBGroupAggregator(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBGroupAggregator(
      long handle, String snapshotDirectory);

  public static native long rocksdbGroupAggregatorStateBytes(long handle);

  public static native long rocksdbGroupAggregatorStagingBytes(long handle);

  public static native long rocksdbGroupAggregatorStagedKeys(long handle);

  public static native void closeRocksDBGroupAggregator(long handle);

  /** Creates a changelog normalizer backed directly by a Rust-owned RocksDB instance. */
  public static native long createRocksDBChangelogNormalizer(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  /** Materializes direct RocksDB normalizer state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBChangelogNormalizerPartitions(long handle);

  public static native void pushRocksDBChangelogNormalizer(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushRocksDBChangelogNormalizer(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBChangelogNormalizer(
      long handle, String snapshotDirectory);

  public static native long rocksdbChangelogNormalizerStateBytes(long handle);

  public static native long rocksdbChangelogNormalizerStagingBytes(long handle);

  public static native long rocksdbChangelogNormalizerStagedKeys(long handle);

  public static native void closeRocksDBChangelogNormalizer(long handle);

  /** Creates an eager deduplicator backed directly by a Rust-owned RocksDB instance. */
  public static native long createRocksDBKeepLastDeduplicator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rowtimeColumn,
      boolean generateUpdateBefore,
      boolean generateInsert,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      boolean compactChanges,
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  /** Materializes direct RocksDB dedup state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBKeepLastDeduplicatorPartitions(long handle);

  public static native void pushRocksDBKeepLastDeduplicator(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushRocksDBKeepLastDeduplicator(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBKeepLastDeduplicator(
      long handle, String snapshotDirectory);

  public static native long rocksdbKeepLastDeduplicatorStateBytes(long handle);

  public static native long rocksdbKeepLastDeduplicatorStagingBytes(long handle);

  public static native long rocksdbKeepLastDeduplicatorStagedKeys(long handle);

  public static native void closeRocksDBKeepLastDeduplicator(long handle);

  /**
   * Whether the keep-first deduplicator's input row shape is encodable by the direct RocksDB
   * store's row codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbKeepFirstDeduplicatorSupported(long schemaAddress);

  /**
   * Creates a watermark-buffered keep-first deduplicator whose pending candidates and fired
   * markers live directly in a Rust-owned RocksDB instance. The declared input schema builds the
   * row codec the persisted rows are decoded with, so restored bytes parse before any batch
   * arrives.
   */
  public static native long createRocksDBKeepFirstDeduplicator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rowtimeColumn,
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      long schemaAddress,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  public static native void pushRocksDBKeepFirstDeduplicator(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  public static native void flushRocksDBKeepFirstDeduplicator(
      long handle, long watermarkMillis, long nowMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBKeepFirstDeduplicator(
      long handle, String snapshotDirectory);

  /** Materializes direct RocksDB keep-first state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBKeepFirstDeduplicatorPartitions(long handle);

  public static native long rocksdbKeepFirstDeduplicatorStateBytes(long handle);

  public static native void closeRocksDBKeepFirstDeduplicator(long handle);

  /** Creates an updating joiner backed directly by a Rust-owned RocksDB instance. */
  public static native long createRocksDBUpdatingJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int[] keyTimestampPrecisions,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      boolean leftJoinKeyUnique,
      boolean rightJoinKeyUnique,
      boolean miniBatch,
      long leftStateTtlMillis,
      long rightStateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  /** Materializes direct RocksDB join state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBUpdatingJoinerPartitions(long handle);

  public static native void pushLeftRocksDBUpdatingJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void pushRightRocksDBUpdatingJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushRocksDBUpdatingJoiner(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBUpdatingJoiner(
      long handle, String snapshotDirectory);

  public static native long rocksdbUpdatingJoinerStateBytes(long handle);

  public static native long rocksdbUpdatingJoinerStagingBytes(long handle);

  public static native long rocksdbUpdatingJoinerStagedKeys(long handle);

  public static native long rocksdbUpdatingJoinerStagedRecords(long handle, boolean left);

  public static native void closeRocksDBUpdatingJoiner(long handle);

  /** Creates a window joiner whose buffered rows live directly in a Rust-owned RocksDB instance. */
  public static native long createRocksDBWindowJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int[] keyTimestampPrecisions,
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
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions,
      long restoredTimerDeadline);

  /** Materializes direct RocksDB window-join state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBWindowJoinerPartitions(long handle);

  public static native void pushLeftRocksDBWindowJoiner(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void pushRightRocksDBWindowJoiner(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void flushRocksDBWindowJoiner(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBWindowJoiner(
      long handle, long timerDeadline, String snapshotDirectory);

  public static native long rocksdbWindowJoinerTimerDeadline(long handle);

  public static native long rocksdbWindowJoinerStateBytes(long handle);

  public static native void closeRocksDBWindowJoiner(long handle);

  /**
   * Whether both interval-join input row shapes are encodable by the direct RocksDB store's row
   * codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbIntervalJoinerSupported(
      long leftSchemaAddress, long rightSchemaAddress);

  /**
   * Creates an interval joiner whose buffered rows (with their outer-join match flags) live
   * directly in a Rust-owned RocksDB instance. The declared input schemas build the row codecs the
   * persisted rows are decoded with, so restored bytes parse before any batch arrives.
   */
  public static native long createRocksDBIntervalJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int[] keyTimestampPrecisions,
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
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions,
      long restoredTimerDeadline);

  public static native void pushLeftRocksDBIntervalJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      boolean proctime,
      long proctimeNowMillis);

  public static native void pushRightRocksDBIntervalJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long outArrayAddress,
      long outSchemaAddress,
      boolean proctime,
      long proctimeNowMillis);

  public static native void advanceRocksDBIntervalJoiner(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBIntervalJoiner(
      long handle, long timerDeadline, String snapshotDirectory);

  public static native long rocksdbIntervalJoinerTimerDeadline(long handle);

  /** Materializes direct RocksDB interval-join state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBIntervalJoinerPartitions(long handle);

  public static native long rocksdbIntervalJoinerStateBytes(long handle);

  public static native void closeRocksDBIntervalJoiner(long handle);

  /**
   * Whether both temporal-join input row shapes (and their shared equi-key shape) are encodable by
   * the direct RocksDB store's row codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbTemporalJoinerSupported(
      int[] leftKeys, int[] rightKeys, long leftSchemaAddress, long rightSchemaAddress);

  /**
   * Creates an event-time temporal joiner whose buffered probe rows, versioned build side, and
   * cleanup deadlines live directly in a Rust-owned RocksDB instance. The declared input schemas
   * build the row codecs the persisted rows are decoded with, so restored bytes parse before any
   * batch arrives.
   */
  public static native long createRocksDBTemporalJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int[] keyTimestampPrecisions,
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
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  public static native void pushLeftRocksDBTemporalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  public static native void pushRightRocksDBTemporalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  public static native void advanceRocksDBTemporalJoiner(
      long handle, long watermarkMillis, long nowMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBTemporalJoiner(
      long handle, String snapshotDirectory);

  /** Materializes direct RocksDB temporal-join state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBTemporalJoinerPartitions(long handle);

  public static native long rocksdbTemporalJoinerStateBytes(long handle);

  public static native void closeRocksDBTemporalJoiner(long handle);

  /**
   * Whether the temporal sort's input row shape is encodable by the direct RocksDB store's row
   * codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbTemporalSorterSupported(long schemaAddress);

  /**
   * Creates an event-time sorter whose buffered rows live directly in a Rust-owned RocksDB
   * instance. The declared input schema builds the row codec the persisted rows are decoded with,
   * so restored bytes parse before any batch arrives.
   */
  public static native long createRocksDBTemporalSorter(
      int rtColumn,
      long memoryBudgetBytes,
      long schemaAddress,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  public static native void pushRocksDBTemporalSorter(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void flushRocksDBTemporalSorter(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBTemporalSorter(
      long handle, String snapshotDirectory);

  /**
   * Serializes the direct RocksDB sort buffer as the memory snapshot's own plain-IPC blob; the
   * host frames it into the singleton key group exactly as it frames the memory snapshot.
   */
  public static native byte[] snapshotRocksDBTemporalSorter(long handle);

  public static native long rocksdbTemporalSorterStateBytes(long handle);

  public static native void closeRocksDBTemporalSorter(long handle);

  /**
   * Whether the aligned-window aggregate's key and accumulator-state shapes are encodable by the
   * direct RocksDB store's row codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbWindowAggregatorSupported(
      int[] valueTypes, int[] aggregateKinds, int[] keyTypes);

  /**
   * Creates an aligned-window (tumble/hop/cumulate) aggregator whose open (window, key) state lives
   * directly in a Rust-owned RocksDB instance. The declared key types build the key codec the
   * persisted state is decoded with, so restored bytes parse before any batch arrives.
   */
  public static native long createRocksDBWindowAggregator(
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] valueTypes,
      int[] aggregateKinds,
      int[] keyTypes,
      int[] keyTimestampPrecisions,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions,
      long restoredTimerDeadline);

  public static native void pushRocksDBWindowAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  /** Global two-phase half: merges a partial batch into the direct RocksDB window state. */
  public static native void pushPartialRocksDBWindowAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void flushRocksDBWindowAggregator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBWindowAggregator(
      long handle, long timerDeadline, String snapshotDirectory);

  public static native long rocksdbWindowAggregatorTimerDeadline(long handle);

  /** Materializes direct RocksDB window state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBWindowAggregatorPartitions(
      long handle, int maxParallelism, int[] keyTimestampPrecisions);

  public static native long rocksdbWindowAggregatorStateBytes(long handle);

  public static native void closeRocksDBWindowAggregator(long handle);

  /**
   * Whether the session-window aggregate's key and accumulator-state shapes are encodable by the
   * direct RocksDB store's row codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbSessionAggregatorSupported(
      int[] valueTypes, int[] aggregateKinds, int[] keyTypes);

  /**
   * Creates a session-window aggregator whose open sessions live directly in a Rust-owned RocksDB
   * instance, keyed by group key so a touched key's sessions hydrate as one range for merging. The
   * declared key types build the key codec the persisted state is decoded with, so restored bytes
   * parse before any batch arrives.
   */
  public static native long createRocksDBSessionAggregator(
      long gapMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      int[] keyTypes,
      int[] keyTimestampPrecisions,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions,
      long restoredTimerDeadline);

  public static native void pushRocksDBSessionAggregator(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void flushRocksDBSessionAggregator(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBSessionAggregator(
      long handle, long timerDeadline, String snapshotDirectory);

  public static native long rocksdbSessionAggregatorTimerDeadline(long handle);

  /** Materializes direct RocksDB session state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBSessionAggregatorPartitions(
      long handle, int maxParallelism, int[] keyTimestampPrecisions);

  public static native long rocksdbSessionAggregatorStateBytes(long handle);

  public static native void closeRocksDBSessionAggregator(long handle);

  /**
   * Whether an OVER aggregate's shape is admitted by the direct RocksDB store: an event-time
   * unbounded fold of non-distinct aggregates or pure window functions, with every input column
   * encodable by the store's row codec. Proctime ordering, bounded frames, and distinct
   * aggregates stay on the snapshot-store compat path.
   */
  public static native boolean rocksdbOverAggregatorSupported(
      int[] valueTypes, int[] aggregateKinds, int frameKind, boolean proctime, long schemaAddress);

  /**
   * Creates an event-time OVER aggregator whose pending rows and per-key running fold live
   * directly in a Rust-owned RocksDB instance. The declared input schema builds the arrow-row
   * converters the persisted state is decoded with, so restored bytes parse before any batch
   * arrives; {@code nowMillis} is the restore clock the retention stamps migrate against.
   */
  public static native long createRocksDBOverAggregator(
      int[] valueTypes,
      int[] aggregateKinds,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int frameKind,
      long frameOffset,
      boolean proctime,
      int[] keyTimestampPrecisions,
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      long schemaAddress,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  public static native void pushRocksDBOverAggregator(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  public static native void flushRocksDBOverAggregator(
      long handle,
      long watermarkMillis,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native String[] checkpointRocksDBOverAggregator(
      long handle, String snapshotDirectory);

  /** Materializes direct RocksDB OVER state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBOverAggregatorPartitions(
      long handle, int maxParallelism, int[] keyTimestampPrecisions);

  public static native long rocksdbOverAggregatorStateBytes(long handle);

  public static native void closeRocksDBOverAggregator(long handle);

  /**
   * Creates an append-only or retracting Top-N ranker backed directly by a Rust-owned RocksDB
   * instance. The declared input schema builds the arrow-row converters the persisted state is
   * decoded with, so restored bytes parse before any batch arrives.
   */
  public static native long createRocksDBTopNRanker(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      boolean netDiff,
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      long schemaAddress,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  /** {@link #createRocksDBTopNRanker} for the update-fast (unique-keyed changelog) variant. */
  public static native long createRocksDBUpdateFastTopNRanker(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] rowKeyColumns,
      int[] rowKeyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      boolean generateUpdateBefore,
      long stateTtlMillis,
      long nowMillis,
      long memoryBudgetBytes,
      long schemaAddress,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions);

  /** Materializes direct RocksDB Top-N state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBTopNRankerPartitions(long handle);

  public static native void pushRocksDBTopNRanker(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushRocksDBTopNRanker(
      long handle, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBTopNRanker(long handle, String snapshotDirectory);

  public static native long rocksdbTopNRankerStateBytes(long handle);

  public static native long rocksdbTopNRankerStagingBytes(long handle);

  public static native long rocksdbTopNRankerStagedPartitions(long handle);

  public static native void closeRocksDBTopNRanker(long handle);

  /**
   * Whether the window ranker's input row shape is encodable by the direct RocksDB store's row
   * codec; anything else stays on the snapshot-store compat path.
   */
  public static native boolean rocksdbWindowRankerSupported(long schemaAddress);

  /**
   * Creates a window ranker (window Top-N / window deduplication) whose open (window, key) buffers
   * live directly in a Rust-owned RocksDB instance. The declared input schema builds the row codec
   * the persisted buffers are decoded with, so restored bytes parse before any batch arrives.
   */
  public static native long createRocksDBWindowRanker(
      int windowStartCol,
      int windowEndCol,
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      long schemaAddress,
      long memoryBudgetBytes,
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned,
      byte[][] restoredPartitions,
      long restoredTimerDeadline);

  public static native void pushRocksDBWindowRanker(
      long handle, long inArrayAddress, long inSchemaAddress);

  public static native void flushRocksDBWindowRanker(
      long handle, long watermarkMillis, long outArrayAddress, long outSchemaAddress);

  public static native String[] checkpointRocksDBWindowRanker(
      long handle, long timerDeadline, String snapshotDirectory);

  public static native long rocksdbWindowRankerTimerDeadline(long handle);

  /** Materializes direct RocksDB window-rank state as backend-independent key-group partitions. */
  public static native byte[][] snapshotRocksDBWindowRankerPartitions(long handle);

  public static native long rocksdbWindowRankerStateBytes(long handle);

  public static native void closeRocksDBWindowRanker(long handle);

  /** Generic RocksDB persistence for operators still using their key-group snapshot codec. */
  public static native long createRocksDBSnapshotStore(
      String databaseDirectory,
      int maxParallelism,
      String optionsJson,
      long sharedResources,
      String[] sourceDirectories,
      String[] sourceSnapshotTokens,
      int keyGroupStart,
      int keyGroupEnd,
      boolean aligned);

  public static native byte[][] restoreRocksDBSnapshotStorePartitions(long handle);

  public static native long rocksdbSnapshotStoreTimerDeadline(long handle);

  public static native String[] checkpointRocksDBSnapshotStore(
      long handle, byte[][] partitions, long timerDeadline, String snapshotDirectory);

  public static native void closeRocksDBSnapshotStore(long handle);

  /**
   * Creates a changelog normalizer (keep-last per unique key) and returns an opaque handle. Each
   * input changelog batch folds into per-key state and the normalizer exports the normalized
   * changelog (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE on the {@code $row_kind$} column). Released
   * with {@link #closeChangelogNormalizer}.
   *
   * @param keyColumns unique-key column indices in the input batch
   * @param generateUpdateBefore whether to emit an UPDATE_BEFORE row before each UPDATE_AFTER
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code 0} disables
   *     expiry. A key expires {@code stateTtlMillis} after its last write and then reads as absent,
   *     and the unchanged-row suppression is disabled — Flink's TTL'd emission
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
   */
  public static native long createChangelogNormalizer(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Folds an input changelog batch into per-key keep-last state, exporting the normalized changelog
   * (the input columns then the {@code $row_kind$} byte column) into the consumer-allocated C structs.
   * {@code nowMillis} is the operator's processing-time reading — the state-TTL clock.
   */
  public static native void pushChangelogNormalizer(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /** Finalizes the current logical mini-batch into one normalized changelog per touched key. */
  public static native void flushChangelogNormalizer(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Lists the non-empty Flink key groups in a normalizer raw keyed-state checkpoint. */
  public static native byte[][] snapshotChangelogNormalizerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Restores a normalizer from all raw keyed-state partitions assigned to this subtask. {@code
   * nowMillis} stamps keys restored from a snapshot that carries no TTL timestamps (a pre-TTL
   * writer), granting them a full retention from the restore — Flink's enable-TTL migration.
   */
  public static native long restoreChangelogNormalizerPartitions(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long stateTtlMillis,
      long nowMillis,
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
   * @param format a {@link FormatCodes} code. JSON, CSV,
   *     raw, and the CDC envelopes decode against the schema C structs (the CDC formats append a
   *     {@code $row_kind$} byte); the Avro variants derive their schema from {@code avroSchema}
   * @param schemaArrayAddress address of an exported (empty) {@code ArrowArray} of the target schema
   * @param schemaAddress address of the matching exported {@code ArrowSchema}
   * @param avroSchema writer-schema JSON for Avro (ignored for JSON; pass ""). For Confluent Avro an
   *     empty string starts an empty schema store, fed by id at runtime via the avro facade's
   *     {@code registerWriterSchema} — the registry-driven path
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
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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

  /** Serializes every non-empty interval-join key group once, framed by key-group id. */
  public static native byte[][] snapshotIntervalJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code <= 1}
   *     disables cleaning (Flink's literal {@code minRetentionTime > 1}). Flink's temporal join
   *     keeps ONE per-key processing-time cleanup deadline at 1.5x the retention (the planner's
   *     max idle retention, derived natively) and clears the key's entire state — both sides —
   *     when it fires
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Buffers a probe-side (left) batch (no output until a watermark). {@code nowMillis} is the
   * operator's processing-time reading — the cleanup-deadline clock.
   */
  public static native void pushLeftTemporalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  /** Folds a build-side (right) changelog batch into the versioned state (no output until a watermark). */
  public static native void pushRightTemporalJoiner(
      long handle, long inArrayAddress, long inSchemaAddress, long nowMillis);

  /**
   * Advances the watermark, exporting the joined rows ({@code [left.., right..]} with a trailing
   * {@code $row_kind$}) for the buffered probe rows it has passed.
   */
  public static native void advanceTemporalJoiner(
      long handle,
      long watermarkMillis,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /** Releases a temporal joiner handle. */
  public static native void closeTemporalJoiner(long handle);

  /** Serializes every non-empty temporal-join key group once, framed by key-group id. */
  public static native byte[][] snapshotTemporalJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Restores a temporal joiner from raw keyed-state partitions assigned to this task. {@code
   * nowMillis} stamps a full max-retention deadline onto keys restored from a snapshot that
   * carries no deadlines (a pre-retention writer) — Flink's enable-TTL migration.
   */
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
      long stateTtlMillis,
      long nowMillis,
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
   * @param leftStateTtlMillis idle-state retention for the left side's rows ({@code
   *     table.exec.state.ttl}, or the per-side {@code STATE_TTL} hint); {@code 0} disables expiry.
   *     Each stored row expires independently {@code ttl} millis after its last write and then
   *     reads as absent — Flink's per-entry MapState TTL on the join state views
   * @param rightStateTtlMillis {@code leftStateTtlMillis} for the right side (the sides may differ)
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
   */
  public static native long createUpdatingJoiner(
      int[] leftKeys,
      int[] rightKeys,
      int[] keyTimestampPrecisions,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      boolean leftJoinKeyUnique,
      boolean rightJoinKeyUnique,
      boolean miniBatch,
      long leftStateTtlMillis,
      long rightStateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Pushes a left batch, exporting the join changelog (left columns, right columns, row kind).
   * {@code nowMillis} is the operator's processing-time reading — the state-TTL clock.
   */
  public static native void pushLeftUpdatingJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /** Pushes a right batch, exporting the join changelog (left columns, right columns, row kind). */
  public static native void pushRightUpdatingJoiner(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  public static native void flushUpdatingJoiner(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases an updating joiner handle. */
  public static native void closeUpdatingJoiner(long handle);

  /** Serializes every non-empty updating-join key group once, framed by key-group id. */
  public static native byte[][] snapshotUpdatingJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Restores an updating joiner from raw keyed-state partitions assigned to this task. {@code
   * nowMillis} stamps rows restored from a snapshot side that carries no TTL timestamps (a
   * pre-TTL writer), granting them a full retention from the restore — Flink's enable-TTL
   * migration.
   */
  public static native long restoreUpdatingJoinerPartitions(
      int[] leftKeys,
      int[] rightKeys,
      int[] keyTimestampPrecisions,
      int joinType,
      long leftSchemaAddress,
      long rightSchemaAddress,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      boolean leftJoinKeyUnique,
      boolean rightJoinKeyUnique,
      boolean miniBatch,
      long leftStateTtlMillis,
      long rightStateTtlMillis,
      long nowMillis,
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
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code 0} disables
   *     expiry. Expired rank state reads as absent and expiry emits nothing — the append-only
   *     ranker expires per sort-key list, the retracting one per whole buffer (Flink's treemap
   *     clock)
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
   */
  public static native long createTopNRanker(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      boolean netDiff,
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Pushes an input batch, exporting the top-N changelog (input columns plus the row kind).
   * {@code nowMillis} is the operator's processing-time reading — the state-TTL clock.
   */
  public static native void pushTopNRanker(
      long handle,
      long inArrayAddress,
      long inSchemaAddress,
      long nowMillis,
      long outArrayAddress,
      long outSchemaAddress);

  /** Flushes the net append-only Top-N changes staged across one logical mini-batch. */
  public static native void flushTopNRanker(
      long handle, long outArrayAddress, long outSchemaAddress);

  /** Releases a Top-N ranker handle. */
  public static native void closeTopNRanker(long handle);

  /** Serializes every non-empty Top-N key group once; each payload starts with its key-group id. */
  public static native byte[][] snapshotTopNRankerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /**
   * Captures an immutable append-only Top-N checkpoint view without encoding IPC on the task
   * thread. Returns zero when the ranker must use the synchronous compatibility path.
   */
  public static native long captureTopNRankerSnapshot(long handle, int maxParallelism);

  /** Returns the non-empty key groups held by an immutable Top-N checkpoint view. */
  public static native int[] topNRankerSnapshotKeyGroups(long snapshotHandle);

  /** Encodes one key-group payload; called by Flink's async heap-state serializer. */
  public static native byte[] encodeTopNRankerSnapshotPartition(
      long snapshotHandle, int keyGroup);

  /** Releases an immutable Top-N checkpoint view. */
  public static native void closeTopNRankerSnapshot(long snapshotHandle);

  /**
   * Restores a Top-N ranker from raw keyed-state partitions assigned to this subtask. {@code
   * nowMillis} stamps rows restored from a snapshot that carries no TTL timestamps (a pre-TTL
   * writer), granting them a full retention from the restore — Flink's enable-TTL migration.
   */
  public static native long restoreTopNRankerPartitions(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      boolean netDiff,
      long stateTtlMillis,
      long nowMillis,
      byte[][] snapshots,
      long memoryBudgetBytes);

  /**
   * Creates an update-fast streaming Top-N ranker — Flink's {@code UpdatableTopNFunction} /
   * {@code FastTop1Function} shape: a changelog whose rows are replaced in place by a unique key
   * (no retractions arrive; the planner proved the sort key monotonic). Only the top-N rows are
   * kept per partition. The handle is served by the shared Top-N push/flush/snapshot/close entry
   * points.
   *
   * @param rowKeyColumns the unique-key column indices identifying the row a record replaces
   * @param stateTtlMillis idle-state retention ({@code table.exec.state.ttl}); {@code 0} disables
   *     expiry. Per-row-key entry TTL: an expired entry reads as absent, so its next version
   *     inserts fresh (for {@code limit == 1} even a strictly worse row becomes top-1)
   */
  public static native long createUpdateFastTopNRanker(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] rowKeyColumns,
      int[] rowKeyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      boolean generateUpdateBefore,
      long stateTtlMillis,
      long memoryBudgetBytes);

  /**
   * Restores an update-fast Top-N ranker from raw keyed-state partitions assigned to this
   * subtask; {@code nowMillis} stamps rows from a timestamp-less snapshot (see
   * {@link #restoreTopNRankerPartitions}).
   */
  public static native long restoreUpdateFastTopNRankerPartitions(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int[] rowKeyColumns,
      int[] rowKeyTimestampPrecisions,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber,
      boolean generateUpdateBefore,
      long stateTtlMillis,
      long nowMillis,
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
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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

  /** Serializes every non-empty window-join key group once, framed by key-group id. */
  public static native byte[][] snapshotWindowJoinerPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

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
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
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
   * @param memoryBudgetBytes task off-heap budget (see {@link #createTumblingAggregator})
   */
  public static native long restoreSessionAggregator(
      long gapMillis, int[] valueTypes, int[] aggregateKinds, byte[] snapshot, long memoryBudgetBytes);

  /** Serializes every non-empty session-window key group once, framed by key-group id. */
  public static native byte[][] snapshotSessionAggregatorPartitions(
      long handle, int maxParallelism, int[] timestampPrecisions);

  /** Restores a session-window aggregator from raw keyed-state partitions assigned to this task. */
  public static native long restoreSessionAggregatorPartitions(
      long gapMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      byte[][] snapshots,
      long memoryBudgetBytes);
}
