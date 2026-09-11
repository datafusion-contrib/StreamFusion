package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.streamfusion.Native;

/**
 * Pending changelog rows of one write destination, held natively and merged per key on {@link
 * #flush}. A pushed batch is handed over whole (the buffer owns it afterwards) and its rows take
 * consecutive sequence numbers in arrival order; the flush sorts by key, keeps one row per key (the
 * last or the first to arrive), and returns the rows in a key-value layout: the key columns, the
 * sequence number, the row kind, then every table column. Paimon's merge-on-read requires exactly
 * that shape of a file, so a flush is what a level-0 file is written from.
 */
public final class KeyedUpsertBuffer implements AutoCloseable {

  /** One flush: the caller closes both batches once written. */
  public static final class Flushed implements AutoCloseable {
    public final VectorSchemaRoot root;
    public final VectorSchemaRoot changelog;
    public final long deleteRows;
    public final long minSequence;
    public final long maxSequence;

    Flushed(
        VectorSchemaRoot root,
        VectorSchemaRoot changelog,
        long deleteRows,
        long minSequence,
        long maxSequence) {
      this.root = root;
      this.changelog = changelog;
      this.deleteRows = deleteRows;
      this.minSequence = minSequence;
      this.maxSequence = maxSequence;
    }

    @Override
    public void close() {
      try {
        root.close();
      } finally {
        if (changelog != null) {
          changelog.close();
        }
      }
    }
  }

  private final BufferAllocator allocator;
  private long handle;

  /**
   * @param keyColumns ordinals of the key columns in the pushed batches
   * @param kindColumn ordinal of the hidden row-kind byte column
   * @param keepLast whether the last row of a key survives (the first otherwise)
   * @param ignoreRetracts whether update-before and delete rows are dropped before merging
   */
  public KeyedUpsertBuffer(
      BufferAllocator allocator,
      int[] keyColumns,
      int kindColumn,
      boolean keepLast,
      boolean ignoreRetracts) {
    this.allocator = allocator;
    this.handle = Native.createKeyedUpsertBuffer(keyColumns, kindColumn, keepLast, ignoreRetracts);
  }

  /**
   * Hands a batch over; its rows take the sequence numbers {@code firstSequence} onwards. Returns
   * the rows retained, which is fewer than pushed when ignored retracts were dropped.
   */
  public long push(VectorSchemaRoot root, long firstSequence) {
    BufferAllocator rootAllocator =
        root.getFieldVectors().isEmpty() ? allocator : root.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(rootAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(rootAllocator)) {
      Data.exportVectorSchemaRoot(rootAllocator, root, NativeAllocator.DICTIONARIES, array, schema);
      return Native.keyedUpsertBufferPush(
          handle, array.memoryAddress(), schema.memoryAddress(), firstSequence);
    } finally {
      root.close();
    }
  }

  public long bytes() {
    return Native.keyedUpsertBufferBytes(handle);
  }

  public long rows() {
    return Native.keyedUpsertBufferRows(handle);
  }

  /** Merges and empties the buffer; {@code null} when no row survives. */
  public Flushed flush() {
    return flush(false);
  }

  /** Also returns the input changelog in key and arrival order when requested. */
  public Flushed flush(boolean includeChangelog) {
    return flush(includeChangelog, true);
  }

  /** Postpone files retain input order and Paimon's unknown sequence number (-1). */
  public Flushed flushUnmerged() {
    return flush(false, false);
  }

  private Flushed flush(boolean includeChangelog, boolean mergeRows) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        ArrowArray changelogArray = includeChangelog ? ArrowArray.allocateNew(allocator) : null;
        ArrowSchema changelogSchema =
            includeChangelog ? ArrowSchema.allocateNew(allocator) : null) {
      VectorSchemaRoot root = null;
      try {
        long[] summary =
            Native.keyedUpsertBufferFlush(
                handle,
                array.memoryAddress(),
                schema.memoryAddress(),
                includeChangelog ? changelogArray.memoryAddress() : 0,
                includeChangelog ? changelogSchema.memoryAddress() : 0,
                mergeRows);
        if (summary[0] == 0) {
          return null;
        }
        root = importBatch(array, schema);
        VectorSchemaRoot changelog =
            includeChangelog ? importBatch(changelogArray, changelogSchema) : null;
        return new Flushed(root, changelog, summary[1], summary[2], summary[3]);
      } catch (Throwable failure) {
        if (root != null) {
          root.close();
        }
        releaseUnimported(array, schema);
        releaseUnimported(changelogArray, changelogSchema);
        throw failure;
      }
    }
  }

  private VectorSchemaRoot importBatch(ArrowArray array, ArrowSchema schema) {
    // Import through non-owning struct views so the outer scope can release either output if
    // importing its sibling fails. The root takes ownership of the exported buffers.
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            Data.importSchema(
                allocator, ArrowSchema.wrap(schema.memoryAddress()), NativeAllocator.DICTIONARIES),
            allocator);
    try {
      Data.importIntoVectorSchemaRoot(
          allocator, ArrowArray.wrap(array.memoryAddress()), root, NativeAllocator.DICTIONARIES);
      return root;
    } catch (Throwable failure) {
      root.close();
      throw failure;
    }
  }

  private static void releaseUnimported(ArrowArray array, ArrowSchema schema) {
    if (array != null && array.snapshot().release != 0) {
      array.release();
    }
    if (schema != null && schema.snapshot().release != 0) {
      schema.release();
    }
  }

  @Override
  public void close() {
    if (handle != 0) {
      Native.closeKeyedUpsertBuffer(handle);
      handle = 0;
    }
  }
}
