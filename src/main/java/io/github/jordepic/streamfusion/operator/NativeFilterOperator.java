package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Stateless native filter, columnar in and out: applies the encoded predicate to each incoming Arrow
 * batch natively and emits the surviving rows (projected to an input-column subset/reorder) as a
 * batch. The predicate is compiled once into a native handle reused across batches. Carrying Arrow
 * lets this chain with other native operators without converting to rows between them.
 */
public class NativeFilterOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] projection;
  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long predicate;

  public NativeFilterOperator(
      int[] projection,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings) {
    this.projection = projection;
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    predicate = Native.createFilterExpression(kinds, payload, childCounts, longs, doubles, strings);
  }

  @Override
  public void close() throws Exception {
    if (predicate != 0) {
      Native.closeFilterExpression(predicate);
      predicate = 0;
    }
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    // The input batch's buffers belong to the upstream operator's allocator; the C Data export must
    // use that same allocator (buffers can only associate within one allocator root). The operator's
    // own allocator owns only the imported result.
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    VectorSchemaRoot filtered;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.filterExpression(
          predicate,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      filtered = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close(); // the input batch is consumed
    }
    output.collect(new StreamRecord<>(new ArrowBatch(project(filtered))));
  }

  /**
   * Selects the projected columns of {@code filtered} into the output batch. For an identity
   * projection the batch passes through; otherwise the projected columns are transferred into a new
   * batch (moving buffer ownership, no copy) and the filtered batch is closed, freeing the columns
   * left out.
   */
  private VectorSchemaRoot project(VectorSchemaRoot filtered) {
    if (isIdentity(filtered.getFieldVectors().size())) {
      return filtered;
    }
    List<FieldVector> columns = new ArrayList<>(projection.length + 1);
    for (int index : projection) {
      TransferPair pair = filtered.getVector(index).getTransferPair(allocator);
      pair.transfer();
      columns.add((FieldVector) pair.getTo());
    }
    // Carry the changelog tag: if the (filtered) batch has a $row_kind$ column, append it so the
    // projection stays changelog-safe — the rows were already filtered alongside it. A deterministic
    // per-row filter + column subset over a retracting stream matches the host's Calc exactly.
    FieldVector rowKind = filtered.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
    if (rowKind != null) {
      TransferPair pair = rowKind.getTransferPair(allocator);
      pair.transfer();
      columns.add((FieldVector) pair.getTo());
    }
    int rows = filtered.getRowCount();
    filtered.close();
    VectorSchemaRoot result = new VectorSchemaRoot(columns);
    result.setRowCount(rows);
    return result;
  }

  private boolean isIdentity(int columnCount) {
    if (projection.length != columnCount) {
      return false;
    }
    for (int i = 0; i < projection.length; i++) {
      if (projection[i] != i) {
        return false;
      }
    }
    return true;
  }
}
