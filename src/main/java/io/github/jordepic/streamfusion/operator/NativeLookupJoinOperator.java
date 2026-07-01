package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.arrow.ArrowReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Processing-time lookup join, columnar in and out: for each probe {@link ArrowBatch} it reads the
 * rows, builds each row's lookup key, and calls the connector's synchronous {@link LookupFunction}
 * (the very function Flink's {@code LookupJoinRunner} would call), emitting the joined rows as an Arrow
 * batch. Because it runs the real lookup function and Flink's {@link JoinedRowData} join semantics —
 * inner drops non-matches, left null-pads — the result is byte-identical to the host; keeping it an
 * Arrow operator lets the probe-side Calc/source stay in the native island rather than the whole query
 * falling back around the lookup.
 *
 * <p>Only the dimension lookup is row-oriented (as it must be — a per-key point lookup); the probe
 * batch and the emitted batch are Arrow. Synchronous lookups only — an async lookup function needs the
 * operator mailbox and falls back to the host (ticket 40). The lookup function lives in this operator
 * (built at plan time), so this covers execution sharing the planner's JVM.
 */
public class NativeLookupJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final LookupFunction lookupFunction;
  private final RowType probeType;
  private final RowType dimType;
  private final RowType outputType;
  private final int[] probeKeyIndices;
  private final int joinType;

  private transient BufferAllocator allocator;
  private transient RowData.FieldGetter[] keyGetters;
  private transient RowDataSerializer probeSerializer;
  private transient RowDataSerializer dimSerializer;
  private transient GenericRowData nullDimRow;

  public NativeLookupJoinOperator(
      LookupFunction lookupFunction,
      RowType probeType,
      RowType dimType,
      RowType outputType,
      int[] probeKeyIndices,
      int joinType) {
    this.lookupFunction = lookupFunction;
    this.probeType = probeType;
    this.dimType = dimType;
    this.outputType = outputType;
    this.probeKeyIndices = probeKeyIndices;
    this.joinType = joinType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    keyGetters = new RowData.FieldGetter[probeKeyIndices.length];
    for (int i = 0; i < probeKeyIndices.length; i++) {
      keyGetters[i] =
          RowData.createFieldGetter(probeType.getTypeAt(probeKeyIndices[i]), probeKeyIndices[i]);
    }
    probeSerializer = new RowDataSerializer(probeType);
    dimSerializer = new RowDataSerializer(dimType);
    nullDimRow = new GenericRowData(dimType.getFieldCount());
    lookupFunction.open(new FunctionContext(getRuntimeContext()));
  }

  @Override
  public void close() throws Exception {
    lookupFunction.close();
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    List<RowData> outRows = new ArrayList<>();
    try (VectorSchemaRoot root = element.getValue().root()) {
      ArrowReader reader = ArrowConversion.createArrowReader(root, probeType);
      int rowCount = root.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        RowData probe = reader.read(i);
        Collection<RowData> matches = lookupFunction.lookup(buildKey(probe));
        if (matches != null && !matches.isEmpty()) {
          for (RowData dim : matches) {
            outRows.add(
                new JoinedRowData(
                    RowKind.INSERT, probeSerializer.copy(probe), dimSerializer.copy(dim)));
          }
        } else if (joinType == 1) { // LEFT: null-pad the dimension side
          outRows.add(new JoinedRowData(RowKind.INSERT, probeSerializer.copy(probe), nullDimRow));
        }
      }
    }
    // Insert-only: a processing-time lookup requires an append-only probe, so no row-kind column.
    VectorSchemaRoot out = RowDataArrowConverter.write(outRows, outputType, allocator, false);
    output.collect(new StreamRecord<>(new ArrowBatch(out)));
  }

  private RowData buildKey(RowData probe) {
    GenericRowData key = new GenericRowData(keyGetters.length);
    for (int i = 0; i < keyGetters.length; i++) {
      key.setField(i, keyGetters[i].getFieldOrNull(probe));
    }
    return key;
  }
}
