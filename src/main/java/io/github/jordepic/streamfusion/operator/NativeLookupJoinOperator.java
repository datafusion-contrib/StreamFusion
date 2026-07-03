package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.arrow.ArrowReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

/**
 * Processing-time lookup join, columnar in and out: for each probe {@link ArrowBatch} it reads the
 * rows and drives Flink's own {@link LookupJoinRunner} — the exact generated pipeline the host's
 * lookup join executes: pre-filter, key building (field references and constants alike), the
 * connector's real {@code LookupFunction}, the optional projection/filter on the dimension table, the
 * residual join condition, and LEFT null-padding — gathering the joined rows back into an Arrow batch.
 * Byte-identical to the host by construction, since the row-level core <em>is</em> the host's code;
 * keeping the operator's boundary Arrow lets the probe-side Calc/source stay in the native island
 * rather than the whole query falling back around the lookup.
 *
 * <p>Only the dimension lookup is row-oriented (as it must be — a per-key point lookup); the probe
 * batch and the emitted batch are Arrow. This is the synchronous connector path; an async connector
 * routes to {@link NativeAsyncLookupJoinOperator}. The runner carries generated code (compiled at
 * open), so the operator works on a distributed task manager, not just the planner's JVM.
 *
 * <p>The pre-filter runs under {@link FilterCondition.Context#INVALID_CONTEXT}, exactly as Flink's own
 * async lookup runner evaluates the same condition — a pre-filter needing the streaming context
 * (current watermark / record timestamp) fails loudly rather than silently diverging.
 */
public class NativeLookupJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final LookupJoinRunner runner;
  private final RowType probeType;
  private final RowType outputType;

  private transient BufferAllocator allocator;
  private transient RowDataSerializer outputSerializer;

  public NativeLookupJoinOperator(LookupJoinRunner runner, RowType probeType, RowType outputType) {
    this.runner = runner;
    this.probeType = probeType;
    this.outputType = outputType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    outputSerializer = new RowDataSerializer(outputType);
    FunctionUtils.setFunctionRuntimeContext(runner, getRuntimeContext());
    FunctionUtils.openFunction(runner, DefaultOpenContext.INSTANCE);
  }

  @Override
  public void close() throws Exception {
    FunctionUtils.closeFunction(runner);
    super.close();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    List<RowData> outRows = new ArrayList<>();
    // The runner reuses its joined-row objects across emissions, so gather copies.
    Collector<RowData> gather =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            outRows.add(outputSerializer.copy(row));
          }

          @Override
          public void close() {}
        };
    try (VectorSchemaRoot root = element.getValue().root()) {
      ArrowReader reader = ArrowConversion.createArrowReader(root, probeType);
      int rowCount = root.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        RowData probe = reader.read(i);
        runner.prepareCollector(probe, gather);
        if (runner.preFilter(FilterCondition.Context.INVALID_CONTEXT, probe)) {
          runner.doFetch(probe);
        }
        runner.padNullForLeftJoin(probe, gather);
      }
    }
    // Insert-only: a processing-time lookup requires an append-only probe, so no row-kind column.
    VectorSchemaRoot out = RowDataArrowConverter.write(outRows, outputType, allocator, false);
    output.collect(new StreamRecord<>(new ArrowBatch(out)));
  }
}
