package tech.streamfusion.hudi;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.adapter.DataStreamSinkProviderAdapter;
import org.apache.hudi.sink.bucket.BucketStreamWriteOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;
import org.apache.hudi.table.HoodieTableSink;
import tech.streamfusion.operator.ArrowBatch;

/** Version-pinned graph boundary; SQL admission is separate from this payload adaptation. */
final class HudiMorSink {
  private HudiMorSink() {}

  @SuppressWarnings({"rawtypes", "unchecked"})
  static DataStreamSink<?> appendValidated(
      DataStream<ArrowBatch> input,
      HoodieTableSink table,
      RowType type,
      DynamicTableSink.Context context) {
    var copied = (HoodieTableSink) table.copy();
    var provider = (DataStreamSinkProviderAdapter) copied.getSinkRuntimeProvider(context);
    var roots = input.getExecutionEnvironment().getTransformations();
    int previous = roots.size();
    DataStream<RowData> host =
        new DataStream<>(
            input.getExecutionEnvironment(), (Transformation) input.getTransformation());
    var sink = provider.consumeDataStream(host);
    var created = new java.util.ArrayList<>(roots.subList(previous, roots.size()));
    var pending = new ArrayDeque<Transformation<?>>();
    var seen = Collections.newSetFromMap(new IdentityHashMap<Transformation<?>, Boolean>());
    pending.add(sink.getTransformation());
    int replaced = 0;
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (!seen.add(next)) continue;
      pending.addAll(next.getInputs());
      if (!(next instanceof OneInputTransformation)) continue;
      var operator = (OneInputTransformation<?, ?>) next;
      var factory = operator.getOperatorFactory();
      if (!(factory instanceof SimpleOperatorFactory)
          || ((SimpleOperatorFactory<?>) factory).getOperator().getClass()
              != BucketStreamWriteOperator.class) continue;
      if (operator.getParallelism() != 1 || operator.getStateKeySelector() != null)
        throw new IllegalArgumentException(
            "MOR Arrow admission requires one unkeyed bucket writer");
      try {
        var replacement = OneInputTransformation.class.getDeclaredField("operatorFactory");
        replacement.setAccessible(true);
        replacement.set(
            operator,
            WriteOperatorFactory.instance(
                copied.getConf(), new HudiMorOperator(copied.getConf(), type)));
        var edge = OneInputTransformation.class.getDeclaredField("input");
        edge.setAccessible(true);
        // With one writer, Hudi's key partitioner routes to zero; a single input needs no shuffle.
        edge.set(
            operator, (input.getParallelism() == 1 ? input : input.global()).getTransformation());
      } catch (ReflectiveOperationException failure) {
        throw new IllegalArgumentException("Unsupported released Hudi bucket topology", failure);
      }
      replaced++;
    }
    if (replaced != 1)
      throw new IllegalArgumentException("Expected one Hudi bucket writer, found " + replaced);
    seen.clear();
    pending.add(sink.getTransformation());
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (seen.add(next)) pending.addAll(next.getInputs());
    }
    // Flink also translates registered roots without a sink. Remove only our bypassed row stages.
    roots.removeIf(root -> created.contains(root) && !seen.contains(root));
    return sink;
  }
}
