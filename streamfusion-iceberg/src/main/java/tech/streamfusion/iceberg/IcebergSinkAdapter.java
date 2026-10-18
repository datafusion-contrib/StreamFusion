package tech.streamfusion.iceberg;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import tech.streamfusion.operator.ArrowBatch;

/** Reuses the released legacy sink graph, changing only the writer's payload/factory boundary. */
final class IcebergSinkAdapter {
  private static final String WRITER = "org.apache.iceberg.flink.sink.IcebergStreamWriter";

  private IcebergSinkAdapter() {}

  static boolean compatible() {
    return "1.12.0".equals(IcebergBuild.version())
        && org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("2.2.");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static DataStreamSink<Void> append(
      DataStream<ArrowBatch> input,
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      IcebergTaskWriterFactory.Decorator decorator) {
    return append(input, table, loader, configuration, options, null, decorator);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static DataStreamSink<Void> append(
      DataStream<ArrowBatch> input,
      Table table,
      TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      String uidPrefix,
      IcebergTaskWriterFactory.Decorator decorator) {
    if (!compatible()) {
      throw new IllegalArgumentException("Iceberg writer adapter requires released Iceberg 1.12.0");
    }
    FlinkWriteConf writeConf = new FlinkWriteConf(table, options, configuration);
    if ((writeConf.distributionMode() != DistributionMode.NONE
            && writeConf.distributionMode() != DistributionMode.HASH
            && writeConf.distributionMode() != DistributionMode.RANGE)
        || writeConf.overwriteMode()) {
      throw new IllegalArgumentException(
          "Iceberg streaming adapter requires none/hash/range distribution");
    }
    // No RowData crosses this edge: replace the sole writer consumer before graph serialization.
    DataStream<RowData> hostInput =
        new DataStream<>(
            input.getExecutionEnvironment(), (Transformation) input.getTransformation());
    var builder =
        FlinkSink.forRowData(hostInput)
            .table(table)
            .tableLoader(loader)
            .flinkConf(configuration)
            .setAll(options);
    if (uidPrefix != null) builder.uidPrefix(uidPrefix);
    DataStreamSink<Void> sink = builder.append();
    if (input.getExecutionEnvironment().getCheckpointConfig().isUnalignedCheckpointsEnabled()) {
      alignCommitInput(sink.getTransformation());
    }
    int replaced = 0;
    for (Transformation<?> transformation : predecessors(sink.getTransformation())) {
      if (!(transformation instanceof OneInputTransformation)) continue;
      var factory = ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
      if (!(factory instanceof SimpleOperatorFactory)) continue;
      Object operator = ((SimpleOperatorFactory<?>) factory).getOperator();
      if (!operator.getClass().getName().equals(WRITER)) continue;
      if (writeConf.distributionMode() == DistributionMode.RANGE
          && table.schema().identifierFieldIds().isEmpty()) {
        adaptRangeInput((OneInputTransformation) transformation, table);
      } else {
        adaptHashInput((OneInputTransformation) transformation, input, table);
      }
      Field field = field(operator.getClass(), "taskWriterFactory");
      try {
        RowDataTaskWriterFactory original = (RowDataTaskWriterFactory) field.get(operator);
        field.set(
            operator,
            new IcebergTaskWriterFactory(
                original,
                FlinkSchemaUtil.convert(table.schema()),
                decorator,
                (org.apache.flink.streaming.api.operators.AbstractStreamOperator<?>) operator));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Cannot install released Iceberg writer adapter", e);
      }
      replaced++;
    }
    if (replaced != 1) {
      throw new IllegalStateException("Expected one normal Iceberg writer, found " + replaced);
    }
    return sink;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void alignCommitInput(Transformation<?> sink) {
    int changed = 0;
    for (var transformation : predecessors(sink)) {
      if (!(transformation instanceof OneInputTransformation committer)
          || !committer
              .getOperatorFactory()
              .getClass()
              .getName()
              .equals("org.apache.iceberg.flink.sink.IcebergFilesCommitterFactory")) continue;
      var partitioner = new org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner<>();
      partitioner.disableUnalignedCheckpoints();
      try {
        field(OneInputTransformation.class, "input")
            .set(
                committer,
                new PartitionTransformation(
                    (Transformation) committer.getInputs().get(0), partitioner));
      } catch (IllegalAccessException failure) {
        throw new IllegalStateException("Cannot align released Iceberg commit results", failure);
      }
      changed++;
    }
    if (changed != 1) throw new IllegalStateException("Expected one released Iceberg committer");
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void adaptRangeInput(OneInputTransformation writer, Table table) {
    if (!(writer.getInputs().get(0) instanceof OneInputTransformation flatten)
        || !(flatten.getInputs().get(0) instanceof PartitionTransformation partition)
        || !(partition.getInputs().get(0) instanceof OneInputTransformation statistics)
        || !(statistics.getOperatorFactory()
            instanceof org.apache.iceberg.flink.sink.shuffle.DataStatisticsOperatorFactory factory)
        || !(partition.getPartitioner()
            instanceof org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper)) {
      throw new IllegalStateException("Unverified released Iceberg range topology");
    }
    try {
      var custom = partition.getPartitioner();
      var original =
          (org.apache.flink.api.common.functions.Partitioner<
                  org.apache.iceberg.flink.sink.shuffle.StatisticsOrRecord>)
              field(
                      org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper.class,
                      "partitioner")
                  .get(custom);
      if (!(original instanceof org.apache.iceberg.flink.sink.shuffle.RangePartitioner))
        throw new IllegalStateException("Unverified range partitioner");
      field(OneInputTransformation.class, "operatorFactory")
          .set(
              statistics,
              new IcebergRangeOperatorFactory(
                  factory,
                  original,
                  FlinkSchemaUtil.convert(table.schema()),
                  writer.getParallelism()));
      var channel =
          new org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper<
              Integer, ArrowBatch>(
              (selected, count) -> {
                if (selected < 0 || selected >= count)
                  throw new IllegalArgumentException("Invalid Arrow range destination");
                return selected;
              },
              ArrowBatch::keyGroup);
      field(PartitionTransformation.class, "partitioner").set(partition, channel);
      field(OneInputTransformation.class, "operatorFactory")
          .set(
              flatten,
              SimpleOperatorFactory.of(
                  new org.apache.flink.streaming.api.operators.StreamMap<ArrowBatch, ArrowBatch>(
                      batch -> batch)));
      for (Object transformation : List.of(statistics, partition, flatten)) {
        field(Transformation.class, "outputType")
            .set(transformation, tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE);
      }
    } catch (IllegalAccessException failure) {
      throw new IllegalStateException("Cannot adapt the released Iceberg range payload", failure);
    }
  }

  private static List<Transformation<?>> predecessors(Transformation<?> root) {
    var seen = new IdentityHashMap<Transformation<?>, Boolean>();
    var pending = new java.util.ArrayDeque<Transformation<?>>();
    var result = new ArrayList<Transformation<?>>();
    pending.add(root);
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (seen.put(next, true) != null) continue;
      result.add(next);
      pending.addAll(next.getInputs());
    }
    return result;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void adaptHashInput(
      OneInputTransformation writer, DataStream<ArrowBatch> input, Table table) {
    var selector = writer.getStateKeySelector();
    if (selector == null) return;
    if (!(writer.getInputs().get(0) instanceof PartitionTransformation)) {
      throw new IllegalStateException("Unsupported released Iceberg hash input layout");
    }
    PartitionTransformation original = (PartitionTransformation) writer.getInputs().get(0);
    if (!(original.getPartitioner()
        instanceof org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner)) {
      throw new IllegalStateException("Unsupported released Iceberg hash partitioner");
    }
    int max = writer.getMaxParallelism();
    if (max <= 0) max = input.getExecutionEnvironment().getMaxParallelism();
    if (max <= 0)
      max = KeyGroupRangeAssignment.computeDefaultMaxParallelism(writer.getParallelism());
    boolean recoverable =
        input.getExecutionEnvironment().getCheckpointConfig().isUnalignedCheckpointsEnabled();
    var router =
        new OneInputTransformation<ArrowBatch, ArrowBatch>(
            (Transformation<ArrowBatch>) original.getInputs().get(0),
            "iceberg-arrow-hash-router",
            org.apache.flink.streaming.api.operators.SimpleOperatorFactory.of(
                new IcebergHashRouter(
                    FlinkSchemaUtil.convert(table.schema()),
                    selector,
                    writer.getParallelism(),
                    max,
                    recoverable)),
            tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
            original.getParallelism(),
            false);
    var exchange =
        new PartitionTransformation<>(
            router,
            new tech.streamfusion.planner.ColumnarKeyGroupPartitioner(max, recoverable),
            original.getExchangeMode());
    try {
      field(OneInputTransformation.class, "input").set(writer, exchange);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot adapt the released Iceberg batch exchange", e);
    }
    writer.setStateKeySelector(new tech.streamfusion.operator.ArrowBatchSubtaskKeySelector(max));
    writer.setStateKeyType(org.apache.flink.api.common.typeinfo.Types.INT);
    writer.setMaxParallelism(max);
  }

  static Field field(Class<?> owner, String name) {
    try {
      Field field = owner.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unsupported released Iceberg writer layout", e);
    }
  }
}
