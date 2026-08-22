package tech.streamfusion.planner;

import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.sql.DeltaDynamicTableSinkFactory;
import io.delta.flink.sink.sql.FlinkUnityCatalogFactory;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.delta.ArrowToDeltaRowsOperator;
import tech.streamfusion.delta.NativeDeltaHadoopTable;
import tech.streamfusion.delta.NativeDeltaSink;
import tech.streamfusion.delta.PartitionedArrowToDeltaRowsOperator;
import tech.streamfusion.delta.KernelBatchRowDataTypeInformation;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ParquetPartitionSplitOperator;
import tech.streamfusion.operator.PartitionedArrowBatch;
import tech.streamfusion.operator.PartitionedArrowBatchTypeInformation;

/** Builds a columnar Delta write followed by the stock Delta merge and commit topology. */
public final class NativeDeltaSinkExecNode extends ExecNodeBase<Object>
    implements StreamExecNode<Object>, SingleTransformationTranslator<Object> {

  private final DeltaSinkMatcher.Planned planned;

  public NativeDeltaSinkExecNode(
      org.apache.flink.configuration.ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      DeltaSinkMatcher.Planned planned) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-delta-sink_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.planned = planned;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<Object> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    Configuration options = Configuration.fromMap(planned.options);
    Integer configuredParallelism =
        options.getOptional(org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM)
            .orElse(null);
    boolean parallelismConfigured = configuredParallelism != null;
    int parallelism = parallelismConfigured ? configuredParallelism : input.getParallelism();

    DataStream<RowData> rows;
    if (planned.partitionKeys.isEmpty()) {
      OneInputTransformation<ArrowBatch, RowData> views =
          new OneInputTransformation<>(
              input,
              "native-delta-arrow-views",
              SimpleOperatorFactory.of(new ArrowToDeltaRowsOperator(planned.rowType)),
              KernelBatchRowDataTypeInformation.INSTANCE,
              parallelism,
              parallelismConfigured);
      rows = new DataStream<>(planner.getExecEnv(), views);
    } else {
      ParquetPartitionSplitOperator splitter =
          new ParquetPartitionSplitOperator(planned.rowType, planned.partitionKeys, "__DEFAULT_PARTITION__");
      OneInputTransformation<ArrowBatch, PartitionedArrowBatch> split =
          new OneInputTransformation<>(
              input,
              "native-delta-partition-split",
              SimpleOperatorFactory.of(splitter),
              PartitionedArrowBatchTypeInformation.INSTANCE,
              input.getParallelism(),
              false);
      DataStream<PartitionedArrowBatch> routed =
          new DataStream<>(planner.getExecEnv(), split)
              .keyBy(PartitionedArrowBatch::bucketId);
      SingleOutputStreamOperator<RowData> views =
          routed.transform(
              "native-delta-arrow-views",
              KernelBatchRowDataTypeInformation.INSTANCE,
              new PartitionedArrowToDeltaRowsOperator(planned.rowType));
      // The Arrow-backed RowData view cannot cross a serialized edge. Keep partition routing,
      // view creation, and the writer at one explicit parallelism even when the input inherited a
      // non-default parallelism from an upstream source or global operator.
      views.setParallelism(parallelism);
      rows = views;
    }

    DeltaSinkConf sinkConf = new DeltaSinkConf(planned.rowType, planned.options);
    StructType deltaSchema = sinkConf.getSinkSchema();
    Map<String, String> tableOptions = new LinkedHashMap<>(planned.options);
    if (planned.path != null) {
      tableOptions.put("type", "hadoop");
      tableOptions.put("hadoop.table_path", planned.path);
    } else {
      tableOptions.putIfAbsent("type", "unitycatalog");
      tableOptions.putIfAbsent("unitycatalog.name", planned.catalogName);
      tableOptions.putIfAbsent("unitycatalog.table_name", planned.tableName);
      if (!tableOptions.containsKey("unitycatalog.endpoint")
          && planned.options.containsKey(FlinkUnityCatalogFactory.ENDPOINT.key())) {
        tableOptions.put(
            "unitycatalog.endpoint",
            planned.options.get(FlinkUnityCatalogFactory.ENDPOINT.key()));
      }
      if (!tableOptions.containsKey("unitycatalog.token")
          && planned.options.containsKey(FlinkUnityCatalogFactory.TOKEN.key())) {
        tableOptions.put(
            "unitycatalog.token", planned.options.get(FlinkUnityCatalogFactory.TOKEN.key()));
      }
    }
    DeltaTable table =
        new NativeDeltaHadoopTable(
            URI.create(planned.path), tableOptions, deltaSchema, planned.partitionKeys);
    // DeltaTable's serializable table state is populated by open(). The sink and committer are
    // serialized separately, so initialize that state before Flink snapshots the topology.
    table.open();
    NativeDeltaSink sink = new NativeDeltaSink(table, sinkConf);
    String uid =
        planned.options.getOrDefault(
            DeltaDynamicTableSinkFactory.UID.key(), UUID.randomUUID().toString());
    String name =
        planned.options.getOrDefault(
            DeltaDynamicTableSinkFactory.NAME.key(), "native-delta-sink");
    DataStreamSink<RowData> end = rows.sinkTo(sink).uid(uid).name(name);
    // Arrow-backed RowData is an ownership-carrying view and deliberately has no byte serializer.
    // Keep the writer at the view operator's parallelism even when an upstream global aggregate
    // reduced it below the environment default; otherwise Sink V2 inserts a network edge between
    // the view and writer (q5 is the canonical example).
    end.setParallelism(parallelism);
    return (Transformation<Object>) (Transformation<?>) end.getTransformation();
  }
}
