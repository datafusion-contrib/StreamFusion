package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/** Kafka source and sink rewrites contributed only by streamfusion-kafka. */
public final class KafkaPlannerExtension implements NativePlannerExtension {

  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(StreamPhysicalSink.class, "kafkaSink", KafkaSinkMatcher::substitute)
            .matching(KafkaSinkMatcher::appliesTo)
            .changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalTableSourceScan.class, KafkaTables::substituteDecode)
            .matching(
                scan ->
                    KafkaTables.isNativeKafkaDecode(scan)
                        && NativeConfig.operatorEnabled("kafkaDecode")));
    entries.add(
        Substitution.of(StreamPhysicalTableSourceScan.class, KafkaTables::substituteDecode)
            .matching(
                scan ->
                    KafkaTables.isCdcDecode(scan) && NativeConfig.operatorEnabled("kafkaDecode"))
            .changelogSafe());
    entries.add(
        Substitution.of(RelNode.class, KafkaTables::reportCdcWatermark)
            .changelogSafe()
            .yieldingOnDecline());
    entries.add(
        Substitution.of(RelNode.class, KafkaTables::reportAppendWatermark).yieldingOnDecline());
  }

  @Override
  public boolean preservesStockSourceState(
      org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> source) {
    return source instanceof NativeKafkaDecodeExecNode;
  }

  @Override
  public boolean preservesStockSinkState(
      org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> sink) {
    return sink instanceof NativeKafkaSinkExecNode;
  }

  @Override
  public java.util.Set<Class<?>> canonicalStatelessOperators() {
    return java.util.Set.of(tech.streamfusion.kafka.NativeKafkaSerializationOperator.class);
  }

  @Override
  public org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> lowerCompiledSink(
      StreamPhysicalSink sink,
      org.apache.flink.configuration.ReadableConfig config,
      org.apache.flink.table.connector.ChangelogMode inputMode) {
    if (!NativeConfig.operatorEnabled("kafkaSink") || !KafkaSinkMatcher.appliesTo(sink))
      return null;
    var planned = KafkaSinkMatcher.plan(sink);
    if (planned.fallbackReason != null
        || KafkaSinkMatcher.inputFallback(
                planned, inputMode.containsOnly(org.apache.flink.types.RowKind.INSERT))
            != null) return null;
    return new NativeKafkaSinkExecNode(
        config,
        org.apache.flink.table.planner.plan.nodes.exec.InputProperty.DEFAULT,
        org.apache.flink.table.planner.calcite.FlinkTypeFactory$.MODULE$.toLogicalRowType(
            sink.getRowType()),
        "NativeKafkaSink(table=["
            + sink.contextResolvedTable().getIdentifier().asSummaryString()
            + "])",
        planned);
  }

  @Override
  public org.apache.flink.table.planner.plan.nodes.exec.ExecNode<
          tech.streamfusion.operator.ArrowBatch>
      lowerCompiledSource(
          StreamPhysicalTableSourceScan scan,
          org.apache.flink.configuration.ReadableConfig config) {
    if (!NativeConfig.operatorEnabled("kafkaDecode")
        || !(KafkaTables.isNativeKafkaDecode(scan) || KafkaTables.isCdcDecode(scan))) return null;
    return new NativeKafkaDecodeExecNode(
        config,
        org.apache.flink.table.planner.calcite.FlinkTypeFactory$.MODULE$.toLogicalRowType(
            scan.getRowType()),
        FilesystemTables.physicalRowType(scan),
        "NativeKafkaDecode(table=[" + String.join(".", scan.getTable().getQualifiedName()) + "])",
        KafkaTables.decodeOptions(scan),
        ScanWatermarkSpec.of(scan));
  }

  @Override
  public String sourceSharingKey(RelNode node) {
    if (node instanceof StreamPhysicalTableSourceScan && KafkaTables.isNativeKafkaDecode(node)) {
      return KafkaTables.decodeSharingKey((StreamPhysicalTableSourceScan) node);
    }
    return null;
  }
}
