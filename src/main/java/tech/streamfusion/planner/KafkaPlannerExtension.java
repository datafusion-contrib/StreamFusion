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
                    KafkaTables.isCdcDecode(scan)
                        && NativeConfig.operatorEnabled("kafkaDecode"))
            .changelogSafe());
    entries.add(
        Substitution.of(RelNode.class, KafkaTables::reportCdcWatermark)
            .changelogSafe()
            .yieldingOnDecline());
    entries.add(
        Substitution.of(RelNode.class, KafkaTables::reportAppendWatermark).yieldingOnDecline());
  }

  @Override
  public String sourceSharingKey(RelNode node) {
    if (node instanceof StreamPhysicalTableSourceScan && KafkaTables.isNativeKafkaDecode(node)) {
      return KafkaTables.decodeSharingKey((StreamPhysicalTableSourceScan) node);
    }
    return null;
  }
}
