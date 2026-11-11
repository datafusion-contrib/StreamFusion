package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import tech.streamfusion.planner.common.StreamRule;

/** ORC contributes its codec and admission rules to the shared filesystem sink. */
public final class OrcPlannerExtension implements NativePlannerExtension {
  private static final FileSinkMatcher.Format FORMAT =
      new FileSinkMatcher.Format() {
        public String name() {
          return "orc";
        }

        public FileSinkMatcher.Settings settings(
            java.util.Map<String, String> options,
            org.apache.flink.table.types.logical.RowType type,
            List<String> partitions) {
          var result = OrcSinkTranslator.translate(options, type, partitions);
          return new FileSinkMatcher.Settings(
              result.keys(), result.values(), result.fallbackReason());
        }

        public tech.streamfusion.format.ColumnarFileCodec codec(
            org.apache.flink.table.types.logical.RowType type, List<String> partitions) {
          return new tech.streamfusion.orc.OrcCodec(OrcSinkTranslator.schema(type, partitions));
        }
      };

  public void addSubstitutions(List<StreamRule<?>> entries) {
    entries.add(
        StreamRule.of(
                StreamPhysicalSink.class,
                (sink, ctx) -> FileSinkMatcher.substitute(sink, ctx, FORMAT))
            .matching(sink -> FileSinkMatcher.appliesTo(sink, FORMAT))
            .changelogSafe());
  }
}
