package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/** Parquet source and sink rewrites contributed only by streamfusion-parquet. */
public final class ParquetPlannerExtension implements NativePlannerExtension {

  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(StreamPhysicalSink.class, ParquetSinkMatcher::substitute)
            .matching(ParquetSinkMatcher::appliesTo)
            .changelogSafe());
    entries.add(
        Substitution.of(
                StreamPhysicalTableSourceScan.class,
                "parquetSource",
                ParquetSourceMatcher::substitute)
            .matching(ParquetSourceMatcher::matches));
  }
}
