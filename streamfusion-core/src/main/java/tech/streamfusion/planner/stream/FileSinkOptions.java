package tech.streamfusion.planner.stream;

import java.util.Set;

/** Settings implemented by the reused Flink filesystem lifecycle. */
final class FileSinkOptions {
  /** Consumed by the reused Flink writer/committer classes, so honored without translation. */
  static final Set<String> HOST_HONORED =
      Set.of(
          "connector",
          "format",
          "path",
          "partition.default-name",
          "sink.rolling-policy.file-size",
          "sink.rolling-policy.rollover-interval",
          "sink.rolling-policy.inactivity-interval",
          "sink.rolling-policy.check-interval",
          "partition.time-extractor.kind",
          "partition.time-extractor.class",
          "partition.time-extractor.timestamp-formatter",
          "partition.time-extractor.timestamp-pattern",
          "sink.partition-commit.trigger",
          "sink.partition-commit.delay",
          "sink.partition-commit.watermark-time-zone",
          "sink.partition-commit.policy.kind",
          "sink.partition-commit.policy.class",
          "sink.partition-commit.policy.class.parameters",
          "sink.partition-commit.success-file.name",
          "sink.parallelism");

  /**
   * No effect on this sink in stock Flink: shuffle-by-partition is registered but consumed nowhere
   * in the streaming filesystem sink, compaction sizing only applies when auto-compaction is on,
   * and source options never reach a sink.
   */
  static final Set<String> HOST_IGNORED =
      Set.of(
          "sink.shuffle-by-partition.enable",
          "compaction.file-size",
          "compaction.parallelism",
          "source.monitor-interval",
          "source.report-statistics",
          "source.path.regex-pattern");
}
