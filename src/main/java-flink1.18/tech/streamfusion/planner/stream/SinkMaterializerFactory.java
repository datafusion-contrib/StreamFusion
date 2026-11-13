package tech.streamfusion.planner.stream;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.NativeSinkMaterializer;

final class SinkMaterializerFactory {
  private SinkMaterializerFactory() {}

  static NativeSinkMaterializer createCanonical(
      RowType type, int[] keys, int[] precisions, int[] upsertKeys, long ttl, int groups,
      String strategy, ReadableConfig config) {
    if (!strategy.equals("LEGACY"))
      throw new IllegalArgumentException("Flink 1.18 has no V2 sink materializer");
    return new NativeSinkMaterializer(keys, precisions, upsertKeys, ttl, groups);
  }
}
