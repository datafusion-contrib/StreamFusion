package tech.streamfusion.planner;

import java.util.stream.IntStream;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.SinkUpsertMaterializeStrategy;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.NativeSinkMaterializer;

/** Keeps released materializer strategy selection consistent across native sink edges. */
final class SinkMaterializerFactory {
  private SinkMaterializerFactory() {}

  static NativeSinkMaterializer create(
      RowType type,
      int[] keys,
      int[] precisions,
      int[] upsertKeys,
      long ttl,
      int groups,
      SinkUpsertMaterializeStrategy strategy,
      ReadableConfig config) {
    if (strategy == SinkUpsertMaterializeStrategy.LEGACY)
      return new NativeSinkMaterializer(keys, precisions, upsertKeys, ttl, groups);
    return new StrategyMaterializer(
        type,
        keys,
        precisions,
        upsertKeys,
        groups,
        strategy,
        config.get(
            ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH),
        config.get(
            ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW));
  }

  private static final class StrategyMaterializer extends NativeSinkMaterializer {
    private final SinkUpsertMaterializeStrategy strategy;
    private final Long high, low;
    private final int[] matchPrecisions;
    private boolean serializedKeys;
    private long effectiveHigh, effectiveLow;

    StrategyMaterializer(
        RowType type,
        int[] keys,
        int[] precisions,
        int[] upsertKeys,
        int groups,
        SinkUpsertMaterializeStrategy strategy,
        Long high,
        Long low) {
      super(keys, precisions, upsertKeys, 0, groups);
      this.strategy = strategy;
      this.high = high;
      this.low = low;
      int[] match =
          upsertKeys == null || upsertKeys.length == 0
              ? IntStream.range(0, type.getFieldCount()).toArray()
              : upsertKeys;
      matchPrecisions = FlinkKeyGroupUtils.timestampPrecisions(type, match);
    }

    @Override
    protected void beforeHandleCreation() {
      String backend = getKeyedStateStore().getBackendTypeIdentifier().trim();
      boolean heap = backend.equalsIgnoreCase("heap") || backend.equalsIgnoreCase("hashmap");
      serializedKeys = !heap;
      if (strategy == SinkUpsertMaterializeStrategy.ADAPTIVE) {
        effectiveHigh = high == null ? (heap ? 400 : 50) : high;
        effectiveLow = low == null ? (heap ? 300 : 40) : low;
        org.apache.flink.util.Preconditions.checkArgument(effectiveHigh > effectiveLow);
      }
    }

    @Override
    protected int strategyMode() {
      switch (strategy) {
        case VALUE:
          return 1;
        case MAP:
          return serializedKeys ? 4 : 2;
        case ADAPTIVE:
          return serializedKeys ? 5 : 3;
        default:
          throw new IllegalStateException("Unexpected materializer strategy: " + strategy);
      }
    }

    @Override
    protected int[] matchTimestampPrecisions() {
      return matchPrecisions;
    }

    @Override
    protected long adaptiveHighThreshold() {
      return effectiveHigh;
    }

    @Override
    protected long adaptiveLowThreshold() {
      return effectiveLow;
    }
  }
}
