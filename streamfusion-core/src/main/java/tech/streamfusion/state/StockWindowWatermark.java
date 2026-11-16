package tech.streamfusion.state;

import java.util.Collections;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;

/** Released SQL window UNION watermark state, including its minimum-on-restore rule. */
public final class StockWindowWatermark {
  private final ListState<Long> state;
  private final long restored;

  public StockWindowWatermark(OperatorStateStore store, boolean isRestored) throws Exception {
    for (String name : store.getRegisteredStateNames())
      if (!name.equals("watermark"))
        throw new IllegalStateException("Cannot import window operator state '" + name + "'");
    if (!store.getRegisteredBroadcastStateNames().isEmpty())
      throw new IllegalStateException("Cannot import broadcast state into a stock window contract");
    state =
        store.getUnionListState(new ListStateDescriptor<>("watermark", LongSerializer.INSTANCE));
    long watermark = Long.MAX_VALUE;
    if (isRestored) {
      Iterable<Long> values = state.get();
      if (values != null) for (long value : values) watermark = Math.min(watermark, value);
    }
    restored = watermark == Long.MAX_VALUE ? Long.MIN_VALUE : watermark;
  }

  public long restoredWatermark() {
    return restored;
  }

  public void snapshot(long watermark) throws Exception {
    state.update(Collections.singletonList(watermark));
  }
}
