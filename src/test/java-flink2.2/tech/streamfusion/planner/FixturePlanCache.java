package tech.streamfusion.planner;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.flink.util.InstantiationUtil;

/** Retain unopened fixture definitions as bytes; each caller receives independent operators. */
final class FixturePlanCache {
  private final int maximumEntries;
  private final long maximumBytes;
  private final LinkedHashMap<List<?>, byte[]> plans = new LinkedHashMap<>(16, 0.75f, true);
  private long bytes;

  FixturePlanCache() {
    this(128, 32L * 1024 * 1024);
  }

  FixturePlanCache(int maximumEntries, long maximumBytes) {
    if (maximumEntries < 1 || maximumBytes < 1) throw new IllegalArgumentException();
    this.maximumEntries = maximumEntries;
    this.maximumBytes = maximumBytes;
  }

  <T extends Serializable> T get(List<?> key, Factory<T> factory) throws Exception {
    return get(
        key, factory, Boolean.parseBoolean(System.getProperty("sf.fixturePlanCache", "true")));
  }

  synchronized <T extends Serializable> T get(List<?> key, Factory<T> factory, boolean enabled)
      throws Exception {
    if (!enabled) return factory.create();
    byte[] serialized = plans.get(key);
    if (serialized == null) {
      serialized = InstantiationUtil.serializeObject(factory.create());
      if (serialized.length <= maximumBytes) {
        while (plans.size() >= maximumEntries || bytes + serialized.length > maximumBytes) {
          var oldest = plans.entrySet().iterator();
          bytes -= oldest.next().getValue().length;
          oldest.remove();
        }
        plans.put(List.copyOf(key), serialized);
        bytes += serialized.length;
      }
    }
    return InstantiationUtil.deserializeObject(serialized, FixturePlanCache.class.getClassLoader());
  }

  @FunctionalInterface
  interface Factory<T> {
    T create() throws Exception;
  }
}
