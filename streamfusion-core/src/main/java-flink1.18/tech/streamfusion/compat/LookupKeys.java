package tech.streamfusion.compat;

import java.util.Map;
import java.util.Set;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;

public final class LookupKeys {
  private final Map<Integer, LookupJoinUtil.LookupKey> values;

  public LookupKeys(Map<Integer, LookupJoinUtil.LookupKey> values) {
    this.values = values;
  }

  public Map<Integer, LookupJoinUtil.LookupKey> values() {
    return values;
  }

  public Set<Integer> keySet() {
    return values.keySet();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    LookupKeys that = (LookupKeys) other;
    return java.util.Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + java.util.Objects.hashCode(values);
    return result;
  }

  @Override
  public String toString() {
    return "LookupKeys[values=" + values + "]";
  }
}
