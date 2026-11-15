package tech.streamfusion.compat;

import java.util.Map;
import java.util.Set;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;

public final class LookupKeys {
  private final Map<Integer, FunctionCallUtil.FunctionParam> values;

  public LookupKeys(Map<Integer, FunctionCallUtil.FunctionParam> values) {
    this.values = values;
  }

  public Map<Integer, FunctionCallUtil.FunctionParam> values() {
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
