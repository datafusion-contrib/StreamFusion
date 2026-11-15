package tech.streamfusion.compat;

import java.util.List;
import org.apache.calcite.rex.RexNode;

public final class ExpandedLookupCalc {
  private final List<RexNode> projection;
  private final RexNode filter;

  public ExpandedLookupCalc(List<RexNode> projection, RexNode filter) {
    this.projection = projection;
    this.filter = filter;
  }

  public List<RexNode> projection() {
    return projection;
  }

  public RexNode filter() {
    return filter;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    ExpandedLookupCalc that = (ExpandedLookupCalc) other;
    return java.util.Objects.equals(projection, that.projection)
        && java.util.Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + java.util.Objects.hashCode(projection);
    result = 31 * result + java.util.Objects.hashCode(filter);
    return result;
  }

  @Override
  public String toString() {
    return "ExpandedLookupCalc[projection=" + projection + ", filter=" + filter + "]";
  }
}
