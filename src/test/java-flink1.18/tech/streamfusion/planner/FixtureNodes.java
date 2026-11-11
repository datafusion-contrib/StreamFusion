package tech.streamfusion.planner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.graph.StreamNode;

/** Released 1.18 StreamNode graphs cannot be serialized for definition caching. */
final class FixtureNodes implements Serializable {
  private final ArrayList<StreamNode> nodes;

  FixtureNodes(Collection<StreamNode> nodes) {
    this.nodes = new ArrayList<>(nodes);
  }

  static boolean cacheable() {
    return false;
  }

  Stream<StreamNode> stream() {
    return nodes.stream();
  }

  @Override
  public String toString() {
    return nodes.toString();
  }
}
