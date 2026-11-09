package tech.streamfusion.planner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

/** StreamNode deliberately makes its operator factory transient; retain both as Flink does. */
final class FixtureNodes implements Serializable {
  private final ArrayList<StreamNode> nodes;
  private final ArrayList<StreamOperatorFactory<?>> factories;

  FixtureNodes(Collection<StreamNode> nodes) {
    this.nodes = new ArrayList<>(nodes);
    this.factories = new ArrayList<>();
    this.nodes.forEach(node -> factories.add(node.getOperatorFactory()));
  }

  private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
    input.defaultReadObject();
    for (int index = 0; index < nodes.size(); index++) {
      nodes.get(index).setOperatorFactory(factories.get(index));
    }
  }

  static boolean cacheable() {
    return true;
  }

  Stream<StreamNode> stream() {
    return nodes.stream();
  }

  @Override
  public String toString() {
    return nodes.toString();
  }
}
