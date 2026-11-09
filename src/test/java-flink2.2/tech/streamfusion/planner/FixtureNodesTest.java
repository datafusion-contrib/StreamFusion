package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.junit.jupiter.api.Test;

class FixtureNodesTest {
  @Test
  void cachedNodesRestoreTransientFactoriesWithIndependentMutableFunctions() throws Exception {
    var cache = new FixturePlanCache();
    FixturePlanCache.Factory<FixtureNodes> build =
        () -> {
          var node =
              new StreamNode(
                  7,
                  "slot",
                  null,
                  new StreamMap<>(new CountingMapper()),
                  "mapper",
                  OneInputStreamTask.class);
          node.setSerializerOut(StringSerializer.INSTANCE);
          return new FixtureNodes(List.of(node));
        };
    var first = cache.get(List.of("map"), build, true).stream().findFirst().orElseThrow();
    var second = cache.get(List.of("map"), build, true).stream().findFirst().orElseThrow();
    assertNotSame(first, second);
    assertNotSame(first.getOperatorFactory(), second.getOperatorFactory());
    assertEquals(StringSerializer.INSTANCE, second.getTypeSerializerOut());
    var a =
        (StreamMap<String, String>)
            ((SimpleOperatorFactory<?>) first.getOperatorFactory()).getOperator();
    var b =
        (StreamMap<String, String>)
            ((SimpleOperatorFactory<?>) second.getOperatorFactory()).getOperator();
    assertEquals("row:1", a.getUserFunction().map("row"));
    assertEquals("row:2", a.getUserFunction().map("row"));
    assertEquals("row:1", b.getUserFunction().map("row"));
  }

  private static final class CountingMapper extends RichMapFunction<String, String> {
    private int calls;

    @Override
    public String map(String value) {
      return value + ":" + ++calls;
    }
  }
}
