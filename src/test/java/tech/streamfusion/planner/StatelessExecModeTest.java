package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.common.CommonArrowToRowDataExecNode;
import tech.streamfusion.planner.common.CommonRowDataToArrowExecNode;
import tech.streamfusion.planner.common.CommonShareExecNode;
import tech.streamfusion.planner.common.CommonUnionExecNode;

class StatelessExecModeTest {
  private final Configuration config = new Configuration();
  private final RowType row = RowType.of(new IntType());

  @Test
  void batchDelegatesHaveNoStreamingModeAndKeepTheirWireType() {
    for (ExecNode<?> common :
        List.of(
            new CommonRowDataToArrowExecNode(
                config, InputProperty.DEFAULT, row, "entry", false, null),
            new CommonArrowToRowDataExecNode(config, InputProperty.DEFAULT, row, "exit"),
            new CommonShareExecNode(config, InputProperty.DEFAULT, row, "share", 2),
            new CommonUnionExecNode(
                config,
                List.of(InputProperty.DEFAULT, InputProperty.DEFAULT),
                row,
                "union",
                true))) {
      assertFalse(common instanceof StreamExecNode, common.getClass().getName());
      assertFalse(common instanceof BatchExecNode, common.getClass().getName());
      var batch = new BatchNativeExecNode<>(config, common);
      assertTrue(batch instanceof BatchExecNode);
      assertFalse(StreamExecNode.class.isInstance(batch));
      assertEquals(!(common instanceof CommonArrowToRowDataExecNode), batch.outputsArrow());
      assertEquals(common.getInputProperties(), batch.getInputProperties());
      assertEquals(common.getOutputType(), batch.getOutputType());
    }
  }

  @Test
  void publicStreamingEntriesKeepTheirModeAndFluentReturnTypes() {
    var entry =
        new RowDataToArrowExecNode(config, InputProperty.DEFAULT, row, "entry", false, null);
    assertTrue(entry instanceof StreamExecNode);
    assertFalse(entry instanceof BatchExecNode);
    assertEquals(entry, entry.withBinaryRowKeys());
    assertEquals(entry, entry.withByteStringColumns(new int[0]));
    assertFalse(entry.acceptsNativeFileSource());
    assertTrue(
        new ArrowToRowDataExecNode(config, InputProperty.DEFAULT, row, "exit")
            instanceof StreamExecNode);
  }
}
