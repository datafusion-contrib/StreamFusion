package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.junit.jupiter.api.Test;

class OverWindowSpecTest {
  private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  private RelDataType input(boolean processing) {
    return types.builder().add("key", SqlTypeName.BIGINT).add("value", SqlTypeName.BIGINT)
        .add("time", processing ? types.createProctimeIndicatorType(true)
            : types.createRowtimeIndicatorType(true, false)).build();
  }

  private OverSpec compiled(boolean rows, RexWindowBound lower, int[] keys, List<RexLiteral> constants) {
    return new OverSpec(new PartitionSpec(keys), List.of(new OverSpec.GroupSpec(
        SortSpec.builder().addField(2, true, false).build(), rows, lower,
        RexWindowBounds.CURRENT_ROW, List.of())), constants, 3);
  }

  private Window.Group physical(boolean rows, RexWindowBound lower, int[] keys) {
    return new Window.Group(ImmutableBitSet.of(keys), rows, lower, RexWindowBounds.CURRENT_ROW,
        RelCollations.of(new RelFieldCollation(2)), List.of());
  }

  @Test
  void ordinaryAndCompiledAdaptersAgreeAcrossEveryTimeFrameAndPartitionShape() {
    for (boolean processing : new boolean[] {false, true})
      for (boolean rows : new boolean[] {false, true})
        for (boolean bounded : new boolean[] {false, true})
          for (int[] keys : List.of(new int[0], new int[] {0}, new int[] {0, 1})) {
            RelDataType input = input(processing);
            List<RexLiteral> constants = List.of(rex.makeBigintLiteral(BigDecimal.valueOf(7)));
            RexWindowBound lower = bounded
                ? RexWindowBounds.preceding(rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 3))
                : RexWindowBounds.UNBOUNDED_PRECEDING;
            var ordinary = OverWindowSpec.physical(physical(rows, lower, keys), constants, input, false);
            var portable = OverWindowSpec.compiled(compiled(rows, lower, keys, constants),
                FlinkTypeFactory.toLogicalRowType(input));
            assertEquals(ordinary.status(), portable.status());
            if (processing && bounded && !rows) {
              assertFalse(ordinary.isAccepted());
              continue;
            }
            assertTrue(ordinary.isAccepted(), ordinary.reason());
            var left = ordinary.value();
            var right = portable.value();
            assertEquals(left.frameKind(), right.frameKind());
            assertEquals(bounded ? 7 : 0, left.offset());
            assertEquals(left.offset(), right.offset());
            assertEquals(processing, left.processingTime());
            assertEquals(processing, right.processingTime());
            assertEquals(2, left.timeColumn());
            assertArrayEquals(keys, right.keyColumns());
            if (keys.length > 0) {
              right.keyColumns()[0] = 99;
              assertArrayEquals(keys, right.keyColumns());
            }
          }
  }

  @Test
  void canonicalKeyAndRowCountRestrictionsRemainSeparateFromFrameSemantics() {
    var input = FlinkTypeFactory.toLogicalRowType(input(false));
    var multiple = compiled(true, RexWindowBounds.UNBOUNDED_PRECEDING, new int[] {0, 1}, List.of());
    assertTrue(OverWindowSpec.compiled(multiple, input).isAccepted());
    assertTrue(StockOverPlan.prepare(multiple, input).reason().contains("one BIGINT"));
    var lower = RexWindowBounds.preceding(rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 3));
    var maximum = compiled(true, lower, new int[] {0},
        List.of(rex.makeBigintLiteral(BigDecimal.valueOf(Long.MAX_VALUE))));
    assertTrue(OverWindowSpec.compiled(maximum, input).isAccepted());
    assertTrue(StockOverPlan.prepare(maximum, input).reason().contains("overflows"));
  }

  @Test
  void releasedCurrentRowAndRankingQualificationsArePreserved() {
    RelDataType input = input(false);
    var current = compiled(true, RexWindowBounds.CURRENT_ROW, new int[] {0}, List.of());
    var stock = StockOverPlan.prepare(current, FlinkTypeFactory.toLogicalRowType(input));
    assertTrue(stock.isAccepted(), stock.reason());
    assertEquals(0, stock.value().window().offset());
    assertFalse(OverWindowSpec.physical(physical(true, RexWindowBounds.CURRENT_ROW, new int[] {0}),
        List.of(), input, false).isAccepted());
    var ranking = OverWindowSpec.physical(
        physical(true, RexWindowBounds.UNBOUNDED_PRECEDING, new int[] {0}), List.of(), input, true);
    assertTrue(ranking.isAccepted());
    assertEquals(0, ranking.value().frameKind());
    assertFalse(OverWindowSpec.physical(physical(true, RexWindowBounds.CURRENT_ROW, new int[] {0}),
        List.of(), input, true).isAccepted());
  }
}
