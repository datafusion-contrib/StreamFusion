package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.junit.jupiter.api.Test;

/** Captures the released host's physical state contract independently of native admission. */
class BatchAggregateContractTest {
  @Test
  void integerAccumulatorLayoutsRemainStableAcrossPhasesAndImplementations() throws Exception {
    for (String disabled : List.of("", "HashAgg")) {
      for (String phase : List.of("ONE_PHASE", "TWO_PHASE")) {
        Configuration config = new Configuration();
        config.setString("table.optimizer.agg-phase-strategy", phase);
        config.setString("table.exec.disabled-operators", disabled);
        var table = BatchSortSqlTest.environment(false, 3, config);
        List<BatchPhysicalGroupAggregateBase> seen = new ArrayList<>();
        var program = FlinkBatchProgram.buildProgram(table.getConfig());
        program.addLast(
            "batch_aggregate_contract",
            (root, context) -> {
              inspect(root, seen);
              return root;
            });
        table
            .getConfig()
            .setPlannerConfig(
                CalciteConfig$.MODULE$
                    .createBuilder(CalciteConfig$.MODULE$.DEFAULT())
                    .replaceBatchProgram(program)
                    .build());
        table.explainSql(
            "SELECT SUM(CAST(id AS TINYINT)), SUM(CAST(id AS SMALLINT)), SUM(CAST(id AS INT)),"
                + " SUM(id), COUNT(*), COUNT(id), MIN(CAST(id AS INT)), MAX(id), AVG(id) FROM"
                + " input");
        assertEquals(phase.equals("ONE_PHASE") ? 1 : 2, seen.size());
        for (var aggregate : seen) {
          assertEquals(0, aggregate.grouping().length);
          assertEquals(0, aggregate.auxGrouping().length);
          assertEquals(
              disabled.isEmpty() ? "Hash" : "Sort",
              aggregate.getRelTypeName().contains("Hash") ? "Hash" : "Sort");
          assertEquals(9, aggregate.getAggCallList().size());
          if (aggregate.isFinal()) {
            assertEquals(phase.equals("TWO_PHASE"), aggregate.isMerge());
            assertEquals(
                List.of(
                    SqlTypeName.TINYINT,
                    SqlTypeName.SMALLINT,
                    SqlTypeName.INTEGER,
                    SqlTypeName.BIGINT,
                    SqlTypeName.BIGINT,
                    SqlTypeName.BIGINT,
                    SqlTypeName.INTEGER,
                    SqlTypeName.BIGINT,
                    SqlTypeName.BIGINT),
                aggregate.getRowType().getFieldList().stream()
                    .map(field -> field.getType().getSqlTypeName())
                    .collect(java.util.stream.Collectors.toList()));
          } else {
            assertFalse(aggregate.isMerge());
            assertEquals(10, aggregate.getRowType().getFieldCount());
            var output = FlinkTypeFactory$.MODULE$.toLogicalRowType(aggregate.getRowType());
            int field = 0;
            var calls = aggregate.getAggCallToAggFunction();
            for (int index = 0; index < calls.size(); index++) {
              assertInstanceOf(DeclarativeAggregateFunction.class, calls.apply(index)._2());
              var function = (DeclarativeAggregateFunction) calls.apply(index)._2();
              for (var buffer : function.getAggBufferTypes())
                assertEquals(
                    buffer.getLogicalType().copy(true), output.getTypeAt(field++).copy(true));
            }
            assertEquals(output.getFieldCount(), field);
          }
        }
      }
    }
  }

  private static void inspect(RelNode node, List<BatchPhysicalGroupAggregateBase> seen) {
    if (node instanceof BatchPhysicalGroupAggregateBase)
      seen.add((BatchPhysicalGroupAggregateBase) node);
    node.getInputs().forEach(input -> inspect(input, seen));
  }
}
