package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.junit.jupiter.api.Test;

class PaimonPartitionPredicateTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(types);
  private final org.apache.paimon.types.RowType table = org.apache.paimon.types.RowType.of(
      new org.apache.paimon.types.DataType[] {
          new org.apache.paimon.types.VarCharType(), new org.apache.paimon.types.VarCharType()},
      new String[] {"payload", "pt"});
  private final RowType projected = RowType.of(
      new org.apache.flink.table.types.logical.LogicalType[] {new VarCharType()}, new String[] {"pt"});

  private RexNode field(int index) {
    return rex.makeInputRef(types.createSqlType(SqlTypeName.VARCHAR), index);
  }

  private RexNode value(String value) {
    return rex.makeLiteral(value, types.createSqlType(SqlTypeName.VARCHAR), true);
  }

  private Predicate hint(RexNode expression) {
    return PaimonPartitionPredicate.of(expression, projected, table, List.of("pt"));
  }

  @Test
  void projectedIndexIsMappedToOriginalTableAndOnlyMatchingPartitionSurvives() {
    Predicate predicate = hint(rex.makeCall(SqlStdOperatorTable.EQUALS, field(0), value("p00")));
    assertEquals(1, ((LeafPredicate) predicate).index());
    assertEquals(predicate, hint(rex.makeCall(SqlStdOperatorTable.EQUALS, field(0), rex.makeLiteral("p00"))));
    assertTrue(predicate.test(GenericRow.of(BinaryString.fromString("x"), BinaryString.fromString("p00"))));
    assertFalse(predicate.test(GenericRow.of(BinaryString.fromString("x"), BinaryString.fromString("p01"))));
    assertFalse(predicate.test(GenericRow.of(BinaryString.fromString("x"), null)));
    assertEquals(predicate, hint(rex.makeCall(SqlStdOperatorTable.EQUALS, value("p00"), field(0))));
  }

  @Test
  void unsupportedAlternativesCannotPruneAwayResidualMatches() {
    RexNode eq = rex.makeCall(SqlStdOperatorTable.EQUALS, field(0), value("p00"));
    RexNode other = rex.makeCall(SqlStdOperatorTable.EQUALS, field(0), value("p01"));
    assertNull(hint(rex.makeCall(SqlStdOperatorTable.OR, eq, other)));
    assertNull(hint(rex.makeCall(SqlStdOperatorTable.IS_NULL, field(0))));
    assertNull(hint(rex.makeCall(SqlStdOperatorTable.EQUALS, field(0), rex.makeNullLiteral(field(0).getType()))));
    assertNull(hint(rex.makeCall(SqlStdOperatorTable.GREATER_THAN, field(0), value("p00"))));
    RowType nonPartition = RowType.of(
        new org.apache.flink.table.types.logical.LogicalType[] {new VarCharType()}, new String[] {"payload"});
    assertNull(PaimonPartitionPredicate.of(eq, nonPartition, table, List.of("pt")));
    assertEquals(hint(eq), hint(rex.makeCall(SqlStdOperatorTable.AND, eq,
        rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, field(0)))));
  }
}
