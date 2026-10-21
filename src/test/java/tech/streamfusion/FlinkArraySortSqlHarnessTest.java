package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArraySortSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void shortArraysRetainOrderNullPlacementAndDecimalScale(String type) throws Exception {
    for (String options : List.of("",",FALSE",",FALSE,TRUE",",TRUE,FALSE"))
      FlinkArrayEditSqlHarnessTest.verify(type,
          "ARRAY_SORT(ARRAY_SLICE(a,1,31)" + options + ")","ARRAY_SORT");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT","SMALLINT","INT","BIGINT"})
  void distinctArraysCanUseTheReleasedSortAtAnyLength(String type) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(type,"ARRAY_SORT(ARRAY_DISTINCT(a))","ARRAY_SORT");
  }

  @Test
  void unrestrictedLongArraysRetainTheReleasedComparatorFailure() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    Integer[] values = {3,1,3,4,4,-3,2,0,0,-2,2,1,-1,-4,1,-3,1,3,-4,-2,-4,2,-4,1,
        -1,-3,0,1,0,1,3,3,1,-4,3,2,3,1,1,0,2,-1,2,-2,2,-1,1,-1,0,-2,1,1,2,-1,
        -3,-2,1,-2,-3,-1,4,1,4,-2};
    var runs = NativeFailureParity.run(() -> BuiltinFunctionParity.environment(
        ROW(FIELD("a",ARRAY(INT()))),List.of(Row.of((Object)values))),
        "SELECT ARRAY_SORT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(IllegalArgumentException.class,"Comparison method violates its general contract!",
        runs.host().phase(),NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nullableRuntimeOptionsKeepFlinkEvaluation() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    BuiltinFunctionParity.assertParity(() -> FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ARRAY_SORT(ARRAY_SLICE(a,1,8),n>0,n<0) FROM src");
  }
}
