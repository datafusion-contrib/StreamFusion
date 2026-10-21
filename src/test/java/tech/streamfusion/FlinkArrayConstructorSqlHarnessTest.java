package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.time.*;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkArrayConstructorSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void matchingNullableBasicValuesConstructArraysInRust(String type) throws Exception {
    if (!tech.streamfusion.compat.FlinkCompat.NARROW_CONSTRUCTORS_COMPILE
        && java.util.Set.of("TINYINT","SMALLINT").contains(type)) {
      for(String expression:List.of("ARRAY[n,n]","ARRAY[n,CAST(NULL AS "+type+")]")) {
        var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment(type),"SELECT "+expression+" FROM src");
        assertNotNull(runs.host().failure());
        runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.FALLBACK);
      }
      return;
    }
    FlinkArrayEditSqlHarnessTest.verify(type,"ARRAY[n,n]","ARRAY");
    FlinkArrayEditSqlHarnessTest.verify(type,"ARRAY[n,CAST(NULL AS "+FlinkArrayEditSqlHarnessTest.sqlType(type)+")]","ARRAY");
  }

  @ParameterizedTest
  @ValueSource(strings={"DATE","TIME","TIMESTAMP3","TIMESTAMP9","LTZ3","LTZ9"})
  void temporalStorageAndPrecisionMatchReleasedArrayWriters(String type) throws Exception {
    var table=temporal(type);var scan=NativePlanner.install(table);
    table.explainSql("SELECT ARRAY[n,n] FROM src");
    assertTrue(scan.functionExecutions().stream().anyMatch(x->x.contains("ARRAY") && x.contains("jvm=[]")),scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(()->temporal(type),"SELECT ARRAY[n,n] FROM src");
    BuiltinFunctionParity.assertParity(()->temporal(type),"SELECT ARRAY[n,n] FROM src WHERE n IS NULL");
  }

  @Test
  void failingElementsStayBehindTheFilter() {
    NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ARRAY[n,1/(id-1)] FROM src WHERE id<0").assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment temporal(String kind) {
    DataType type;Object value;
    switch(kind) {
      case "DATE":type=DATE();value=LocalDate.of(1969,12,31);break;
      case "TIME":type=TIME(3);value=LocalTime.of(23,59,59,999000000);break;
      case "TIMESTAMP3":type=TIMESTAMP(3);value=LocalDateTime.of(1969,12,31,23,59,59,999000000);break;
      case "TIMESTAMP9":type=TIMESTAMP(9);value=LocalDateTime.of(1969,12,31,23,59,59,999999999);break;
      case "LTZ3":type=TIMESTAMP_LTZ(3);value=Instant.ofEpochSecond(-1,999000000);break;
      default:type=TIMESTAMP_LTZ(9);value=Instant.ofEpochSecond(-1,999999999);break;
    }
    return BuiltinFunctionParity.environment(ROW(FIELD("n",type)),List.of(Row.of(value),Row.of((Object)null)));
  }
}
