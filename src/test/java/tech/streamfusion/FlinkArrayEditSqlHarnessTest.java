package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkArrayEditSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void appendPreservesNullsResolvedTypesAndFilteredBatches(String type) throws Exception {
    verify(type, "ARRAY_APPEND(a,n)", "ARRAY_APPEND");
    verify(type, "ARRAY_APPEND(a,CAST(NULL AS " + sqlType(type) + "))", "ARRAY_APPEND");
  }

  static void verify(String type, String expression, String function) throws Exception {
    if (!function.equals("ARRAY"))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql = "SELECT id," + expression + " FROM src";
    var table = environment(type);
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("native=[") && execution.contains(function)
            && execution.contains("jvm=[]")), scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(() -> environment(type), sql);
    BuiltinFunctionParity.assertParity(() -> environment(type), sql + " WHERE MOD(id,3)=0");
    BuiltinFunctionParity.assertParity(() -> environment(type), sql + " WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void prependPreservesNullsAndPlacesTheElementFirst(String type) throws Exception {
    verify(type, "ARRAY_PREPEND(a,n)", "ARRAY_PREPEND");
    verify(type, "ARRAY_PREPEND(a,CAST(NULL AS " + sqlType(type) + "))", "ARRAY_PREPEND");
  }

  static String sqlType(String type) {
    return type.equals("DECIMAL") ? "DECIMAL(38,9)" : type;
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void removeDropsAllMatchesAndCanRemoveNullElements(String type) throws Exception {
    verify(type,"ARRAY_REMOVE(a,n)","ARRAY_REMOVE");
    verify(type,"ARRAY_REMOVE(a,CAST(NULL AS " + sqlType(type) + "))","ARRAY_REMOVE");
  }

  static TableEnvironment environment(String type) {
    DataType element;
    switch (type) {
      case "BOOLEAN": element = BOOLEAN(); break;
      case "TINYINT": element = TINYINT(); break;
      case "SMALLINT": element = SMALLINT(); break;
      case "INT": element = INT(); break;
      case "DECIMAL": element = DECIMAL(38,9); break;
      default: element = BIGINT(); break;
    }
    List<Row> rows = new ArrayList<>();
    int[] widths = {0,1,2,8,31,32,33,64,65};
    for (int row = 0; row < 2053; row++) {
      Object[] values = null;
      if (row % 7 != 0) {
        values = (Object[]) java.lang.reflect.Array.newInstance(
            value(type,0).getClass(), widths[row % widths.length]);
        for (int i = 0; i < values.length; i++)
          if (i % 5 != 0) values[i] = value(type, (row+i)%9-4);
      }
      Object[] other = null;
      if (row%5 != 0) {
        other = (Object[]) java.lang.reflect.Array.newInstance(value(type,0).getClass(),row%4);
        for (int i=0;i<other.length;i++) if (i%2 != 0) other[i]=value(type,(row+i)%9-4);
      }
      rows.add(Row.of(row, values, row%3 == 0 ? null : value(type, row%9-4),other));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("a", ARRAY(element)), FIELD("n", element),
            FIELD("b",ARRAY(element))), rows);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void concatIsNullStrictAndPreservesVariadicArgumentOrder(String type) throws Exception {
    verify(type,"ARRAY_CONCAT(a,b)","ARRAY_CONCAT");
    verify(type,"ARRAY_CONCAT(a,b,a)","ARRAY_CONCAT");
  }

  private static Object value(String type, int value) {
    switch (type) {
      case "BOOLEAN": return value%2 == 0;
      case "TINYINT": return (byte) value;
      case "SMALLINT": return (short) value;
      case "INT": return value;
      case "DECIMAL": return new BigDecimal("9007199254740993.000000001").add(BigDecimal.valueOf(value));
      default: return (long) value;
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void intersectionKeepsLeftOrderRegardlessOfWhichInputIsShorter(String type) throws Exception {
    verify(type,"ARRAY_INTERSECT(a,b)","ARRAY_INTERSECT");
    verify(type,"ARRAY_INTERSECT(b,a)","ARRAY_INTERSECT");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void exceptDeduplicatesAndKeepsLeftInputOrder(String type) throws Exception {
    verify(type,"ARRAY_EXCEPT(a,b)","ARRAY_EXCEPT");
    verify(type,"ARRAY_EXCEPT(b,a)","ARRAY_EXCEPT");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void unionPreservesFirstOccurrenceAcrossBothInputs(String type) throws Exception {
    verify(type,"ARRAY_UNION(a,b)","ARRAY_UNION");
    verify(type,"ARRAY_UNION(b,a)","ARRAY_UNION");
  }
}
