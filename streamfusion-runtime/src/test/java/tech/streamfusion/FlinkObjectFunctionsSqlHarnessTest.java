package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Objects;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkObjectFunctionsSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"object", "update", "nested", "array", "field", "null"})
  void computedObjectsMatchReleasedFlink(String shape) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("OBJECT_OF");
    if (shape.equals("update") || shape.equals("null"))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("OBJECT_UPDATE");
    String object = "OBJECT_OF('" + Item.class.getName() + "','a',n,'b',s)";
    String projection;
    switch (shape) {
      case "update":
        projection = "OBJECT_UPDATE(" + object + ",'a',n + 1,'b',CONCAT(s,'!'))";
        break;
      case "nested":
        projection = "ROW(" + object + ",n)";
        break;
      case "array":
        projection = "ARRAY[" + object + "]";
        break;
      case "field":
        projection = "(" + object + ").a";
        break;
      case "null":
        projection =
            "OBJECT_UPDATE(CAST(NULL AS STRUCTURED<'"
                + Item.class.getName()
                + "',a INT,b STRING>),'b',s)";
        break;
      default:
        projection = object;
    }
    var runs =
        NativeFailureParity.run(
            FlinkObjectFunctionsSqlHarnessTest::environment,
            "SELECT id," + projection + " FROM src");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  private static TableEnvironment environment() {
    var table =
        BuiltinFunctionParity.environment(
            ROW(FIELD("id", INT()), FIELD("n", INT()), FIELD("s", STRING())),
            List.of(
                Row.of(0, 42, "é中"),
                Row.of(1, null, null),
                Row.of(2, -1, ""),
                Row.of(3, Integer.MAX_VALUE, "\u0000")));
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "true");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }

  public static class Item {
    public Integer a;
    public String b;

    @Override
    public boolean equals(Object other) {
      return other instanceof Item
          && Objects.equals(a, ((Item) other).a)
          && Objects.equals(b, ((Item) other).b);
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b);
    }

    @Override
    public String toString() {
      return "Item(" + a + "," + b + ")";
    }
  }
}
