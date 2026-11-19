package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class JsonRuntimeAdmissionTest {
  @Test
  void unavailableJacksonDeclinesEveryJsonReaderBeforeNativeCompilation() throws Exception {
    ClassLoader loader = new MissingJacksonLoader();
    var runtime = loader.loadClass("tech.streamfusion.operator.NativeJsonRuntime");
    assertEquals(false, runtime.getMethod("available").invoke(null));
    assertEquals(false, runtime.getMethod("available").invoke(null));
    var encoder = loader.loadClass("tech.streamfusion.planner.RexExpression");
    var encode = encoder.getDeclaredMethod("encodeProjections", List.class, List.class);
    encode.setAccessible(true);
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var string = types.createSqlType(SqlTypeName.VARCHAR);
    for (String name :
        List.of(
            "JSON_VALUE",
            "JSON_EXISTS",
            "IS JSON VALUE",
            "IS JSON OBJECT",
            "IS JSON ARRAY",
            "IS JSON SCALAR",
            "IS NOT JSON VALUE")) {
      var function =
          new SqlFunction(
              name, SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.SYSTEM);
      List<org.apache.calcite.rex.RexNode> arguments =
          name.startsWith("IS ")
              ? List.of(rex.makeInputRef(string, 0))
              : List.of(rex.makeInputRef(string, 0), rex.makeLiteral("$.a"));
      var output = name.equals("JSON_VALUE") ? string : types.createSqlType(SqlTypeName.BOOLEAN);
      var call = rex.makeCall(output, function, arguments);
      assertNull(encode.invoke(null, List.of(call), List.of("json")), name);
    }
  }

  /** Isolate the admission classes while simulating a missing shaded Jackson dependency. */
  private static final class MissingJacksonLoader extends ClassLoader {
    MissingJacksonLoader() {
      super(JsonRuntimeAdmissionTest.class.getClassLoader());
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      if (name.startsWith("org.apache.flink.shaded.jackson2.")) {
        throw new ClassNotFoundException(name);
      }
      if (!name.startsWith("tech.streamfusion.planner.")
          && !name.startsWith("tech.streamfusion.operator.NativeJsonRuntime")) {
        return super.loadClass(name, resolve);
      }
      Class<?> loaded = findLoadedClass(name);
      if (loaded == null) {
        try (var input = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
          if (input == null) {
            throw new ClassNotFoundException(name);
          }
          byte[] bytes = input.readAllBytes();
          loaded = defineClass(name, bytes, 0, bytes.length);
        } catch (IOException e) {
          throw new ClassNotFoundException(name, e);
        }
      }
      if (resolve) {
        resolveClass(loaded);
      }
      return loaded;
    }
  }
}
