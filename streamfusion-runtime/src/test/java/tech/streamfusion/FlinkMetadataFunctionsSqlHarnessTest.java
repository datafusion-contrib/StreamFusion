package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMetadataFunctionsSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void metadataConsumersPreserveJavaUnicodeOrdering(boolean database) throws Exception {
    String expression =
        database ? "CURRENT_DATABASE() < '\ue000'" : "TYPEOF(r) < 'ROW<`\ue000` INT>'";
    NativeParity.assertFallbackReasonContains(
        () -> unicodeEnvironment("😀", database),
        "SELECT " + expression + " FROM default_catalog.default_database.src",
        "Flink string ordering depends on its Java or serialized representation");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void metadataPreservesUnpairedIdentifierCharacters(boolean database) throws Exception {
    for (boolean collections : new boolean[] {false, true}) {
      NativeParity.assertFallbackReasonContains(
          () -> {
            var table = unicodeEnvironment("\ud800", database);
            table
                .getConfig()
                .set(
                    "streamfusion.experimental.collectionExpressions",
                    Boolean.toString(collections));
            table
                .getConfig()
                .set(
                    "streamfusion.experimental.metadataExpressions",
                    Boolean.toString(!collections));
            return table;
          },
          "SELECT "
              + (database ? "CURRENT_DATABASE()" : "TYPEOF(r)")
              + (collections ? ", ARRAY[r]" : "")
              + " FROM default_catalog.default_database.src",
          "metadata identifiers containing unpaired UTF-16");
    }
  }

  private static TableEnvironment unicodeEnvironment(String name, boolean database) {
    var table =
        BuiltinFunctionParity.environment(
            ROW(FIELD("r", ROW(FIELD(database ? "value" : name, INT())))),
            List.of(Row.of(Row.of(1))));
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
    if (database) {
      table.executeSql("CREATE DATABASE `" + name + "`");
      table.useDatabase(name);
    }
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "TYPEOF(id)",
        "TYPEOF(id, TRUE)",
        "TYPEOF(s)",
        "TYPEOF(s, FALSE)",
        "TYPEOF(a)",
        "TYPEOF(a, TRUE)",
        "TYPEOF(d)",
        "TYPEOF(ts, TRUE)",
        "TYPEOF(CAST(NULL AS STRING))",
        "TYPEOF(id + 1)",
        "TYPEOF(a[1])",
        "TYPEOF(CARDINALITY(a))",
        "TYPEOF(CASE WHEN id = 0 THEN a ELSE NULL END)",
        "CURRENT_DATABASE()",
        "CONCAT(CURRENT_DATABASE(), ':', TYPEOF(a))"
      })
  void resolvedTypesAndQueryDatabaseMatchTheReleasedHost(String expression) throws Exception {
    BuiltinFunctionParity.assertParity(
        FlinkMetadataFunctionsSqlHarnessTest::environment,
        "SELECT id, " + expression + " FROM src");
  }

  @Test
  void databaseIsCapturedForEachQueryIncludingQuotedNames() throws Exception {
    for (String name : List.of("default_database", "db-é'quoted")) {
      BuiltinFunctionParity.assertParity(
          () -> {
            var table = environment();
            if (!name.equals("default_database")) {
              table.executeSql("CREATE DATABASE `" + name + "`");
              table.useDatabase(name);
            }
            return table;
          },
          "SELECT id, CURRENT_DATABASE() FROM default_catalog.default_database.src WHERE id >= 0");
    }
  }

  @Test
  void metadataDoesNotSuppressErrorsInItsArgument() {
    var runs =
        NativeFailureParity.run(
            FlinkMetadataFunctionsSqlHarnessTest::environment,
            "SELECT TYPEOF(CAST(s AS INT)) FROM src");
    assertNotNull(runs.host().failure(), runs.host().toString());
    runs.assertFailure(
        runs.host().rootCause().getClass(), runs.host().phase(), NativeFailureParity.Route.NATIVE);
  }

  @Test
  void typeInspectionStillInvokesItsUserFunctionArgument() {
    var runs =
        NativeFailureParity.run(
            () -> {
              var table = environment();
              table.createTemporarySystemFunction("observed", Observed.class);
              return table;
            },
            "SELECT TYPEOF(observed(id)) FROM src");
    runs.assertFailure(
        IllegalStateException.class,
        "observed:1",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TYPEOF(id)", "CURRENT_DATABASE()"})
  void metadataExtensionCanBeDisabled(String expression) throws Exception {
    NativeParity.assertFallback(
        () -> {
          var table = environment();
          table.getConfig().set("streamfusion.experimental.metadataExpressions", "false");
          table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
          return table;
        },
        "SELECT " + expression + " FROM src");
  }

  public static class Observed extends ScalarFunction {
    public Integer eval(Integer id) {
      if (id == 1) throw new IllegalStateException("observed:1");
      return id;
    }
  }

  @Test
  void registeredFunctionsKeepTheirImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> {
          var table = environment();
          table.createTemporarySystemFunction("TYPEOF", CustomTypeOf.class);
          return table;
        },
        "SELECT TYPEOF(id) FROM src");
  }

  @Test
  void typeOnlyReferencesDoNotMaterializeUnusedValues() throws Exception {
    String sql = "SELECT TYPEOF(a), TYPEOF(d), TYPEOF(ts) FROM src";
    String plan = tech.streamfusion.planner.NativePlanner.explain(environment(), sql);
    assertTrue(plan.contains("RowDataToArrow(fields=[0]"), plan);
    BuiltinFunctionParity.assertParity(FlinkMetadataFunctionsSqlHarnessTest::environment, sql);
  }

  @Test
  void fusedHostCallsKeepTheirActualInputReferences() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("PRINTF");
    BuiltinFunctionParity.assertParity(
        FlinkMetadataFunctionsSqlHarnessTest::environment,
        "SELECT TYPEOF(s), PRINTF('%s', TYPEOF(s)) FROM src");
  }

  public static class CustomTypeOf extends ScalarFunction {
    public String eval(Integer value) {
      return "custom:" + value;
    }
  }

  private static TableEnvironment environment() {
    var table =
        BuiltinFunctionParity.environment(
            ROW(
                FIELD("id", INT().notNull()),
                FIELD("s", STRING()),
                FIELD("a", ARRAY(ROW(FIELD("x", INT()), FIELD("y", STRING().notNull())))),
                FIELD("d", DECIMAL(38, 9)),
                FIELD("ts", TIMESTAMP_LTZ(9))),
            List.of(
                Row.of(
                    0,
                    "bad",
                    new Row[] {Row.of(1, "é")},
                    new java.math.BigDecimal("1.234567890"),
                    java.time.Instant.EPOCH),
                Row.of(1, null, null, null, null)));
    return table;
  }
}
