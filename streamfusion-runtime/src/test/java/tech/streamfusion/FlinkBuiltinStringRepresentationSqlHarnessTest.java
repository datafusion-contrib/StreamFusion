package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.runtime.functions.table.BuiltInTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkBuiltinStringRepresentationSqlHarnessTest {
  private static final BuiltInFunctionDefinition DEF =
      BuiltInFunctionDefinition.newBuilder()
          .name("COMPARE_TEXT")
          .kind(FunctionKind.TABLE)
          .inputTypeStrategy(
              InputTypeStrategies.sequence(
                  InputTypeStrategies.logical(LogicalTypeFamily.CHARACTER_STRING),
                  InputTypeStrategies.logical(LogicalTypeFamily.CHARACTER_STRING)))
          .outputTypeStrategy(TypeStrategies.argument(0))
          .runtimeClass(Compare.class.getName())
          .build();

  private static final String FROM =
      " FROM src s, LATERAL TABLE(COMPARE_TEXT(s.first_text, s.second_text)) t(v)";

  @Test
  void supplementaryAndBmpStringsRetainFlinkJavaOrdering() {
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      var comparison =
          NativeFailureParity.run(() -> environment(metrics), "SELECT s.id, t.v" + FROM);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      // UTF-16 orders U+E000 above U+1F642. Their UTF-8 byte order is reversed.
      assertEquals(List.of(List.of("+I", 0, "1")), comparison.nativeRun().rows());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @Test
  void stringConsumersOutsideTheFinalRowBoundaryStayOnFlink() {
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics), "SELECT t.v, COUNT(*)" + FROM + " GROUP BY t.v");
      comparison.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          comparison.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("StringData requires a final row output")),
          comparison::toString);
    }
  }

  private static StreamTableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        env.fromElements(Row.of(0, "\uE000", "🙂"))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "first_text", "second_text"},
                    Types.INT,
                    Types.STRING,
                    Types.STRING)));
    table.loadModule(
        "compare_text",
        new Module() {
          @Override
          public Set<String> listFunctions() {
            return Set.of(DEF.getName());
          }

          @Override
          public Optional<FunctionDefinition> getFunctionDefinition(String name) {
            return DEF.getName().equalsIgnoreCase(name) ? Optional.of(DEF) : Optional.empty();
          }
        });
    table.useModules("compare_text", "core");
    return table;
  }

  public static class Compare extends BuiltInTableFunction<StringData> {
    public Compare(SpecializedContext context) {
      super(DEF, context);
    }

    @Override
    public DataType getOutputDataType() {
      return getArgumentDataTypes().get(0);
    }

    public void eval(StringData first, StringData second) {
      collect(StringData.fromString(Integer.toString(Integer.signum(first.compareTo(second)))));
    }
  }
}
