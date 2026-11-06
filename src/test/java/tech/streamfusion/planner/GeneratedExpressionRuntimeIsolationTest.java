package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.NativeCalcOperator;

class GeneratedExpressionRuntimeIsolationTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"scalar", "cast", "row", "clock", "time", "sql", "containers"})
  void workersRestoreAndExecuteWithoutPlannerOrCalcite(String mode) throws Exception {
    Path input = directory.resolve("function.bin");
    try (var output = new ObjectOutputStream(Files.newOutputStream(input))) {
      output.writeObject(fixture(mode));
    }
    String classpath =
        Arrays.stream(
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"))
                    .split(File.pathSeparator))
            .filter(
                entry -> {
                  String name = Path.of(entry).getFileName().toString();
                  return !name.contains("flink-table-planner") && !name.contains("calcite");
                })
            .collect(Collectors.joining(File.pathSeparator));
    var command = new ArrayList<String>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-Xmx768m");
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens=") || argument.startsWith("-Darrow."))
        command.add(argument);
    }
    command.add("-Dstreamfusion.native.development=true");
    command.add("-Djava.library.path=" + System.getProperty("java.library.path"));
    command.addAll(
        List.of(
            "-cp",
            classpath,
            GeneratedExpressionRuntimeProbe.class.getName(),
            mode,
            input.toString()));
    Path log = directory.resolve("worker.log");
    Process worker =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(worker.waitFor(60, TimeUnit.SECONDS), "isolated worker timed out");
      String result = Files.readString(log);
      assertEquals(0, worker.exitValue(), result);
      assertTrue(result.contains("ISOLATED_RUNTIME_OK " + mode), result);
    } finally {
      if (worker.isAlive()) {
        worker.destroyForcibly();
        worker.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  private Object fixture(String mode) throws Exception {
    if (mode.equals("sql")) return sqlFactory();
    if (mode.equals("containers")) {
      var ints =
          org.apache.flink.table.api.DataTypes.ARRAY(
                  org.apache.flink.table.api.DataTypes.INT().notNull())
              .bridgedTo(int[].class);
      var rows =
          org.apache.flink.table.api.DataTypes.ARRAY(
                  org.apache.flink.table.api.DataTypes.ROW(
                      org.apache.flink.table.api.DataTypes.FIELD(
                          "n", org.apache.flink.table.api.DataTypes.STRING())))
              .bridgedTo(java.util.List.class);
      return new ExternalContainerConverter(
          org.apache.flink.table.api.DataTypes.ROW(
              org.apache.flink.table.api.DataTypes.FIELD(
                  "m", org.apache.flink.table.api.DataTypes.MAP(ints, rows))));
    }
    if (mode.equals("clock")) return GeneratedProcessingTimeTest.scalar(3);
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var integer = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true);
    var input = rex.makeInputRef(integer, 0);
    if (mode.equals("scalar")) {
      return FlinkExpressionGenerator.generate(
          input,
          new LogicalType[] {new IntType()},
          TableConfig.getDefault(),
          getClass().getClassLoader());
    }
    if (mode.equals("cast")) {
      var string = types.createTypeWithNullability(types.createSqlType(SqlTypeName.VARCHAR), true);
      return FlinkExpressionGenerator.generate(
          rex.makeCast(integer, rex.makeInputRef(string, 0)),
          new LogicalType[] {new VarCharType()},
          TableConfig.getDefault(),
          getClass().getClassLoader());
    }
    if (mode.equals("time")) {
      var string = types.createTypeWithNullability(types.createSqlType(SqlTypeName.VARCHAR), true);
      var time = types.createFieldTypeFromLogicalType(new TimeType());
      assertEquals(0, ((TimeType) FlinkTypeFactory.toLogicalType(time)).getPrecision());
      var cast =
          rex.makeCall(time, FlinkSqlOperatorTable.TRY_CAST, List.of(rex.makeInputRef(string, 0)));
      return FlinkExpressionGenerator.generate(
          cast,
          new LogicalType[] {new VarCharType()},
          TableConfig.getDefault(),
          getClass().getClassLoader());
    }
    var projection =
        rex.makeCall(
            integer,
            SqlStdOperatorTable.DIVIDE,
            List.of(rex.makeExactLiteral(BigDecimal.valueOf(100), integer), input));
    var condition =
        rex.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            input,
            rex.makeExactLiteral(BigDecimal.ZERO, integer));
    return FlinkExpressionGenerator.generate(
        List.of(projection),
        condition,
        new LogicalType[] {new IntType()},
        RowType.of(new IntType()),
        TableConfig.getDefault(),
        getClass().getClassLoader());
  }

  private Configuration sqlFactory() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    NativePlanner.install(tables);
    tables.createTemporarySystemFunction("tag", GeneratedExpressionRuntimeProbe.Tag.class);
    tables.executeSql("CREATE TABLE input_rows (v STRING) WITH ('connector'='datagen')");
    tables.executeSql(
        "CREATE TABLE output_rows (tagged STRING, n BIGINT) " + "WITH ('connector'='blackhole')");
    tables
        .createStatementSet()
        .addInsertSql(
            "INSERT INTO output_rows SELECT tag(CAST(v AS BIGINT)), CAST(v AS BIGINT) "
                + "FROM input_rows WHERE REGEXP(v, '[0-9]') AND CAST(v AS BIGINT) >= 0")
        .attachAsDataStream();
    var factories =
        env.getStreamGraph().getStreamNodes().stream()
            .map(node -> node.getOperatorFactory())
            .filter(
                factory ->
                    factory instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) factory).getOperator()
                            instanceof NativeCalcOperator)
            .collect(Collectors.toList());
    assertEquals(1, factories.size(), "the regression must exercise the production native Calc");
    var config = new StreamConfig(new Configuration());
    config.setStreamOperatorFactory(factories.get(0));
    config.serializeAllConfigs();
    return config.getConfiguration();
  }
}
