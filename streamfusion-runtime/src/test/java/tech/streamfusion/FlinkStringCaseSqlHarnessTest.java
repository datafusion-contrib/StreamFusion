package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Default case conversion uses released Flink expressions inside columnar Calc, retaining both
 * Java-backed and binary-only string behavior under the task JVM's locale.
 */
class FlinkStringCaseSqlHarnessTest {

  @Test
  void lowerAndUpperMatchHost() throws Exception {
    NativeParity.assertParity(
        environment(),
        "SELECT s, LOWER(s) AS lo, UPPER(s) AS up, LOWER(s) = 'apple' AS is_apple FROM t");
  }

  @ParameterizedTest
  @ValueSource(strings = {"tr-TR", "az-AZ", "lt-LT"})
  void caseConversionRetainsTheRuntimeLocaleAndItsConsumers(String language) throws Exception {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag(language));
      NativeParity.assertParity(
          () -> TextTimeFunctionTestInputs.textRows("I", "i", "I\u0301", "\u0130", "", null),
          "SELECT id, LOWER(s), UPPER(s), LOWER(s) = 'i', UPPER(s) = 'I', "
              + "REPLACE(LOWER(s), 'i', 'x') FROM inputs");
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void binarySourceDoesNotAcquireAJavaCacheDuringTheUpcall(@TempDir Path directory) throws Exception {
    Files.writeString(directory.resolve("input.csv"), "I\ni\n");
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      NativeParity.assertParity(
          () -> {
            TableEnvironment tables = environment().get();
            tables.executeSql(
                "CREATE TEMPORARY TABLE cold (s STRING) WITH ('connector' = 'filesystem', "
                    + "'path' = '" + directory.toUri() + "', 'format' = 'csv')");
            return tables;
          },
          "SELECT REPLACE(s, 'X', 'Y'), LOWER(s), UPPER(s), LOWER(s) = 'i' FROM cold");
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void caseConversionAfterANonMergeableProjectionRetainsSourceRepresentation() throws Exception {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      NativeParity.assertParity(
          () -> TextTimeFunctionTestInputs.textRows("I", "i", "", null),
          "SELECT LOWER(s), UPPER(s), r >= 0 AND r <= 1 "
              + "FROM (SELECT s, RAND() AS r FROM inputs)");
    } finally {
      Locale.setDefault(previous);
    }
  }

  private static Supplier<TableEnvironment> environment() {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      DataStream<Row> t =
          fromData(
              env,
              Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
              Row.of("Apple"),
              Row.of("GOOGLE"),
              Row.of("baiDu"),
              Row.of("Grüße"),
              Row.of("MAÑANA"),
              Row.of("straße"));
      tEnv.createTemporaryView("t", t, Schema.newBuilder().column("s", DataTypes.STRING()).build());
      return tEnv;
    };
  }
}
