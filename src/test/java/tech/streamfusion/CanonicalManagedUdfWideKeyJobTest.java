package tech.streamfusion;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

abstract class CanonicalManagedUdfWideKeyJobTest extends CanonicalManagedUdfGroupJobTest {
  private String keyShape;
  private int aggregateMode;

  static Stream<Arguments> cases() {
    return Stream.of(
            "DECIMAL19",
            "DECIMAL38",
            "TIMESTAMP9",
            "LOCAL_TIMESTAMP9",
            "COMPOSITE",
            "NESTED_ROW",
            "DECIMAL_ARRAY",
            "BINARY")
        .flatMap(
            key ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        phase ->
                            Stream.of(
                                Arguments.of(
                                    key + "/" + phase + "/continue", false, false, "hashmap"),
                                Arguments.of(
                                    key + "/" + phase + "/continue", true, true, "hashmap"),
                                Arguments.of(
                                    key + "/" + phase + "/continue", false, true, "rocksdb"),
                                Arguments.of(
                                    key + "/" + phase + "/continue", true, false, "rocksdb"),
                                Arguments.of(
                                    key + "/" + phase + "/no-input",
                                    phase % 2 == 0,
                                    phase % 2 != 0,
                                    "rocksdb"),
                                Arguments.of(
                                    key + "/" + phase + "/checkpoint",
                                    phase % 2 != 0,
                                    true,
                                    "hashmap"),
                                Arguments.of(
                                    key + "/" + phase + "/rescale",
                                    phase % 2 == 0,
                                    phase % 2 != 0,
                                    "rocksdb"))))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.wideKeys.job.nested-smoke")
                    || ((String) args.get()[0]).startsWith("NESTED_ROW/")
                        && ((String) args.get()[0]).endsWith("/continue")
                        && !(boolean) args.get()[1]
                        && !(boolean) args.get()[2]
                        && args.get()[3].equals("hashmap"))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.wideKeys.job.smoke")
                    || !(boolean) args.get()[1]
                        && args.get()[3].equals("hashmap")
                        && Set.of("DECIMAL38/2/continue", "TIMESTAMP9/0/continue")
                            .contains(args.get()[0]))
        .filter(
            args ->
                !Boolean.getBoolean("sf.canonical.wideKeys.job.uid-smoke")
                    || (boolean) args.get()[1]
                        && args.get()[3].equals("hashmap")
                        && Set.of("DECIMAL19/1/continue", "DECIMAL38/2/continue")
                            .contains(args.get()[0])
                    || Boolean.getBoolean("sf.canonical.wideKeys.job.uid-smoke")
                        && Set.of("DECIMAL38/1/rescale", "TIMESTAMP9/2/rescale")
                            .contains(args.get()[0]));
  }

  @Override
  @ParameterizedTest(name = "{0},ordinary={1},nativeFirst={2},backend={3}")
  @MethodSource("cases")
  @Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
  void generatedArithmeticStateContinuesAcrossIsolatedEngines(
      String scenario, boolean ordinary, boolean first, String backend) throws Exception {
    var parts = scenario.split("/");
    keyShape = parts[0];
    aggregateMode = Integer.parseInt(parts[1]);
    super.generatedArithmeticStateContinuesAcrossIsolatedEngines(
        parts[2], ordinary, first, backend);
  }

  @Override
  int cacheEntries() {
    return 0;
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    if (aggregateMode > 0) {
      Files.writeString(
          root.resolve(aggregateMode == 2 ? "two-phase-mini-batch" : "one-phase-mini-batch"), "5");
      Files.writeString(root.resolve("mini-batch-latency"), "1 h");
      Files.writeString(
          root.resolve("require-checkpoint-flush-families"),
          aggregateMode == 2
              ? "NativeColumnarLocalJvmGroup,NativeColumnarJvmGroup"
              : "NativeColumnarJvmGroup");
    }
    if (aggregateMode == 2) {
      Files.createFile(root.resolve("merge-history-aggregate"));
      Files.writeString(root.resolve("required-plan-node"), "stream-exec-global-group-aggregate");
      Files.writeString(
          root.resolve("native-operator"),
          "NativeColumnarLocalJvmGroup,NativeColumnarJvmGroup,RowDataToArrow,ArrowToRowData");
      Files.writeString(
          root.resolve("native-input-families"),
          "NativeColumnarLocalJvmGroup,NativeColumnarJvmGroup");
    }
    Files.createFile(root.resolve("same-cut-stock-reference"));
    Files.writeString(root.resolve("uid-generation"), "ALWAYS");
    String timestamp =
        "CAST(CONCAT('1969-12-31 23:59:59.000000',CAST(k AS STRING),'1') AS TIMESTAMP(9))";
    String expression;
    switch (keyShape) {
      case "DECIMAL19":
        expression = "CAST(k AS DECIMAL(19,2))";
        break;
      case "DECIMAL38":
        expression = "CAST(k AS DECIMAL(38,2))";
        break;
      case "TIMESTAMP9":
        expression = timestamp;
        break;
      case "LOCAL_TIMESTAMP9":
        expression = "CAST(" + timestamp + " AS TIMESTAMP_LTZ(9))";
        break;
      case "COMPOSITE":
        expression = "CAST(k AS DECIMAL(38,2)) AS g0," + timestamp + " AS g1,CAST(k AS STRING)";
        break;
      case "NESTED_ROW":
        expression = "ROW(CAST(k AS DECIMAL(38,2))," + timestamp + ")";
        break;
      case "DECIMAL_ARRAY":
        expression = "ARRAY[CAST(NULL AS DECIMAL(38,2)),CAST(k AS DECIMAL(38,2))]";
        break;
      case "BINARY":
        expression = "ENCODE(CAST(k AS STRING),'UTF-8')";
        break;
      default:
        throw new AssertionError(keyShape);
    }
    String keys = keyShape.equals("COMPOSITE") ? "g0,g1,g" : "g";
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT MAX(original_key),fixture_history(v),COUNT(*)"
            + " FROM (SELECT k AS original_key,v,"
            + expression
            + " AS g FROM input_table) GROUP BY "
            + keys);
    return root;
  }
}
