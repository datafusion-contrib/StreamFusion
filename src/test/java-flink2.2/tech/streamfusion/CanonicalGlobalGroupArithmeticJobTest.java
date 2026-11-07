package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGlobalGroupArithmeticJobTest extends CanonicalGroupArithmeticJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.globalGroupJobSmoke")
                    || test.get()[0].equals(
                            System.getProperty(
                                "streamfusion.test.globalGroupJobSmokeMode", "continue"))
                        && !(boolean) test.get()[1]
                        && (boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"));
  }

  @Override
  @ParameterizedTest
  @MethodSource("cases")
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void generatedArithmeticStateContinuesAcrossIsolatedEngines(
      String mode, boolean ordinary, boolean nativeFirst, String backend) throws Exception {
    super.generatedArithmeticStateContinuesAcrossIsolatedEngines(
        mode, ordinary, nativeFirst, backend);
    var switched = directory.resolve("switched");
    assertTrue(Files.exists(directory.resolve("baseline/baseline-global-singleton-verified")));
    var stages = new java.util.ArrayList<>(List.of("prefix", "middle", "suffix"));
    if (mode.equals("no-input")) stages.addAll(List.of("no-input", "return"));
    if (mode.equals("checkpoint")) stages.add("canonical");
    for (String stage : stages)
      assertTrue(Files.exists(switched.resolve(stage + "-global-singleton-verified")), stage);
  }

  boolean appendOnly() {
    return false;
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    if (appendOnly()) CanonicalGroupArithmeticAppendJobTest.writeAppendInput(root, mode);
    Files.createFile(root.resolve("global-aggregate"));
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(0 AS BIGINT),SUM(v),AVG(v) FROM input_table");
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    return Map.of("0", mode.equals("rescale") ? "2112|5" : appendOnly() ? "11|0" : "22|7");
  }
}
