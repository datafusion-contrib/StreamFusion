package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

class PlannerModeBoundaryTest {
  private static final String PLANNER = "tech/streamfusion/planner/";

  @Test
  void compiledBatchAndCommonLayersDoNotDependOnStreamingNodes() throws Exception {
    Map<String, byte[]> classes = classes();
    assertTrue(
        classes.keySet().stream().filter(name -> name.startsWith(PLANNER + "batch/")).count() > 25);
    assertTrue(
        classes.keySet().stream().filter(name -> name.startsWith(PLANNER + "common/")).count()
            > 10);
    assertTrue(
        classes.keySet().stream().filter(name -> name.startsWith(PLANNER + "stream/")).count()
            > 130);
    for (var entry : classes.entrySet()) {
      String name = entry.getKey();
      if (!name.startsWith(PLANNER + "batch/") && !name.startsWith(PLANNER + "common/")) continue;
      // Class-file constants include dependencies in method bodies as well as public signatures.
      String constants = new String(entry.getValue(), StandardCharsets.ISO_8859_1);
      for (String forbidden :
          List.of(
              "org/apache/flink/table/planner/plan/nodes/exec/stream/",
              PLANNER + "stream/",
              PLANNER + "StreamPhysical",
              PLANNER + "NativeStreamPlanner",
              PLANNER + "PhysicalPlanScan",
              PLANNER + "PlanContext",
              PLANNER + "NativeCalcExecNode",
              PLANNER + "NativeUnionExecNode",
              PLANNER + "NativeShareExecNode",
              PLANNER + "RowDataToArrowExecNode",
              PLANNER + "ArrowToRowDataExecNode")) {
        assertFalse(constants.contains(forbidden), name + " depends on " + forbidden);
      }
      if (name.startsWith(PLANNER + "common/"))
        assertFalse(constants.contains(PLANNER + "batch/"), name + " depends on batch planning");
    }
  }

  @Test
  void sharedExpressionServicesDoNotDependOnModeImplementation() throws Exception {
    Map<String, byte[]> classes = classes();
    for (String service :
        List.of(
            "RexExpression",
            "ExpressionPlanning",
            "CollectionExpressionSemantics",
            "JvmFunctionConversions",
            "JvmFunctionTypes",
            "JsonStringIdentity")) {
      var entries =
          classes.entrySet().stream()
              .filter(
                  entry ->
                      entry.getKey().equals(PLANNER + service + ".class")
                          || entry.getKey().startsWith(PLANNER + service + "$"))
              .collect(java.util.stream.Collectors.toList());
      assertFalse(entries.isEmpty(), service);
      for (var entry : entries) {
        String constants = new String(entry.getValue(), StandardCharsets.ISO_8859_1);
        assertFalse(constants.contains(PLANNER + "stream/"), entry.getKey());
        assertFalse(constants.contains(PLANNER + "batch/"), entry.getKey());
      }
    }
  }

  @Test
  void publicStreamingInstallationRetainsItsFacadeAndOneDeclaringScan() throws Exception {
    assertTrue(
        tech.streamfusion.planner.stream.StreamPlanScan.class.isAssignableFrom(
            PhysicalPlanScan.class));
    assertTrue(
        PhysicalPlanScan.class.getMethod("explainSummary").getDeclaringClass()
            == tech.streamfusion.planner.stream.StreamPlanScan.class);
  }

  private static Map<String, byte[]> classes() throws Exception {
    Path location =
        Path.of(NativePlanner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    Map<String, byte[]> classes = new LinkedHashMap<>();
    if (Files.isDirectory(location)) {
      try (var paths = Files.walk(location.resolve(PLANNER))) {
        for (Path path :
            (Iterable<Path>) paths.filter(path -> path.toString().endsWith(".class"))::iterator)
          classes.put(
              location.relativize(path).toString().replace('\\', '/'), Files.readAllBytes(path));
      }
    } else {
      readJar(location, classes);
    }
    return classes;
  }

  private static void readJar(Path location, Map<String, byte[]> classes) throws IOException {
    try (var jar = new JarFile(location.toFile())) {
      for (var entry :
          (Iterable<java.util.jar.JarEntry>)
              jar.stream()
                      .filter(
                          entry ->
                              entry.getName().startsWith(PLANNER)
                                  && entry.getName().endsWith(".class"))
                  ::iterator) {
        try (var input = jar.getInputStream(entry)) {
          classes.put(entry.getName(), input.readAllBytes());
        }
      }
    }
  }
}
