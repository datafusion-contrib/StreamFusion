package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CanonicalTemporalJoinCommitFailureJobTest {
  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  Path directory;

  @ParameterizedTest
  @CsvSource({
    "before,true,hashmap", "after,true,hashmap",
    "before,true,rocksdb", "after,true,rocksdb",
    "before,false,hashmap", "after,false,hashmap",
    "before,false,rocksdb", "after,false,rocksdb"
  })
  @Timeout(value = 8, unit = TimeUnit.MINUTES)
  void temporalOutputCommitsRecoverBeforeSwitchingEngines(
      String position, boolean nativeFirst, String backend) throws Exception {
    var runner = new CanonicalJobRunner(directory);
    Path fixture = runner.fixtureJar();
    Path reference = prepare("baseline");
    Path switched = prepare("switched");
    runner.run(fixture, false, reference, Integer.MAX_VALUE, "none", "baseline", 3, "hashmap");
    Files.createFile(switched.resolve("checkpoint-native-checkpoint"));
    Files.createFile(switched.resolve("checkpoint-expect-commit-failure"));
    Files.createFile(switched.resolve("fail-" + position + "-commit"));
    runner.run(
        fixture,
        nativeFirst,
        switched,
        4,
        "none",
        "checkpoint",
        3,
        nativeFirst ? backend : "hashmap");
    assertTrue(Files.exists(switched.resolve("checkpoint-commit-failure-verified")));
    assertTrue(Files.exists(switched.resolve("checkpoint-native-witness-sink-output-ready")));
    assertFalse(Files.exists(switched.resolve("fail-" + position + "-commit")));
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var initial = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    assertEquals(position.equals("before") ? 0 : 2, initial.size(), initial.toString());
    String pointer = Files.readString(switched.resolve("checkpoint-savepoint")).trim();
    var location = AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(pointer);
    try (var input = new java.io.DataInputStream(location.getMetadataHandle().openInputStream())) {
      var metadata =
          Checkpoints.loadCheckpointMetadata(input, getClass().getClassLoader(), pointer);
      assertFalse(metadata.getCheckpointProperties().isSavepoint());
    }
    runner.run(
        fixture,
        nativeFirst,
        switched,
        8,
        "checkpoint",
        "prefix",
        3,
        nativeFirst ? backend : "hashmap");
    runner.run(
        fixture,
        !nativeFirst,
        switched,
        11,
        "prefix",
        "middle",
        3,
        nativeFirst ? "hashmap" : backend);
    runner.run(
        fixture,
        nativeFirst,
        switched,
        Integer.MAX_VALUE,
        "middle",
        "suffix",
        3,
        nativeFirst ? backend : "hashmap");
    var actual = CanonicalFixtureFactory.committedRows(switched.resolve("committed"), type);
    var expected = CanonicalFixtureFactory.committedRows(reference.resolve("committed"), type);
    actual.sort(String::compareTo);
    expected.sort(String::compareTo);
    assertEquals(CanonicalTemporalJoinJobTest.expectedRows(switched, "INNER", false), actual);
    assertEquals(expected, actual);
    for (String stage : List.of("prefix", "middle")) {
      var savepoint =
          Path.of(
              java.net.URI.create(Files.readString(switched.resolve(stage + "-savepoint")).trim()));
      assertFalse(
          new String(
                  Files.readAllBytes(savepoint.resolve("_metadata")),
                  java.nio.charset.StandardCharsets.ISO_8859_1)
              .contains("tech.streamfusion"));
    }
    assertTrue(
        Files.exists(
            switched.resolve((nativeFirst ? "middle" : "prefix") + "-stock-classpath-verified")));
    assertFalse(Files.exists(switched.resolve("plan.json")));
  }

  private Path prepare(String name) throws Exception {
    var fixture = new CanonicalTemporalJoinJobTest();
    fixture.directory = directory;
    Path root = fixture.prepare(name, "INNER", false);
    Files.createFile(root.resolve("ordinary-sql"));
    Files.createFile(root.resolve("eager-watermarks"));
    Files.createFile(root.resolve("wait-sink-output"));
    for (String schema : List.of("input-columns.sql", "right_input_table-columns.sql")) {
      Path path = root.resolve(schema);
      Files.writeString(
          path, Files.readString(path).replace("INTERVAL '1' DAY", "INTERVAL '0.001' SECOND"));
    }
    return root;
  }
}
