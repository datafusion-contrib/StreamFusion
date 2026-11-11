package org.example.canonical;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalKafkaCommitBoundaryTest {
  @TempDir Path directory;

  @Test
  void checkpointCompletionWaitsForTheExternalTransaction() throws Exception {
    var type = RowType.of(new BigIntType());
    Path committed = directory.resolve("committed");
    var executor = Executors.newSingleThreadExecutor();
    try {
      var completion =
          executor.submit(
              () -> {
                CanonicalKafkaJobWorker.awaitCommittedRows(
                    committed, type, 1, Duration.ofSeconds(10));
                return null;
              });
      assertThrows(
          java.util.concurrent.TimeoutException.class,
          () -> completion.get(100, TimeUnit.MILLISECONDS));
      var encoded = new DataOutputSerializer(16);
      new RowDataSerializer(type).serialize(GenericRowData.of(42L), encoded);
      byte[] bytes =
          new CanonicalFixtureFactory.RowsSerializer()
              .serialize(
                  new CanonicalFixtureFactory.WriterState(0, List.of(encoded.getCopyOfBuffer())));
      Files.createDirectories(committed);
      Path pending = Files.write(committed.resolve("pending.tmp"), bytes);
      assertThrows(
          java.util.concurrent.TimeoutException.class,
          () -> completion.get(100, TimeUnit.MILLISECONDS));
      Files.move(pending, committed.resolve("transaction-00000000000000000000"));
      completion.get(10, TimeUnit.SECONDS);
      assertEquals(List.of("+I|42"), CanonicalFixtureFactory.committedRows(committed, type));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void duplicateExternalRowsFailTheBoundary() throws Exception {
    var type = RowType.of(new BigIntType());
    var encoded = new DataOutputSerializer(16);
    new RowDataSerializer(type).serialize(GenericRowData.of(42L), encoded);
    byte[] bytes =
        new CanonicalFixtureFactory.RowsSerializer()
            .serialize(
                new CanonicalFixtureFactory.WriterState(
                    0, List.of(encoded.getCopyOfBuffer(), encoded.getCopyOfBuffer())));
    Files.write(directory.resolve("transaction-00000000000000000000"), bytes);
    assertThrows(
        AssertionError.class,
        () ->
            CanonicalKafkaJobWorker.awaitCommittedRows(directory, type, 1, Duration.ofSeconds(1)));
  }
}
