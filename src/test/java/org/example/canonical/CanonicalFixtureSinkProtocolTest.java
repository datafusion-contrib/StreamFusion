package org.example.canonical;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalFixtureSinkProtocolTest {
  private static final RowType TYPE = RowType.of(new BigIntType());
  @TempDir Path directory;

  @Test
  void independentWritersRetainDuplicateRowsAndPreparedReplayIsIdempotent() throws Exception {
    var first = new WriterState(0, 0, 0, List.of(row(7)));
    var second = new WriterState(0, 1, 0, List.of(row(7)));
    var committer = new RowsCommitter(directory.toString());
    committer.commit(List.of(request(first), request(second)));
    committer.commit(List.of(request(first)));
    assertEquals(List.of("+I|7", "+I|7"), CanonicalFixtureFactory.committedRows(directory, TYPE));
    assertThrows(
        IOException.class,
        () -> committer.commit(List.of(request(new WriterState(0, 0, 0, List.of(row(8)))))));
    assertEquals(List.of("+I|7", "+I|7"), CanonicalFixtureFactory.committedRows(directory, TYPE));
  }

  @Test
  void restoredAndNewWritersUseNewCheckpointNamespaces() throws Exception {
    try (var original =
        new RowsWriter(TYPE, directory.resolve("original").toString(), List.of())
            .withIdentity(-1, 0)) {
      original.write(GenericRowData.of(7L), null);
      var prepared = original.prepareCommit().iterator().next();
      var saved = original.snapshotState(3);
      try (var restored =
              new RowsWriter(TYPE, directory.resolve("restored").toString(), saved)
                  .withIdentity(3, 0);
          var added =
              new RowsWriter(TYPE, directory.resolve("added").toString(), List.of())
                  .withIdentity(3, 1)) {
        restored.write(GenericRowData.of(8L), null);
        added.write(GenericRowData.of(9L), null);
        var resumed = restored.prepareCommit().iterator().next();
        var fresh = added.prepareCommit().iterator().next();
        assertEquals(0, prepared.generation);
        assertEquals(4, resumed.generation);
        assertEquals(4, fresh.generation);
        assertEquals(0, resumed.writer);
        assertEquals(1, fresh.writer);
        var serializer = new RowsSerializer();
        var replay =
            serializer.deserialize(serializer.getVersion(), serializer.serialize(prepared));
        assertEquals(prepared.generation, replay.generation);
        assertEquals(prepared.writer, replay.writer);
        assertEquals(prepared.transaction, replay.transaction);
        var output = directory.resolve("committed");
        var committer = new RowsCommitter(output.toString());
        committer.commit(List.of(request(prepared), request(resumed), request(fresh)));
        committer.commit(List.of(request(replay)));
        assertEquals(
            List.of("+I|7", "+I|8", "+I|9"), CanonicalFixtureFactory.committedRows(output, TYPE));
      }
    }
  }

  @Test
  void earlierSingleWriterStateStillDecodesAndInvalidOwnersFail() throws Exception {
    var bytes = new ByteArrayOutputStream();
    try (var output = new DataOutputStream(bytes)) {
      output.writeLong(7);
      output.writeInt(1);
      byte[] row = row(42);
      output.writeInt(row.length);
      output.write(row);
    }
    var serializer = new RowsSerializer();
    var old = serializer.deserialize(1, bytes.toByteArray());
    assertEquals(0, old.generation);
    assertEquals(0, old.writer);
    assertEquals(7, old.transaction);
    assertArrayEquals(row(42), old.rows.get(0));
    Files.write(directory.resolve("transaction-00000000000000000007"), bytes.toByteArray());
    new RowsCommitter(directory.toString()).commit(List.of(request(old)));
    assertEquals(List.of("+I|42"), CanonicalFixtureFactory.committedRows(directory, TYPE));
    byte[] invalid = serializer.serialize(old);
    ByteBuffer.wrap(invalid).putInt(Long.BYTES * 2, -1);
    assertThrows(IOException.class, () -> serializer.deserialize(serializer.getVersion(), invalid));
  }

  private static byte[] row(long value) throws IOException {
    var output = new DataOutputSerializer(32);
    new RowDataSerializer(TYPE).serialize(GenericRowData.of(value), output);
    return output.getCopyOfBuffer();
  }

  private static Committer.CommitRequest<WriterState> request(WriterState state) {
    return new Committer.CommitRequest<>() {
      @Override
      public WriterState getCommittable() {
        return state;
      }

      @Override
      public int getNumberOfRetries() {
        return 0;
      }

      @Override
      public void signalFailedWithKnownReason(Throwable reason) {
        fail(reason);
      }

      @Override
      public void signalFailedWithUnknownReason(Throwable reason) {
        fail(reason);
      }

      @Override
      public void retryLater() {
        fail("unexpected commit retry");
      }

      @Override
      public void updateAndRetryLater(WriterState updated) {
        fail("unexpected commit replacement");
      }

      @Override
      public void signalAlreadyCommitted() {}
    };
  }
}
