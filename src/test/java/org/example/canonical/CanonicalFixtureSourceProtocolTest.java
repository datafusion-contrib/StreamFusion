package org.example.canonical;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalFixtureSourceProtocolTest {
  @TempDir Path directory;

  @Test
  void registrationAndRequestFinishEachReaderOnce() {
    var calls = new EnumeratorCalls();
    var enumerator = new CanonicalFixtureFactory.RowsEnumerator(calls.context, 0);
    enumerator.addReader(0);
    enumerator.handleSplitRequest(0, null);
    enumerator.addReader(1);
    enumerator.handleSplitRequest(1, null);
    assertEquals(List.of(0), calls.owners);
    assertEquals(0, calls.assigned.get(0).offset);
    assertEquals(List.of(0, 1), calls.finished);
    assertEquals(-1, enumerator.snapshotState(1));
  }

  @Test
  void restoredEnumeratorWaitsForReaderRequest() {
    var calls = new EnumeratorCalls();
    var enumerator = new CanonicalFixtureFactory.RowsEnumerator(calls.context, -1);
    enumerator.addReader(0);
    assertTrue(calls.finished.isEmpty());
    assertTrue(calls.assigned.isEmpty());
    enumerator.handleSplitRequest(0, null);
    assertEquals(List.of(0), calls.finished);
    assertTrue(calls.assigned.isEmpty());
    assertEquals(-1, enumerator.snapshotState(1));
  }

  @Test
  void freshReaderWaitsForNoMoreSplitsAfterLastRecord() throws Exception {
    var calls = new ReaderCalls();
    var reader = reader(calls.context, List.of("INSERT,1,7"), Integer.MAX_VALUE);
    var output = new OutputCalls();
    reader.start();
    reader.addSplits(List.of(new CanonicalFixtureFactory.OffsetSplit(0)));
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output.output));
    assertEquals(1, reader.snapshotState(1).get(0).offset);
    var waiting = reader.isAvailable();
    assertFalse(waiting.isDone());
    reader.notifyNoMoreSplits();
    assertTrue(waiting.isDone());
    assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(output.output));
    assertEquals(1, calls.requests);
    assertEquals(1, output.rows.size());
    assertEquals(1, output.rows.get(0).getLong(0));
    assertEquals(7, output.rows.get(0).getLong(1));
  }

  @Test
  void restoredExhaustedSplitWaitsForNoMoreSplits() throws Exception {
    var reader = reader(new ReaderCalls().context, List.of("INSERT,1,7"), Integer.MAX_VALUE);
    var output = new OutputCalls();
    reader.addSplits(List.of(new CanonicalFixtureFactory.OffsetSplit(1)));
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output.output));
    var waiting = reader.isAvailable();
    assertFalse(waiting.isDone());
    reader.notifyNoMoreSplits();
    assertTrue(waiting.isDone());
    assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(output.output));
    assertEquals(1, reader.snapshotState(2).get(0).offset);
    assertTrue(output.rows.isEmpty());
  }

  @Test
  void completedEnumerationAllowsFinalRecordToFinish() throws Exception {
    var reader =
        reader(new ReaderCalls().context, List.of("INSERT,1,7", "INSERT,2,9"), Integer.MAX_VALUE);
    var output = new OutputCalls();
    reader.addSplits(List.of(new CanonicalFixtureFactory.OffsetSplit(0)));
    reader.notifyNoMoreSplits();
    assertEquals(InputStatus.MORE_AVAILABLE, reader.pollNext(output.output));
    assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(output.output));
    assertTrue(reader.isAvailable().isDone());
    assertEquals(2, reader.snapshotState(2).get(0).offset);
    assertEquals(2, output.rows.size());
    assertEquals(1, output.rows.get(0).getLong(0));
    assertEquals(2, output.rows.get(1).getLong(0));
  }

  @Test
  void readerWithoutAssignedSplitWaitsThenFinishes() throws Exception {
    var reader = reader(new ReaderCalls().context, List.of("INSERT,1,7"), Integer.MAX_VALUE);
    var output = new OutputCalls();
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output.output));
    var waiting = reader.isAvailable();
    assertFalse(waiting.isDone());
    reader.notifyNoMoreSplits();
    assertTrue(waiting.isDone());
    assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(output.output));
    assertTrue(reader.snapshotState(1).isEmpty());
    assertTrue(output.rows.isEmpty());
  }

  @Test
  void savepointCutStaysOpenAfterEnumerationCompletes() throws Exception {
    var reader = reader(new ReaderCalls().context, List.of("INSERT,1,7", "INSERT,2,9"), 1);
    var output = new OutputCalls();
    reader.addSplits(List.of(new CanonicalFixtureFactory.OffsetSplit(0)));
    reader.notifyNoMoreSplits();
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output.output));
    assertEquals("1", Files.readString(directory.resolve("ready")));
    assertEquals(1, reader.snapshotState(1).get(0).offset);
    assertFalse(reader.isAvailable().isDone());
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output.output));
    assertEquals(1, output.rows.size());
  }

  @Test
  void idleReaderAtSavepointCutStaysOpen() throws Exception {
    var reader = reader(new ReaderCalls().context, List.of("INSERT,1,7", "INSERT,2,9"), 1);
    var output = new OutputCalls();
    reader.notifyNoMoreSplits();
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output.output));
    assertFalse(reader.isAvailable().isDone());
    assertTrue(reader.snapshotState(1).isEmpty());
    assertTrue(output.rows.isEmpty());
  }

  private CanonicalFixtureFactory.RowsReader reader(
      SourceReaderContext context, List<String> rows, int limit) {
    return new CanonicalFixtureFactory.RowsReader(
        context,
        rows,
        directory.resolve("ready").toString(),
        limit,
        RowType.of(new BigIntType(), new BigIntType()),
        false);
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<?> type, InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
  }

  private static final class EnumeratorCalls {
    final List<Integer> owners = new ArrayList<>();
    final List<CanonicalFixtureFactory.OffsetSplit> assigned = new ArrayList<>();
    final List<Integer> finished = new ArrayList<>();
    final SplitEnumeratorContext<CanonicalFixtureFactory.OffsetSplit> context =
        proxy(
            SplitEnumeratorContext.class,
            (instance, method, args) -> {
              if (method.getName().equals("assignSplit")) {
                assigned.add((CanonicalFixtureFactory.OffsetSplit) args[0]);
                owners.add((Integer) args[1]);
              } else if (method.getName().equals("signalNoMoreSplits")) {
                finished.add((Integer) args[0]);
              } else throw new AssertionError("Unexpected enumerator callback: " + method);
              return null;
            });
  }

  private static final class ReaderCalls {
    int requests;
    final SourceReaderContext context =
        proxy(
            SourceReaderContext.class,
            (instance, method, args) -> {
              if (!method.getName().equals("sendSplitRequest"))
                throw new AssertionError("Unexpected reader callback: " + method);
              requests++;
              return null;
            });
  }

  private static final class OutputCalls {
    final List<RowData> rows = new ArrayList<>();
    final ReaderOutput<RowData> output =
        proxy(
            ReaderOutput.class,
            (instance, method, args) -> {
              if (!method.getName().equals("collect"))
                throw new AssertionError("Unexpected output callback: " + method);
              rows.add((RowData) args[0]);
              return null;
            });
  }
}
