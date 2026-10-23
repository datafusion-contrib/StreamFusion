package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CompletableFuture;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.Test;

class CommonSinkSourceReadinessTest {
  @Test
  void splitHolderWaitsForTheCoordinatorAndThenPreservesTheSinkFailure() throws Exception {
    var reader = reader();
    reader.getClass().getField("hasSplit").set(reader, true);
    var failure = new IllegalStateException("NOT NULL sink sentinel");
    reader.getClass().getField("failure").set(reader, failure);
    var available = reader.isAvailable();
    assertFalse(available.isDone());
    assertSame(available, reader.isAvailable());
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext());
    assertEquals(0, reader.getClass().getField("emitted").get(reader));
    reader.notifyNoMoreSplits();
    assertTrue(available.isDone());
    assertSame(failure, assertThrows(IllegalStateException.class, reader::pollNext));
    assertEquals(1, reader.getClass().getField("emitted").get(reader));
    reader.close();
  }

  @Test
  void emptyReadersAlsoWaitBeforeEndOfInput() throws Exception {
    var reader = reader();
    assertFalse(reader.isAvailable().isDone());
    assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext());
    reader.notifyNoMoreSplits();
    assertTrue(reader.isAvailable().isDone());
    assertEquals(InputStatus.END_OF_INPUT, reader.pollNext());
    reader.close();
  }

  @Test
  void closingAnUnnotifiedReaderReleasesItsPendingAvailability() throws Exception {
    var first = reader();
    var second = reader();
    var available = first.isAvailable();
    var other = second.isAvailable();
    first.notifyNoMoreSplits();
    assertTrue(available.isDone());
    assertFalse(other.isDone());
    second.close();
    assertTrue(other.isDone());
    assertTrue(second.closed);
    first.close();
  }

  private static BaseReader reader() throws Exception {
    return (BaseReader)
        CommonSinkSourceReadiness.instrument(new ByteBuddy().redefine(FixtureReader.class))
            .make()
            .load(FixtureReader.class.getClassLoader(), ClassLoadingStrategy.Default.CHILD_FIRST)
            .getLoaded()
            .getConstructor()
            .newInstance();
  }

  public static class BaseReader {
    public boolean closed;

    public CompletableFuture<Void> isAvailable() {
      return CompletableFuture.completedFuture(null);
    }

    public InputStatus pollNext() {
      throw new UnsupportedOperationException();
    }

    public void notifyNoMoreSplits() {}

    public void close() {
      closed = true;
    }
  }

  public static class FixtureReader extends BaseReader {
    private boolean noMoreSplits;
    public boolean hasSplit;
    public int emitted;
    public RuntimeException failure;

    @Override
    public void notifyNoMoreSplits() {
      noMoreSplits = true;
    }

    @Override
    public InputStatus pollNext() {
      if (hasSplit) {
        emitted++;
        if (failure != null) throw failure;
        return InputStatus.END_OF_INPUT;
      }
      return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }
  }
}
