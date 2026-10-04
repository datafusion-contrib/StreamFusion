package tech.streamfusion.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ParquetBenchmarkInputTest {
  @Test
  void refusesCallbacksAfterTheHostInputHasClosed() throws Exception {
    for (boolean filesystem : new boolean[] {false, true}) {
      var input = new ParquetBenchmarkInput(new byte[] {0, 1, 2}, filesystem);
      input.close();
      assertThrows(IOException.class, () -> input.readFully(0, ByteBuffer.allocateDirect(1)));
      assertEquals(0, input.reads());
      assertEquals(0, input.bytesRead());
    }
  }

  @Test
  void fillsOnlyTheRequestedDirectBufferRangeInBothModes() throws Exception {
    for (boolean filesystem : new boolean[] {false, true}) {
      try (var input = new ParquetBenchmarkInput(new byte[] {0, 1, 2, 3, 4}, filesystem)) {
        var target = ByteBuffer.allocateDirect(8);
        target.position(2);
        target.limit(5);
        input.readFully(1, target);
        assertEquals(5, target.position());
        assertEquals(0, target.get(1));
        assertEquals(1, target.get(2));
        assertEquals(2, target.get(3));
        assertEquals(3, target.get(4));
        assertEquals(1, input.reads());
        assertEquals(3, input.bytesRead());
      }
    }
  }

  @Test
  void rejectsOutOfRangeReadsWithoutAdvancingTheBufferOrCounters() throws Exception {
    for (boolean filesystem : new boolean[] {false, true}) {
      try (var input = new ParquetBenchmarkInput(new byte[] {0, 1, 2}, filesystem)) {
        var target = ByteBuffer.allocateDirect(2);
        assertThrows(EOFException.class, () -> input.readFully(-1, target));
        assertThrows(EOFException.class, () -> input.readFully(2, target));
        assertThrows(EOFException.class, () -> input.readFully(Long.MAX_VALUE, target));
        assertEquals(0, target.position());
        assertEquals(0, input.reads());
        assertEquals(0, input.bytesRead());
      }
    }
  }
}
