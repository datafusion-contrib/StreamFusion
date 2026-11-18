package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.Test;

/**
 * Old window state must fail at the envelope, before a native decoder can reinterpret its clock.
 */
class WindowBoundaryStateVersionTest {
  @Test
  void previousCanonicalWindowVersionsAreRejected() throws Exception {
    for (String decoder : new String[] {"decodeHeader", "decodeAsyncHeader"}) {
      var method =
          CanonicalNativeState.class.getDeclaredMethod(decoder, byte[].class, String.class);
      method.setAccessible(true);
      for (int version : new int[] {3, 4}) {
        byte[] header = ByteBuffer.allocate(32).putInt(0x53464353).putInt(version).array();
        InvocationTargetException failure =
            assertThrows(
                InvocationTargetException.class, () -> method.invoke(null, header, "window rank"));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertTrue(failure.getCause().getMessage().contains("state version " + version));
      }
    }
  }

  @Test
  void previousRocksWindowVersionIsRejected() {
    byte[] bytes = ByteBuffer.allocate(8).putInt(0x5346524b).putInt(3).array();
    IOException failure =
        assertThrows(
            IOException.class,
            () ->
                RocksDBNativeSnapshotStrategy.readMetaDocument(
                    new ByteStreamStateHandle("old", bytes)));
    assertTrue(failure.getMessage().contains("metadata version 3"));
  }
}
