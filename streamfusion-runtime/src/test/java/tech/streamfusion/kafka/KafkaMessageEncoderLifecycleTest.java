package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.format.*;

class KafkaMessageEncoderLifecycleTest {
  @Test
  void serializerPlanSurvivesShippingWithoutCapturingAProvider() throws Exception {
    var plan =
        EncodeFormat.resolved(
            700,
            "option=value",
            null,
            true,
            (options, types, names, fields) -> new StubEncoder(options));
    byte[] serialized;
    try (var bytes = new ByteArrayOutputStream();
        var out = new ObjectOutputStream(bytes)) {
      out.writeObject(plan);
      serialized = bytes.toByteArray();
    }
    EncodeFormat restored;
    try (var in = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
      restored = (EncodeFormat) in.readObject();
    }
    assertTrue(restored.acceptsChangelog);
    try (var encoder = restored.createEncoder(new String[0], new String[0], new int[0])) {
      assertEquals("option=value", ((StubEncoder) encoder).options);
    }
  }

  @Test
  void valueOpenFailureClosesAnAlreadyOpenedKeyEncoder() {
    List<String> closed = new ArrayList<>();
    var key =
        EncodeFormat.resolved(
            700,
            "",
            null,
            false,
            (options, types, names, fields) ->
                new StubEncoder("key") {
                  @Override
                  public void close() {
                    closed.add("key");
                  }
                });
    var value =
        EncodeFormat.resolved(
            701,
            "",
            null,
            false,
            (options, types, names, fields) -> {
              throw new IOException("open value");
            });
    IOException failure =
        assertThrows(
            IOException.class,
            () ->
                new KafkaMessageEncoder(
                    value,
                    key,
                    new String[] {"BIGINT"},
                    new String[] {"id"},
                    new int[] {0},
                    new int[] {0},
                    true));
    assertEquals("open value", failure.getMessage());
    assertEquals(List.of("key"), closed);
  }

  @Test
  void cleanupAttemptsBothEncodersWhenOneFails() throws Exception {
    List<String> closed = new ArrayList<>();
    var key =
        EncodeFormat.resolved(
            700,
            "",
            null,
            false,
            (options, types, names, fields) ->
                new StubEncoder("key") {
                  @Override
                  public void close() {
                    closed.add("key");
                  }
                });
    var value =
        EncodeFormat.resolved(
            701,
            "",
            null,
            false,
            (options, types, names, fields) ->
                new StubEncoder("value") {
                  @Override
                  public void close() throws Exception {
                    closed.add("value");
                    throw new IOException("close value");
                  }
                });
    var encoder =
        new KafkaMessageEncoder(
            value,
            key,
            new String[] {"BIGINT"},
            new String[] {"id"},
            new int[] {0},
            new int[] {0},
            true);
    assertThrows(IOException.class, encoder::close);
    assertEquals(List.of("value", "key"), closed);
  }

  @Test
  void emptyMessageIsPresentAndInvalidSlabRangesAreRejected() {
    assertArrayEquals(
        new byte[0], new EncodedMessages(new byte[0], new int[] {0}, new int[] {0}).message(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EncodedMessages(new byte[1], new int[] {1}, new int[] {1}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EncodedMessages(new byte[1], new int[] {0}, new int[] {-1}));
  }

  @Test
  void nativeFacadeOwnerClosesExactlyOnceAndRejectsUseAfterClose() {
    List<Long> released = new ArrayList<>();
    var encoder =
        NativeHandleMessageEncoder.open(
            () -> 42L,
            (handle, array, schema) -> {
              throw new AssertionError("closed encoder must not enter JNI");
            },
            released::add);
    encoder.close();
    encoder.close();
    assertEquals(List.of(42L), released);
    assertThrows(IllegalStateException.class, () -> encoder.encode(null));
  }

  @Test
  void failedNativeEntryReleasesUnconsumedCDataCallbacks() throws Exception {
    try (var allocator = new org.apache.arrow.memory.RootAllocator()) {
      try (var vector = new org.apache.arrow.vector.IntVector("v", allocator)) {
        vector.allocateNew(1);
        vector.setSafe(0, 7);
        vector.setValueCount(1);
        try (var root = org.apache.arrow.vector.VectorSchemaRoot.of(vector);
            var encoder =
                NativeHandleMessageEncoder.open(
                    () -> 1L,
                    (handle, array, schema) -> {
                      throw new IOException("rejected before import");
                    },
                    handle -> {})) {
          IOException failure = assertThrows(IOException.class, () -> encoder.encode(root));
          assertEquals("rejected before import", failure.getMessage());
          assertEquals(7, vector.get(0));
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private static class StubEncoder implements NativeMessageEncoder {
    private final String options;

    StubEncoder(String options) {
      this.options = options;
    }

    @Override
    public EncodedMessages encode(org.apache.arrow.vector.VectorSchemaRoot input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {}
  }
}
