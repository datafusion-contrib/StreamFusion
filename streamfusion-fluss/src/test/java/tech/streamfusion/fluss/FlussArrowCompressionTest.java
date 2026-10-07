package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.compression.UnshadedArrowCompressionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlussArrowCompressionTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 8192, 65535, 65536, 65537, 1000000})
  void streamedFramesInteroperateWithTheReleasedCodec(int length) {
    byte[] expected = new byte[length];
    new Random(42).nextBytes(expected);
    for (int i = 0; i < length / 2; i++) expected[i] = 7;
    var stock =
        UnshadedArrowCompressionFactory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
    var streamed = FlussArrowCompression.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
    try (var allocator = new RootAllocator()) {
      for (boolean stockWrites : new boolean[] {false, true}) {
        var writer = stockWrites ? stock : streamed;
        var reader = stockWrites ? streamed : stock;
        try (var input = allocator.buffer(length)) {
          input.writeBytes(expected);
          input.getReferenceManager().retain();
          try (var compressed = writer.compress(allocator, input)) {
            compressed.getReferenceManager().retain();
            try (var decoded = reader.decompress(allocator, compressed)) {
              assertEquals(length, decoded.writerIndex());
              byte[] actual = new byte[length];
              decoded.getBytes(0, actual);
              assertArrayEquals(expected, actual);
            }
          }
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void invalidDeclaredLengthReleasesThePartialOutput() {
    var codec = FlussArrowCompression.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME);
    try (var allocator = new RootAllocator();
        var input = allocator.buffer(65537)) {
      input.setZero(0, 65537);
      input.writerIndex(65537);
      input.getReferenceManager().retain();
      try (var compressed = codec.compress(allocator, input)) {
        assertEquals(65537, compressed.getLong(0));
        compressed.setLong(0, 65538);
        long before = allocator.getAllocatedMemory();
        assertThrows(IllegalStateException.class, () -> codec.decompress(allocator, compressed));
        assertEquals(before, allocator.getAllocatedMemory());
        compressed.setLong(0, 65536);
        assertThrows(IllegalStateException.class, () -> codec.decompress(allocator, compressed));
        assertEquals(before, allocator.getAllocatedMemory());
      }
    }
  }
}
