package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.compression.BufferDecompressionException;
import org.apache.flink.runtime.io.compression.CompressorUtils;
import org.apache.flink.runtime.io.compression.Lz4BlockCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.shuffle.NativeLz4BlockDecompressor;
import tech.streamfusion.shuffle.NativeShuffleDecompression;

class NativeLz4ShuffleTest {
  private static final class Metrics extends UnregisteredMetricsGroup {
    final Counter blocks = new SimpleCounter();
    final Counter bytes = new SimpleCounter();
    final Counter gates = new SimpleCounter();

    @Override
    public Counter counter(String name) {
      if (name.equals("nativeLz4Blocks")) return blocks;
      if (name.equals("nativeLz4Bytes")) return bytes;
      if (name.equals("nativeLz4InputGates")) return gates;
      return super.counter(name);
    }
  }

  @Test
  void readsStockBlocksWithArrayOffsetsAndReusedDecoder() {
    var metrics = new Metrics();
    var nativeCodec = new NativeLz4BlockDecompressor(metrics);
    assertEquals("LZ4JNISafeDecompressor", nativeCodec.implementation());
    long total = 0;
    for (int size : new int[] {1, 7, 32768, 131072}) {
      for (boolean random : new boolean[] {false, true}) {
        byte[] input = values(size, random), block = stockBlock(input);
        byte[] wrapped = new byte[block.length + 17];
        System.arraycopy(block, 0, wrapped, 9, block.length);
        byte[] output = new byte[size + 13];
        Arrays.fill(output, (byte) 77);
        assertEquals(size, nativeCodec.decompress(wrapped, 9, block.length, output, 5));
        assertArrayEquals(input, Arrays.copyOfRange(output, 5, size + 5));
        assertEquals(77, output[4]);
        assertEquals(77, output[size + 5]);
        total += size;
      }
    }
    assertEquals(8, metrics.blocks.getCount());
    assertEquals(total, metrics.bytes.getCount());
  }

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "true,false,false",
    "false,true,false",
    "true,true,false",
    "false,true,true",
    "false,false,true",
    "true,false,true",
    "true,true,true"
  })
  void handlesHeapDirectAndReadOnlyHeapViews(
      boolean directInput, boolean directOutput, boolean readOnlyInput) {
    byte[] original = values(65536, false), block = stockBlock(original);
    ByteBuffer input =
        directInput
            ? ByteBuffer.allocateDirect(block.length + 17)
            : ByteBuffer.allocate(block.length + 17);
    input.position(11);
    input.put(block);
    input.limit(11 + block.length);
    input.position(7);
    if (readOnlyInput) input = input.asReadOnlyBuffer();
    ByteBuffer output =
        directOutput
            ? ByteBuffer.allocateDirect(original.length + 19)
            : ByteBuffer.allocate(original.length + 19);
    output.position(3);
    var metrics = new Metrics();
    var codec = new NativeLz4BlockDecompressor(metrics);
    assertEquals(original.length, codec.decompress(input, 4, block.length, output, 5));
    assertEquals(11 + block.length, input.position());
    assertEquals(8 + original.length, output.position());
    byte[] decoded = new byte[original.length];
    output.position(8);
    output.get(decoded);
    assertArrayEquals(original, decoded);
    assertEquals(1, metrics.blocks.getCount());
    assertEquals(original.length, metrics.bytes.getCount());
  }

  @Test
  void rejectsCorruptLengthsTruncatedPayloadsAndUndersizedOutputs() {
    byte[] input = values(32768, false), valid = stockBlock(input);
    var metrics = new Metrics();
    var codec = new NativeLz4BlockDecompressor(metrics);
    byte[] output = new byte[input.length + 8];
    Arrays.fill(output, (byte) 77);
    assertThrows(
        BufferDecompressionException.class,
        () -> codec.decompress(valid, -1, valid.length, output, 0));
    assertThrows(
        BufferDecompressionException.class, () -> codec.decompress(valid, 0, 7, output, 0));
    assertThrows(
        BufferDecompressionException.class,
        () -> codec.decompress(valid, 0, valid.length - 1, output, 0));
    assertThrows(
        BufferDecompressionException.class,
        () -> codec.decompress(valid, 0, valid.length, new byte[16], 0));
    for (int length : new int[] {-1, Integer.MAX_VALUE}) {
      byte[] corrupted = valid.clone();
      CompressorUtils.writeIntLE(length, corrupted, 0);
      assertThrows(
          BufferDecompressionException.class,
          () -> codec.decompress(corrupted, 0, corrupted.length, output, 4));
    }
    byte[] wrongSize = valid.clone();
    CompressorUtils.writeIntLE(input.length - 1, wrongSize, 4);
    assertThrows(
        BufferDecompressionException.class,
        () -> codec.decompress(wrongSize, 0, wrongSize.length, output, 4));
    byte[] wrongLargerSize = valid.clone();
    CompressorUtils.writeIntLE(input.length + 1, wrongLargerSize, 4);
    assertThrows(
        BufferDecompressionException.class,
        () -> codec.decompress(wrongLargerSize, 0, wrongLargerSize.length, output, 4));
    assertEquals(77, output[3]);
    assertEquals(77, output[output.length - 1]);
    assertEquals(0, metrics.blocks.getCount());
    assertEquals(0, metrics.bytes.getCount());
    assertThrows(
        BufferDecompressionException.class,
        () ->
            codec.decompress(
                ByteBuffer.wrap(valid),
                0,
                valid.length,
                ByteBuffer.allocate(input.length).asReadOnlyBuffer(),
                0));
  }

  @Test
  void installsOnceAndLeavesOtherCodecsAndInstancesUnchanged() throws Exception {
    var selected = buffer("LZ4");
    var stock = buffer("LZ4");
    var other = buffer("ZSTD");
    var metrics = new Metrics();
    var field = BufferDecompressor.class.getDeclaredField("blockDecompressor");
    field.setAccessible(true);
    Object originalStock = field.get(stock), originalOther = field.get(other);
    var customized = buffer("LZ4");
    Object custom = new org.apache.flink.runtime.io.compression.Lz4BlockDecompressor() {};
    field.set(customized, custom);
    assertTrue(NativeShuffleDecompression.install(selected, metrics));
    assertInstanceOf(NativeLz4BlockDecompressor.class, field.get(selected));
    assertFalse(NativeShuffleDecompression.install(selected, metrics));
    assertFalse(NativeShuffleDecompression.install(other, metrics));
    assertFalse(NativeShuffleDecompression.install(customized, metrics));
    assertSame(originalStock, field.get(stock));
    assertSame(originalOther, field.get(other));
    assertSame(custom, field.get(customized));
    assertEquals(1, metrics.gates.getCount());
  }

  private static byte[] values(int size, boolean random) {
    byte[] bytes = new byte[size];
    if (random) new Random(123).nextBytes(bytes);
    else for (int i = 0; i < size; i++) bytes[i] = (byte) (i % 7);
    return bytes;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static BufferDecompressor buffer(String codec) throws Exception {
    var constructor = BufferDecompressor.class.getConstructors()[0];
    Class<?> type = constructor.getParameterTypes()[1];
    Object value = type == String.class ? codec : Enum.valueOf((Class) type, codec);
    return (BufferDecompressor) constructor.newInstance(32768, value);
  }

  private static byte[] stockBlock(byte[] input) {
    var stock = new Lz4BlockCompressor();
    byte[] output = new byte[stock.getMaxCompressedSize(input.length)];
    int count = stock.compress(input, 0, input.length, output, 0);
    return Arrays.copyOf(output, count);
  }
}
