package tech.streamfusion.shuffle;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.compression.BlockDecompressor;
import org.apache.flink.runtime.io.compression.BufferDecompressionException;
import org.apache.flink.runtime.io.compression.CompressorUtils;

/** Reads Flink's existing block framing through the bounds-checked native LZ4 API. */
public final class NativeLz4BlockDecompressor implements BlockDecompressor {
  private final LZ4SafeDecompressor decoder;
  private final Counter blocks;
  private final Counter bytes;
  private byte[] inputCopy = new byte[0];

  public NativeLz4BlockDecompressor(MetricGroup metrics) {
    this(metrics.counter("nativeLz4Blocks"), metrics.counter("nativeLz4Bytes"));
  }

  NativeLz4BlockDecompressor(Counter blocks, Counter bytes) {
    decoder = LZ4Factory.nativeInstance().safeDecompressor();
    if (!decoder.getClass().getSimpleName().equals("LZ4JNISafeDecompressor"))
      throw new IllegalStateException("Native LZ4 safe decoder is unavailable");
    this.blocks = blocks;
    this.bytes = bytes;
  }

  public String implementation() {
    return decoder.getClass().getSimpleName();
  }

  @Override
  public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) {
    checkSource(src.length, srcOff, srcLen);
    int compressed = CompressorUtils.readIntLE(src, srcOff);
    int original = CompressorUtils.readIntLE(src, srcOff + 4);
    checkLengths(compressed, original, srcLen, dst.length, dstOff);
    try {
      int decoded =
          decoder.decompress(
              src, srcOff + CompressorUtils.HEADER_LENGTH, compressed, dst, dstOff, original);
      return completed(decoded, original);
    } catch (net.jpountz.lz4.LZ4Exception failure) {
      throw new BufferDecompressionException("Input is corrupted", failure);
    }
  }

  @Override
  public int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff) {
    checkSource(src.remaining(), srcOff, srcLen);
    if (dst.isReadOnly()) throw new BufferDecompressionException("Output buffer is read-only");
    int start = src.position() + srcOff;
    src.order(ByteOrder.LITTLE_ENDIAN);
    int compressed = src.getInt(start);
    int original = src.getInt(start + 4);
    checkLengths(compressed, original, srcLen, dst.remaining(), dstOff);
    int output = dst.position() + dstOff;
    try {
      ByteBuffer input = src;
      int offset = start + CompressorUtils.HEADER_LENGTH;
      // A read-only heap view hides its array from JNI. Copy only this representation;
      // ordinary Flink heap arrays and direct network buffers keep their existing storage.
      if (!src.hasArray() && !src.isDirect()) {
        if (inputCopy.length < compressed) inputCopy = new byte[compressed];
        var view = src.duplicate();
        view.position(offset);
        view.get(inputCopy, 0, compressed);
        input = ByteBuffer.wrap(inputCopy);
        offset = 0;
      }
      int decoded = decoder.decompress(input, offset, compressed, dst, output, original);
      completed(decoded, original);
      src.position(start + CompressorUtils.HEADER_LENGTH + compressed);
      dst.position(output + original);
      return original;
    } catch (net.jpountz.lz4.LZ4Exception failure) {
      throw new BufferDecompressionException("Input is corrupted", failure);
    }
  }

  private static void checkSource(int available, int offset, int length) {
    if (offset < 0 || length < CompressorUtils.HEADER_LENGTH || offset > available - length)
      throw new BufferDecompressionException("Input is truncated or has an invalid offset");
  }

  private static void checkLengths(
      int compressed, int original, int available, int outputSize, int outputOffset) {
    CompressorUtils.validateLength(compressed, original);
    if (compressed > available - CompressorUtils.HEADER_LENGTH)
      throw new BufferDecompressionException("Input is truncated");
    if (outputOffset < 0 || outputOffset > outputSize - original)
      throw new BufferDecompressionException("Output buffer is too small");
  }

  private int completed(int decoded, int original) {
    if (decoded != original)
      throw new BufferDecompressionException("Input is corrupted, unexpected decompressed length");
    blocks.inc();
    bytes.inc(original);
    return original;
  }
}
