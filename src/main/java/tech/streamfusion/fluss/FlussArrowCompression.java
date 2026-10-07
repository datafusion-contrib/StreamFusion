package tech.streamfusion.fluss;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.compression.FlussLZ4BlockInputStream;
import org.apache.fluss.compression.FlussLZ4BlockOutputStream;
import org.apache.fluss.compression.UnshadedArrowCompressionFactory;

/** Reuses Fluss framing without staging complete LZ4 vectors in growing heap streams. */
final class FlussArrowCompression implements CompressionCodec.Factory {
  static final FlussArrowCompression INSTANCE = new FlussArrowCompression();

  private FlussArrowCompression() {}

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType type) {
    return streaming(type)
        ? new StreamingLz4()
        : UnshadedArrowCompressionFactory.INSTANCE.createCodec(type);
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType type, int level) {
    return streaming(type)
        ? new StreamingLz4()
        : UnshadedArrowCompressionFactory.INSTANCE.createCodec(type, level);
  }

  private static boolean streaming(CompressionUtil.CodecType type) {
    return type == CompressionUtil.CodecType.LZ4_FRAME
        && Boolean.parseBoolean(
            System.getProperty("streamfusion.fluss.lz4-streaming.enabled", "true"));
  }

  private static final class StreamingLz4 extends AbstractCompressionCodec {
    private final byte[] scratch = new byte[8192];

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf input) {
      long length = input.writerIndex();
      if (length > Integer.MAX_VALUE)
        throw new IllegalArgumentException("LZ4 vector exceeds integer range");
      long blocks = (length + 65535) / 65536;
      ArrowBuf result =
          allocator.buffer(
              CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
                  + length
                  + blocks * 4
                  + FlussLZ4BlockOutputStream.LZ4_MAX_HEADER_LENGTH
                  + 4);
      BufferOutput output = new BufferOutput(result, CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
      try {
        try (var stream = new FlussLZ4BlockOutputStream(output)) {
          for (long position = 0; position < length; ) {
            int count = (int) Math.min(scratch.length, length - position);
            input.getBytes(position, scratch, 0, count);
            stream.write(scratch, 0, count);
            position += count;
          }
        }
        result.writerIndex(output.position);
        return result;
      } catch (IOException failure) {
        result.close();
        throw new IllegalStateException("Fluss LZ4 compression failed", failure);
      } catch (RuntimeException | Error failure) {
        result.close();
        throw failure;
      }
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf input) {
      long length = readUncompressedLength(input);
      if (length < 0 || length > Integer.MAX_VALUE)
        throw new IllegalArgumentException("Invalid LZ4 vector length");
      ArrowBuf result = allocator.buffer(length);
      try (var stream =
          new FlussLZ4BlockInputStream(
              input.nioBuffer(
                  CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
                  Math.toIntExact(
                      input.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)))) {
        long position = 0;
        while (position < length) {
          int count = stream.read(scratch, 0, (int) Math.min(scratch.length, length - position));
          if (count <= 0) throw new IOException("Truncated Fluss LZ4 vector");
          result.setBytes(position, scratch, 0, count);
          position += count;
        }
        if (stream.read() != -1) throw new IOException("Fluss LZ4 vector exceeds declared length");
        result.writerIndex(length);
        return result;
      } catch (IOException failure) {
        result.close();
        throw new IllegalStateException("Fluss LZ4 decompression failed", failure);
      } catch (RuntimeException | Error failure) {
        result.close();
        throw failure;
      }
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return CompressionUtil.CodecType.LZ4_FRAME;
    }
  }

  private static final class BufferOutput extends OutputStream {
    private final ArrowBuf buffer;
    private long position;

    BufferOutput(ArrowBuf buffer, long position) {
      this.buffer = buffer;
      this.position = position;
    }

    @Override
    public void write(int value) {
      buffer.setByte(position++, value);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
      buffer.setBytes(position, bytes, offset, length);
      position += length;
    }
  }
}
