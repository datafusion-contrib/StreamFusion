package tech.streamfusion.paimon;

import java.nio.ByteBuffer;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.compression.CompressorUtils;

/** Paimon's fast LZO compressor applied to bounded Arrow IPC byte blocks. */
public final class PaimonLzoCompressor {
  private final BlockCompressor compressor =
      BlockCompressionFactory.create(new CompressOptions("lzo", 1)).getCompressor();
  private final byte[] input = new byte[64 * 1024];
  private final byte[] output = new byte[compressor.getMaxCompressedSize(input.length)];

  /** JNI borrows these buffers for this call; neither direct buffer is retained. */
  public int compress(ByteBuffer source, ByteBuffer destination) {
    int length = source.remaining();
    source.get(input, 0, length);
    int size = compressor.compress(input, 0, length, output, 0);
    int header = CompressorUtils.HEADER_LENGTH;
    destination.put(output, header, size - header);
    return size - header;
  }
}
