package tech.streamfusion.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;

/** Released list/sorted-map entry streams, keeping raw TTL envelopes separate from payloads. */
final class StockRankValues<V> implements Iterable<V> {
  final long written;
  final int size;
  private final byte[] encoded;
  private final int offset;
  private final Decoder<V> decoder;

  @FunctionalInterface
  private interface Decoder<V> {
    V read(DataInputDeserializer input) throws IOException;
  }

  private StockRankValues(
      byte[] encoded, DataInputDeserializer input, long written, Decoder<V> decoder)
      throws IOException {
    this.encoded = encoded;
    this.written = written;
    this.decoder = decoder;
    size = input.readInt();
    if (size < 0) throw new IOException("Negative canonical rank collection size");
    offset = encoded.length - input.available();
  }

  static <V> StockRankValues<V> list(byte[] encoded, boolean ttl, TypeSerializer<V> values)
      throws IOException {
    var input = new DataInputDeserializer(encoded);
    if (input.readBoolean()) throw new IllegalStateException("NULL retained list");
    long written = ttl ? LongSerializer.INSTANCE.deserialize(input) : 0;
    return new StockRankValues<>(encoded, input, written, values::deserialize);
  }

  static <K> StockRankValues<Tuple2<K, Long>> counters(
      byte[] encoded, boolean ttl, TypeSerializer<K> keys) throws IOException {
    var input = new DataInputDeserializer(encoded);
    long written = ttl ? LongSerializer.INSTANCE.deserialize(input) : 0;
    return new StockRankValues<>(
        encoded,
        input,
        written,
        reader -> {
          K key = keys.deserialize(reader);
          Long value = reader.readBoolean() ? null : LongSerializer.INSTANCE.deserialize(reader);
          return Tuple2.of(key, value);
        });
  }

  @Override
  public Iterator<V> iterator() {
    var input = new DataInputDeserializer(encoded, offset, encoded.length - offset);
    return new Iterator<>() {
      private int remaining = size;

      @Override
      public boolean hasNext() {
        if (remaining == 0 && input.available() != 0)
          throw new IllegalArgumentException("Trailing canonical rank value bytes");
        return remaining > 0;
      }

      @Override
      public V next() {
        if (!hasNext()) throw new NoSuchElementException();
        try {
          V value = decoder.read(input);
          remaining--;
          if (remaining == 0) hasNext();
          return value;
        } catch (IOException failure) {
          throw new UncheckedIOException(failure);
        }
      }
    };
  }
}
