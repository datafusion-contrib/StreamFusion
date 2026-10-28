package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.RecordSerializationSchema;
import com.starrocks.connector.flink.table.sink.v2.StarRocksCommittable;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSink;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSinkContext;
import com.starrocks.connector.flink.table.sink.v2.StarRocksWriter;
import com.starrocks.connector.flink.table.sink.v2.StarRocksWriterState;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import tech.streamfusion.operator.ArrowBatch;

/**
 * The connector's unified sink ({@code sink.use.new-sink-api}) fed with natively encoded rows. Its
 * writer receives every row through the released pre-serialized-row path, and its writer state,
 * committables and committer are the connector's own. A task whose JVM the native spellings do not
 * match writes the connector's own serialized rows instead.
 */
public final class NativeStarRocksSink
    implements StatefulSink<ArrowBatch, StarRocksWriterState>,
        TwoPhaseCommittingSink<ArrowBatch, StarRocksCommittable> {
  private static final long serialVersionUID = 1L;

  private final StarRocksSink<StarRocksRowData> rows;
  private final StarRocksEncoding encoding;

  public NativeStarRocksSink(
      StarRocksSinkOptions options, StreamLoadProperties properties, StarRocksEncoding encoding) {
    this.rows = new StarRocksSink<>(options, new EncodedRows(), properties);
    this.encoding = encoding;
  }

  @Override
  public Writer createWriter(InitContext context) throws IOException {
    StarRocksRowSource source = StarRocksRowSource.open(encoding, context.metricGroup());
    try {
      return new Writer(rows.createWriter(context), source);
    } catch (IOException | RuntimeException failure) {
      source.close();
      throw failure;
    }
  }

  @Override
  public Writer restoreWriter(InitContext context, Collection<StarRocksWriterState> state)
      throws IOException {
    StarRocksRowSource source = StarRocksRowSource.open(encoding, context.metricGroup());
    try {
      return new Writer(rows.restoreWriter(context, state), source);
    } catch (IOException | RuntimeException failure) {
      source.close();
      throw failure;
    }
  }

  @Override
  public SimpleVersionedSerializer<StarRocksWriterState> getWriterStateSerializer() {
    return rows.getWriterStateSerializer();
  }

  @Override
  public Committer<StarRocksCommittable> createCommitter() throws IOException {
    return rows.createCommitter();
  }

  @Override
  public SimpleVersionedSerializer<StarRocksCommittable> getCommittableSerializer() {
    return rows.getCommittableSerializer();
  }

  /** The connector's writer, handed each batch's rows as its serializer would produce them. */
  static final class Writer
      implements StatefulSinkWriter<ArrowBatch, StarRocksWriterState>,
          PrecommittingSinkWriter<ArrowBatch, StarRocksCommittable> {
    private final StarRocksWriter<StarRocksRowData> rows;
    private final StarRocksRowSource source;

    Writer(StarRocksWriter<StarRocksRowData> rows, StarRocksRowSource source) {
      this.rows = rows;
      this.source = source;
    }

    @Override
    public void write(ArrowBatch batch, Context context) throws IOException, InterruptedException {
      source.begin(batch);
      try {
        for (StarRocksRowData row; (row = source.next()) != null; ) rows.write(row, context);
      } finally {
        source.end();
      }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
      rows.writeWatermark(watermark);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
      rows.flush(endOfInput);
    }

    @Override
    public Collection<StarRocksCommittable> prepareCommit()
        throws IOException, InterruptedException {
      return rows.prepareCommit();
    }

    @Override
    public List<StarRocksWriterState> snapshotState(long checkpointId) throws IOException {
      return rows.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
      try {
        rows.close();
      } finally {
        source.close();
      }
    }
  }

  /** Passes the natively encoded rows through the connector's serialization step unchanged. */
  private static final class EncodedRows implements RecordSerializationSchema<StarRocksRowData> {
    private static final long serialVersionUID = 1L;

    @Override
    public void open(
        SerializationSchema.InitializationContext context, StarRocksSinkContext sinkContext) {}

    @Override
    public StarRocksRowData serialize(StarRocksRowData row) {
      return row;
    }

    @Override
    public void close() {}
  }
}
