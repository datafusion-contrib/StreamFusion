package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionV2;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import java.lang.reflect.Field;
import org.apache.flink.configuration.Configuration;
import tech.streamfusion.operator.ArrowBatch;

/**
 * The connector's default transactional sink function fed with natively encoded rows. Each row goes
 * through the function's own pre-serialized-row path, so it keeps the released framing, buffering,
 * checkpoint state, label generation, lingering-transaction abort and commit on checkpoint
 * completion. A task whose JVM the native spellings do not match writes the connector's own
 * serialized rows instead.
 *
 * <p>The connector's pre-serialized-row constructor derives Stream Load properties without the
 * table validation its row serializer path performs, which would add a {@code columns} header for
 * an aligned CSV table. The manager is therefore rebuilt from the planner's validated properties
 * before the function is serialized, so every load request carries the stock headers.
 */
public final class NativeStarRocksSinkFunction extends StarRocksDynamicSinkFunctionV2<Object> {
  private static final long serialVersionUID = 1L;

  private final StarRocksEncoding encoding;
  private transient StarRocksRowSource rows;

  public NativeStarRocksSinkFunction(
      StarRocksSinkOptions options, StreamLoadProperties properties, StarRocksEncoding encoding)
      throws ReflectiveOperationException {
    super(options);
    this.encoding = encoding;
    Field manager = StarRocksDynamicSinkFunctionV2.class.getDeclaredField("sinkManager");
    manager.setAccessible(true);
    manager.set(
        this,
        new StreamLoadManagerV2(
            properties, options.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    rows = StarRocksRowSource.open(encoding, getRuntimeContext().getMetricGroup());
  }

  @Override
  public void invoke(Object batch, Context context) throws Exception {
    rows.begin((ArrowBatch) batch);
    try {
      for (StarRocksRowData row; (row = rows.next()) != null; ) super.invoke(row, context);
    } finally {
      rows.end();
    }
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      if (rows != null) {
        rows.close();
        rows = null;
      }
    }
  }
}
