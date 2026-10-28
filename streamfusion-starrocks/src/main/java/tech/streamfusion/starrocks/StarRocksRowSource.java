package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import java.io.IOException;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.LoggerFactory;
import tech.streamfusion.operator.ArrowBatch;

/**
 * A writer task's connector rows for each Arrow batch: {@link #begin} hands over a batch, {@link
 * #next} returns its rows in order, and {@link #end} releases what remains of it.
 */
interface StarRocksRowSource extends AutoCloseable {

  /**
   * The native encoder, or the connector's own row serialization when this task's JVM would make
   * the stock connector spell a value differently, as a TaskManager on another JDK feature release
   * or in another time zone than the planning JVM can.
   */
  static StarRocksRowSource open(StarRocksEncoding encoding, MetricGroup metrics)
      throws IOException {
    String mismatch = StarRocksSpellingProbe.mismatch(encoding);
    if (mismatch == null) return new NativeStarRocksEncoder(encoding, metrics);
    LoggerFactory.getLogger(StarRocksRowSource.class)
        .warn(
            "StarRocks native sink: {} on this task, unlike the JVM that planned the job, so this"
                + " task writes its rows through the connector's own serializer.",
            mismatch);
    return new ConnectorStarRocksRows(encoding);
  }

  void begin(ArrowBatch batch) throws IOException;

  /** The current batch's next row, or null after the last. The row may be reused by later calls. */
  StarRocksRowData next() throws IOException;

  void end();

  @Override
  void close();
}
