package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.v2.RowDataSerializationSchema;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Rows produced by the connector's own row serialization, built in the planning JVM as its sink
 * builds it, for a task whose JVM the native encoder's spellings do not match. Each batch is read
 * back into rows, so the task writes exactly what the stock writer would.
 */
final class ConnectorStarRocksRows implements StarRocksRowSource {
  private final RowType type;
  private final RowDataSerializationSchema serialization;
  private Iterator<RowData> rows = Collections.emptyIterator();

  ConnectorStarRocksRows(StarRocksEncoding encoding) throws IOException {
    type = encoding.type;
    try {
      serialization =
          InstantiationUtil.clone(
              encoding.connectorRows, ConnectorStarRocksRows.class.getClassLoader());
    } catch (ClassNotFoundException incompatible) {
      throw new IOException("cannot copy the connector's row serialization", incompatible);
    }
    serialization.open(null, null);
  }

  @Override
  public void begin(ArrowBatch batch) {
    try (VectorSchemaRoot root = batch.root()) {
      rows = RowDataArrowConverter.read(root, type).iterator();
    }
  }

  @Override
  public StarRocksRowData next() {
    while (rows.hasNext()) {
      StarRocksRowData row = serialization.serialize(rows.next());
      if (row != null) return row;
    }
    return null;
  }

  @Override
  public void end() {
    rows = Collections.emptyIterator();
  }

  @Override
  public void close() {
    serialization.close();
  }
}
