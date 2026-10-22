package tech.streamfusion.doris;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Only the operation sidecar is materialized; business column buffers remain shared. */
final class DorisArrowChangelog {
  private DorisArrowChangelog() {}

  static VectorSchemaRoot attach(
      org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot source,
      VectorSchemaRoot business)
      throws IOException {
    var kinds =
        new TinyIntVector(
            RowDataArrowConverter.ROW_KIND_COLUMN,
            business.getFieldVectors().isEmpty()
                ? tech.streamfusion.operator.NativeAllocator.SHARED
                : business.getVector(0).getAllocator());
    try {
      var tso = metadata(source, "__DORIS_BINLOG_TSO__");
      var lsn = metadata(source, "__DORIS_BINLOG_LSN__");
      var op = metadata(source, "__DORIS_BINLOG_OP__");
      kinds.allocateNew(source.getRowCount());
      for (int i = 0; i < source.getRowCount(); i++) {
        number(tso, i);
        number(lsn, i);
        long operation = number(op, i);
        RowKind kind;
        if (operation == 0) kind = RowKind.INSERT;
        else if (operation == 1) kind = RowKind.DELETE;
        else if (operation == 2) kind = RowKind.UPDATE_BEFORE;
        else if (operation == 3) kind = RowKind.UPDATE_AFTER;
        else throw new IOException("Unsupported Doris binlog operation: " + operation);
        kinds.set(i, kind.toByteValue());
      }
      kinds.setValueCount(source.getRowCount());
      var vectors = new ArrayList<>(business.getFieldVectors());
      vectors.add(kinds);
      return new VectorSchemaRoot(vectors);
    } catch (Throwable failure) {
      kinds.close();
      throw failure;
    }
  }

  private static org.apache.doris.shaded.org.apache.arrow.vector.FieldVector metadata(
      org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot source, String name)
      throws IOException {
    var matches =
        source.getFieldVectors().stream()
            .filter(vector -> vector.getName().equalsIgnoreCase(name))
            .collect(java.util.stream.Collectors.toList());
    if (matches.size() != 1)
      throw new IOException("Missing or duplicate Doris binlog field: " + name);
    return matches.get(0);
  }

  private static long number(
      org.apache.doris.shaded.org.apache.arrow.vector.FieldVector vector, int row)
      throws IOException {
    if (vector.isNull(row)) throw new IOException("Null Doris binlog field: " + vector.getName());
    Object value = vector.getObject(row);
    try {
      return value instanceof Number
          ? ((Number) value).longValue()
          : Long.parseLong(value.toString());
    } catch (NumberFormatException failure) {
      throw new IOException("Invalid Doris binlog field: " + vector.getName(), failure);
    }
  }
}
