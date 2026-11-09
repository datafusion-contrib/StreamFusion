package tech.streamfusion;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

final class RecoveryResultEncoding {
  private RecoveryResultEncoding() {}

  static byte[] encode(RowDataSerializer serializer, RowData row, boolean compressed)
      throws IOException {
    var output = new DataOutputSerializer(64);
    serializer.serialize(row, output);
    if (!compressed) return output.getCopyOfBuffer();
    var bytes = new ByteArrayOutputStream();
    var deflater = new Deflater(Deflater.BEST_SPEED);
    try (var stream = new DeflaterOutputStream(bytes, deflater)) {
      stream.write(output.getSharedBuffer(), 0, output.length());
    } finally {
      deflater.end();
    }
    return bytes.toByteArray();
  }

  static RowData decode(RowDataSerializer serializer, byte[] bytes, boolean compressed)
      throws IOException {
    if (!compressed) return serializer.deserialize(new DataInputDeserializer(bytes));
    try (var input = new InflaterInputStream(new ByteArrayInputStream(bytes))) {
      return serializer.deserialize(new DataInputViewStreamWrapper(input));
    }
  }
}
