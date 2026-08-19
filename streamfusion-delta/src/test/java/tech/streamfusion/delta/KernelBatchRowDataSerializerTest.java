package tech.streamfusion.delta;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class KernelBatchRowDataSerializerTest {

  private final KernelBatchRowDataSerializer serializer = new KernelBatchRowDataSerializer();

  @Test
  void chainedCopiesTransferTheSameView() {
    RowData row = GenericRowData.of(1L, 2);

    assertSame(row, serializer.copy(row));
    assertSame(row, serializer.copy(row, GenericRowData.of()));
  }

  @Test
  void serializedEdgesAreRejected() {
    assertThrows(
        IOException.class,
        () -> serializer.serialize(GenericRowData.of(1), new DataOutputSerializer(32)));
    assertThrows(
        IOException.class,
        () -> serializer.deserialize(new DataInputDeserializer(new byte[0])));
    assertThrows(
        IOException.class,
        () ->
            serializer.copy(
                new DataInputDeserializer(new byte[0]), new DataOutputSerializer(32)));
  }
}
