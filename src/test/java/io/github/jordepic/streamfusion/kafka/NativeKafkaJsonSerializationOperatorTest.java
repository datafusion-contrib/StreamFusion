package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchSerializer;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaJsonSerializationOperatorTest {

  @Test
  void emitsOnePreSerializedValuePerArrowRow() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    List<byte[]> output = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, byte[]> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeKafkaJsonSerializationOperator(false, "SQL"),
                new ArrowBatchSerializer())) {
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(
                          GenericRowData.of(1, StringData.fromString("one")),
                          GenericRowData.of(2, null)),
                      rowType,
                      allocator))));
      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord) {
          output.add(((StreamRecord<byte[]>) record).getValue());
        }
      }
    }

    assertEquals(2, output.size());
    assertArrayEquals(
        "{\"id\":1,\"name\":\"one\"}".getBytes(StandardCharsets.UTF_8), output.get(0));
    assertArrayEquals(
        "{\"id\":2,\"name\":null}".getBytes(StandardCharsets.UTF_8), output.get(1));
  }

  @Test
  void stockKafkaSchemaPublishesTheBytesUnchanged() {
    byte[] value = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
    PreSerializedKafkaRecordSchema schema = new PreSerializedKafkaRecordSchema("output");

    ProducerRecord<byte[], byte[]> record = schema.serialize(value, null, 123L);

    assertEquals("output", record.topic());
    assertNull(record.key());
    assertNull(record.timestamp());
    assertArrayEquals(value, record.value());
    assertEquals(
        List.of("output"),
        schema.getKafkaDatasetFacet().orElseThrow().getTopicIdentifier().getTopics());
  }
}
