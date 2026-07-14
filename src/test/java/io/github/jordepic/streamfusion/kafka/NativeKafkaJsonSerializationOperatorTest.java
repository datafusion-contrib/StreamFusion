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
import org.apache.flink.types.RowKind;
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
        OneInputStreamOperatorTestHarness<ArrowBatch, PreSerializedKafkaRecord> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeKafkaJsonSerializationOperator(
                    false,
                    "SQL",
                    rowType.getChildren().stream().map(Object::toString).toArray(String[]::new),
                    new int[0],
                    new int[] {0, 1},
                    false),
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
          output.add(((StreamRecord<PreSerializedKafkaRecord>) record).getValue().value());
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
    byte[] key = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
    byte[] value = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
    PreSerializedKafkaRecordSchema schema = new PreSerializedKafkaRecordSchema("output");

    ProducerRecord<byte[], byte[]> record =
        schema.serialize(new PreSerializedKafkaRecord(key, value), null, 123L);

    assertEquals("output", record.topic());
    assertArrayEquals(key, record.key());
    assertNull(record.timestamp());
    assertArrayEquals(value, record.value());
    assertEquals(
        List.of("output"),
        schema.getKafkaDatasetFacet().orElseThrow().getTopicIdentifier().getTopics());
  }

  @Test
  void serializesUpsertKeysAndTombstonesInOneBatch() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    GenericRowData insert = GenericRowData.of(1, StringData.fromString("one"));
    GenericRowData before = GenericRowData.of(1, StringData.fromString("one"));
    before.setRowKind(RowKind.UPDATE_BEFORE);
    GenericRowData after = GenericRowData.of(1, StringData.fromString("updated"));
    after.setRowKind(RowKind.UPDATE_AFTER);
    GenericRowData delete = GenericRowData.of(1, StringData.fromString("updated"));
    delete.setRowKind(RowKind.DELETE);
    List<PreSerializedKafkaRecord> output = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, PreSerializedKafkaRecord> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeKafkaJsonSerializationOperator(
                    false,
                    "SQL",
                    rowType.getChildren().stream().map(Object::toString).toArray(String[]::new),
                    new int[] {0},
                    new int[] {0, 1},
                    true),
                new ArrowBatchSerializer())) {
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(insert, before, after, delete), rowType, allocator, true))));
      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord) {
          output.add(((StreamRecord<PreSerializedKafkaRecord>) record).getValue());
        }
      }
    }

    assertEquals(4, output.size());
    for (PreSerializedKafkaRecord record : output) {
      assertArrayEquals("{\"id\":1}".getBytes(StandardCharsets.UTF_8), record.key());
    }
    assertArrayEquals(
        "{\"id\":1,\"name\":\"one\"}".getBytes(StandardCharsets.UTF_8), output.get(0).value());
    assertNull(output.get(1).value());
    assertArrayEquals(
        "{\"id\":1,\"name\":\"updated\"}".getBytes(StandardCharsets.UTF_8),
        output.get(2).value());
    assertNull(output.get(3).value());
  }
}
