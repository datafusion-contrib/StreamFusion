package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.NativeBodyBatchDecoder;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.json.JsonFormatProvider;
import tech.streamfusion.operator.NativeAllocator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A poison-pill message must fail the split fetch diagnosably: the IOException names the batch's
 * topic, partition, and offset range and keeps the format's own parse error, so an operator can be
 * pointed at the exact records without replaying the job under a debugger.
 */
@Tag("streamfusion-json")
class NativeKafkaDecodeFailureContextTest {

  private static final RowType OUTPUT =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});

  @Test
  void decodeFailureNamesTopicPartitionAndOffsets() throws Exception {
    try (NativeBodyBatchDecoder decoder =
        new NativeBodyBatchDecoder(
            new JsonFormatProvider()
                .createDecoder(new NativeFormatContext(OUTPUT, OUTPUT, Map.of("format", "json"), false)),
            OUTPUT,
            NativeAllocator.SHARED)) {
      VectorSchemaRoot bodies =
          bodies(NativeAllocator.SHARED, "{\"id\": 1}", "{\"id\": not json");
      IOException failure =
          assertThrows(
              IOException.class,
              () ->
                  NativeKafkaSplitReader.decodeSplitBodies(decoder, bodies, "events", 3, 40, 42));
      assertTrue(
          failure.getMessage().startsWith("decode failed on topic-partition events-3 offsets [40..42): "),
          "missing topic/partition/offset context: " + failure.getMessage());
      assertTrue(
          failure.getMessage().contains("failed to decode JSON record"),
          "missing the format's own error text: " + failure.getMessage());
      assertNotNull(failure.getCause(), "the decoder's exception must stay on the cause chain");
    }
  }

  private static VectorSchemaRoot bodies(BufferAllocator allocator, String... docs) {
    VarBinaryVector vector = new VarBinaryVector("body", allocator);
    vector.allocateNew(docs.length);
    for (int i = 0; i < docs.length; i++) {
      vector.setSafe(i, docs[i].getBytes(StandardCharsets.UTF_8));
    }
    vector.setValueCount(docs.length);
    VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));
    root.setRowCount(docs.length);
    return root;
  }
}
