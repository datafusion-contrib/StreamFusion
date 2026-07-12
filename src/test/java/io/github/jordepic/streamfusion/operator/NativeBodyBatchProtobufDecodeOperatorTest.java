package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.protobuf.ProtobufFormatProvider;
import io.github.jordepic.streamfusion.proto.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Verifies the generic body-batch operator delegates protobuf decoding to its format provider. */
@Tag("streamfusion-protobuf")
class NativeBodyBatchProtobufDecodeOperatorTest {

  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
          },
          new String[] {"id", "name", "score"});

  @Test
  void decodesProtobufBodiesToTypedBatch() throws Exception {
    byte[] first = Row.newBuilder().setId(1L).setName("a").setScore(1.5).build().toByteArray();
    byte[] second = Row.newBuilder().setId(2L).setName("b").setScore(2.5).build().toByteArray();

    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(decoder(), new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(bodies(NativeAllocator.SHARED, first, second)));

      assertEquals(List.of(List.of(1L, "a", 1.5), List.of(2L, "b", 2.5)), collect(harness));
    }
  }

  private static NativeBodyBatchDecodeOperator decoder() {
    return new NativeBodyBatchDecodeOperator(
        OUTPUT,
        new ProtobufFormatProvider()
            .createDecoder(
                new NativeFormatContext(
                    OUTPUT,
                    OUTPUT,
                    Map.of(
                        "format", "protobuf",
                        "protobuf.message-class-name", Row.class.getName()),
                    false)));
  }

  /** Builds an Arrow batch of one binary column ("body") holding the raw protobuf messages. */
  private static ArrowBatch bodies(BufferAllocator allocator, byte[]... messages) {
    VarBinaryVector vector = new VarBinaryVector("body", allocator);
    vector.allocateNew(messages.length);
    for (int i = 0; i < messages.length; i++) {
      vector.setSafe(i, messages[i]);
    }
    vector.setValueCount(messages.length);
    VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));
    root.setRowCount(messages.length);
    return new ArrowBatch(root);
  }

  /** Reads the provider-decoded batch by the protobuf field names. */
  private static List<List<Object>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            rows.add(
                List.of(
                    ((Number) root.getVector("id").getObject(i)).longValue(),
                    root.getVector("name").getObject(i).toString(),
                    ((Number) root.getVector("score").getObject(i)).doubleValue()));
          }
        }
      }
    }
    return rows;
  }
}
