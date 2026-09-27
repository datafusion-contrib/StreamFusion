package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class NativeCalcSelectionOperatorTest {
  @Test
  void retainedOutputsKeepKeyGroupsAndChangelogAfterInputRelease() throws Exception {
    NativeCalcOperator operator =
        new NativeCalcOperator(
            new int[] {6, 0, 7, 6, 0, 7, 7, 0},
            new int[] {10, 1, 0, 125, 0, 1, 2, 1},
            new int[] {2, 0, 0, 3, 0, 0, 0, 0},
            new long[] {0, 1, 2},
            new double[0],
            new String[0],
            new int[] {3, 7},
            0,
            new String[] {"short_payload", "n"},
            NativeUdf.Binding.EMPTY);
    List<ArrowBatch> outputs = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator()) {
      try (var harness =
          new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
              operator, new ArrowBatchSerializer())) {
        harness.setup(new ArrowBatchSerializer());
        harness.open();
        for (int keyGroup : new int[] {7, 9, 11}) {
          harness.processElement(new StreamRecord<>(input(allocator, keyGroup)));
          @SuppressWarnings("unchecked")
          StreamRecord<ArrowBatch> record = (StreamRecord<ArrowBatch>) harness.getOutput().poll();
          outputs.add(record.getValue());
        }
        // Outputs remain valid across later batches and operator shutdown. Each retained output
        // owns its short result, rather than holding the source payload allocation alive.
      }
      assertEquals(0, allocator.getAllocatedMemory());
      for (int index = 0; !outputs.isEmpty(); index++) {
        ArrowBatch batch = outputs.remove(0);
        try (VectorSchemaRoot root = batch.root()) {
          assertEquals(7 + index * 2, batch.keyGroup());
          assertEquals(2, root.getRowCount());
          assertEquals("é中", root.getVector(0).getObject(0).toString());
          assertEquals("é中", root.getVector(0).getObject(1).toString());
          assertEquals(1, root.getVector(1).getObject(0));
          assertEquals(2, root.getVector(1).getObject(1));
          assertEquals((byte) 1, root.getVector(2).getObject(0));
          assertEquals((byte) 3, root.getVector(2).getObject(1));
        }
      }
    } finally {
      for (ArrowBatch output : outputs) output.root().close();
    }
  }

  private static ArrowBatch input(BufferAllocator allocator, int keyGroup) {
    Schema schema =
        new Schema(
            List.of(
                Field.nullable("payload", ArrowType.Utf8.INSTANCE),
                Field.nullable("n", new ArrowType.Int(32, true)),
                Field.notNullable("$row_kind$", new ArrowType.Int(8, true))));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    byte[] payload = "é中🙂".repeat(500).getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < 4; i++) {
      ((VarCharVector) root.getVector(0)).setSafe(i, payload);
      ((IntVector) root.getVector(1)).setSafe(i, i / 2 + (i % 2 == 0 ? -3 : 1));
      ((TinyIntVector) root.getVector(2)).setSafe(i, i);
    }
    root.setRowCount(4);
    return new ArrowBatch(root, keyGroup);
  }
}
