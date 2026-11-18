package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * The plan-time inference compiles an encoded Calc without evaluating it and reports the Arrow types
 * its projections would produce. Encodings are built by hand here with the wire codes the planner
 * uses (a double literal is kind 2, a bigint literal kind 1), so the inference itself is exercised
 * independently of what the encoder currently admits.
 */
class NativeCalcOutputTypeTest {

  private static final Schema INPUT =
      new Schema(List.of(Field.nullable("v", new ArrowType.Int(32, true))));

  @Test
  void reportsTheTypeEachProjectionCompilesTo() {
    Inference inference =
        infer(
            new int[] {2, 0}, new int[] {0, 0}, new int[] {0, 0}, new long[0],
            new double[] {1.5}, new int[] {0, 1}, -1, new String[] {"lit", "v"});
    assertNull(inference.failure);
    assertEquals(
        List.of(
            Field.nullable("lit", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Field.nullable("v", new ArrowType.Int(32, true))),
        inference.schema.getFields());
  }

  @Test
  void rejectsANonBooleanCondition() {
    Inference inference =
        infer(
            new int[] {1}, new int[] {0}, new int[] {0}, new long[] {1L}, new double[0],
            new int[0], 0, new String[0]);
    assertNull(inference.schema);
    assertTrue(inference.failure.contains("condition"), inference.failure);
    assertTrue(inference.failure.contains("Int64"), inference.failure);
  }

  private static final class Inference {
    final Schema schema;
    final String failure;

    Inference(Schema schema, String failure) {
      this.schema = schema;
      this.failure = failure;
    }
  }

  private static Inference infer(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      int[] projectionRoots,
      int conditionRoot,
      String[] outputNames) {
    try (BufferAllocator allocator = new RootAllocator();
        ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
        ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, INPUT, null, inputSchema);
      String failure =
          Native.inferCalcOutputSchema(
              kinds,
              payload,
              childCounts,
              longs,
              doubles,
              new String[0],
              projectionRoots,
              conditionRoot,
              outputNames,
              inputSchema.memoryAddress(),
              outputSchema.memoryAddress());
      Schema schema =
          failure == null ? Data.importSchema(allocator, outputSchema, null) : null;
      return new Inference(schema, failure);
    }
  }
}
