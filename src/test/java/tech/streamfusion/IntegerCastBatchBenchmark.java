package tech.streamfusion;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.planner.HostCastFunction;

/**
 * Same-build comparison with the existing production HostCast/JVM/Arrow boundary, not an emulation.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class IntegerCastBatchBenchmark {
  private static final int BATCHES = Integer.getInteger("cast.batches", 256);
  private static final int WARMUP = Integer.getInteger("cast.warmup", 2);
  private static final int RUNS = Integer.getInteger("cast.runs", 5);
  private static final int NULL_EVERY = Integer.getInteger("cast.nullEvery", 0);

  @Test
  void compareBoundaries() throws Exception {
    if (BATCHES < 1 || WARMUP < 0 || RUNS < 1 || NULL_EVERY < 0) {
      throw new IllegalArgumentException("Invalid cast benchmark sizes/trials");
    }
    List<String> csv =
        new ArrayList<>(List.of("cast,batch_rows,batches,null_every,engine,trial,seconds"));
    for (String size : System.getProperty("cast.batchSizes", "128,1024,4096").split(",")) {
      int rows = Integer.parseInt(size);
      if (rows < 1) throw new IllegalArgumentException("batch size must be positive");
      for (String name : List.of("STRING_INT", "INT_STRING", "INT_VARCHAR2", "ROUNDTRIP")) {
        double[][] seconds = new double[2][RUNS];
        try (Plan host = plan(name, false);
            Plan nativePlan = plan(name, true)) {
          for (int trial = 0; trial < WARMUP + RUNS; trial++) {
            for (int turn = 0; turn < 2; turn++) {
              int engine = (trial + turn) % 2;
              double elapsed = run(engine == 0 ? host : nativePlan, name, rows);
              if (trial >= WARMUP) {
                seconds[engine][trial - WARMUP] = elapsed;
                csv.add(
                    String.format(
                        Locale.ROOT,
                        "%s,%d,%d,%d,%s,%d,%.9f",
                        name,
                        rows,
                        BATCHES,
                        NULL_EVERY,
                        engine == 0 ? "host_cast" : "rust_cast",
                        trial - WARMUP,
                        elapsed));
              }
            }
          }
        }
        double host = median(seconds[0]);
        double nativeTime = median(seconds[1]);
        System.out.printf(
            Locale.ROOT,
            "[integer-cast-batch] %s batch=%d nullEvery=%d HostCast=%.6fs Rust=%.6fs"
                + " speedup=%.2fx%n",
            name,
            rows,
            NULL_EVERY,
            host,
            nativeTime,
            host / nativeTime);
      }
    }
    Path output = Path.of(System.getProperty("cast.output", "target/integer-cast-batches.csv"));
    if (output.getParent() != null) Files.createDirectories(output.getParent());
    Files.write(output, csv);
  }

  private static double run(Plan plan, String name, int rows) {
    boolean integerInput = name.startsWith("INT_");
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot input =
            VectorSchemaRoot.of(
                integerInput ? new IntVector("v", allocator) : new VarCharVector("v", allocator))) {
      String[] texts = {"0", "42", "-12345", "2147483647", "-2147483648", " +00123 ", "12.9"};
      int[] numbers = {0, 42, -12345, Integer.MAX_VALUE, Integer.MIN_VALUE, 123, 12};
      input.allocateNew();
      for (int row = 0; row < rows; row++) {
        if (NULL_EVERY > 0 && row % NULL_EVERY == 0) input.getVector(0).setNull(row);
        else if (integerInput)
          ((IntVector) input.getVector(0)).setSafe(row, numbers[row % numbers.length]);
        else
          ((VarCharVector) input.getVector(0))
              .setSafe(row, texts[row % texts.length].getBytes(StandardCharsets.UTF_8));
      }
      input.setRowCount(rows);
      long start = System.nanoTime();
      for (int batch = 0; batch < BATCHES; batch++) {
        // The Java importer consumes and closes the output C structs on every batch.
        try (ArrowArray inArray = ArrowArray.allocateNew(allocator);
            ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
          Native.calcExpression(
              plan.handle,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress());
          try (VectorSchemaRoot output =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
            if (output.getRowCount() != rows) throw new IllegalStateException("wrong batch size");
          }
        }
      }
      return (System.nanoTime() - start) / 1e9;
    }
  }

  private static Plan plan(String name, boolean nativeRun) throws Exception {
    boolean roundtrip = name.equals("ROUNDTRIP");
    boolean parse = name.equals("STRING_INT");
    int length = name.equals("INT_VARCHAR2") ? 2 : Integer.MAX_VALUE;
    List<Integer> ids = new ArrayList<>();
    try {
      int[] kinds;
      int[] payload;
      long[] literals;
      if (nativeRun) {
        kinds = roundtrip ? new int[] {31, 30, 0} : new int[] {parse ? 30 : 31, 0};
        payload = roundtrip ? new int[] {length, 0, 0} : new int[] {parse ? 0 : length, 0};
        literals = new long[0];
      } else {
        int outer = register(parse, length);
        ids.add(outer);
        kinds = roundtrip ? new int[] {17, 17, 0} : new int[] {17, 0};
        payload = roundtrip ? new int[] {0, 2, 0} : new int[] {0, 0};
        if (roundtrip) {
          int inner = register(true, length);
          ids.add(inner);
          literals = new long[] {outer, NativeUdf.TYPE_STRING, inner, NativeUdf.TYPE_INT};
        } else {
          literals = new long[] {outer, parse ? NativeUdf.TYPE_INT : NativeUdf.TYPE_STRING};
        }
      }
      int[] children = roundtrip ? new int[] {1, 1, 0} : new int[] {1, 0};
      long handle =
          Native.createCalcExpression(
              kinds,
              payload,
              children,
              literals,
              new double[0],
              new String[0],
              new int[] {0},
              -1,
              new String[] {"cast"});
      return new Plan(handle, ids);
    } catch (Throwable failure) {
      ids.forEach(NativeUdf::unregister);
      throw failure;
    }
  }

  private static int register(boolean parse, int length) throws Exception {
    HostCastFunction function =
        new HostCastFunction(
            parse ? VarCharType.STRING_TYPE : new IntType(),
            parse ? new IntType() : new VarCharType(length));
    return NativeUdf.register(
        function,
        HostCastFunction.class.getMethod("eval", Object.class),
        new int[] {parse ? NativeUdf.TYPE_STRING : NativeUdf.TYPE_INT},
        parse ? NativeUdf.TYPE_INT : NativeUdf.TYPE_STRING);
  }

  private record Plan(long handle, List<Integer> ids) implements AutoCloseable {
    public void close() {
      try {
        Native.closeCalcExpression(handle);
      } finally {
        ids.forEach(NativeUdf::unregister);
      }
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }
}
