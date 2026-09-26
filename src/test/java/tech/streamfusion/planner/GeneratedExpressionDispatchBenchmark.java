package tech.streamfusion.planner;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.NativeUdf;

/** Includes Arrow import/export and argument/result conversion; excludes SQL/source/sink work. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class GeneratedExpressionDispatchBenchmark {
  private static volatile long consumed;

  @Test
  void generatedVersusReflectiveBatchDispatch() throws Exception {
    var mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    mx.setThreadAllocatedMemoryEnabled(true);
    long thread = Thread.currentThread().getId();
    int[] ids = {
      GeneratedExpressionDispatchTest.register(false),
      GeneratedExpressionDispatchTest.register(true)
    };
    try {
      for (int rows : new int[] {1, 128, 1024}) {
        double[][] elapsed = new double[2][5];
        double[][] allocations = new double[2][5];
        try (var input = GeneratedExpressionDispatchTest.input(rows)) {
          for (int trial = -3; trial < 5; trial++) {
            for (int order = 0; order < 2; order++) {
              int variant = (order + Math.max(trial, 0)) % 2;
              long bytes = mx.getThreadAllocatedBytes(thread);
              long start = System.nanoTime();
              for (int batch = 0; batch < 500; batch++) {
                try (var output = GeneratedExpressionDispatchTest.invoke(ids[variant], input)) {
                  consumed += output.getRowCount();
                }
              }
              double nanos = (System.nanoTime() - start) / 500.0;
              double allocated = (mx.getThreadAllocatedBytes(thread) - bytes) / 500.0;
              if (trial >= 0) {
                elapsed[variant][trial] = nanos;
                allocations[variant][trial] = allocated;
              }
            }
          }
        }
        for (int variant = 0; variant < 2; variant++) {
          Arrays.sort(elapsed[variant]);
          Arrays.sort(allocations[variant]);
          System.out.printf(
              "GENERATED_BRIDGE rows=%d direct=%s nsPerBatch=%.1f allocatedBytesPerBatch=%.1f%n",
              rows, variant == 1, elapsed[variant][2], allocations[variant][2]);
        }
      }
    } finally {
      for (int id : ids) NativeUdf.unregister(id);
    }
  }
}
