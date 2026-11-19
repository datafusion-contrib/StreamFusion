package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Sink boundary: routing, RowData/Arrow conversion, JNI, merge, encoding and snapshot commit. */
class PaimonTimestampBenchmark {
  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_TIMESTAMP_BENCHMARK", matches = "true")
  void compareReleasedSink() throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "ts7", DataTypes.TIMESTAMP(7)),
                new DataField(2, "ts9", DataTypes.TIMESTAMP(9)),
                new DataField(3, "ltz8", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(8))));
    int warmup = Integer.getInteger("timestamp.warmup", 2);
    int samples = Integer.getInteger("timestamp.samples", 5);
    for (int count : new int[] {32768, 131072}) {
      List<InternalRow> input = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        long millis = 1_700_000_000_000L + i * 1001L;
        int nanos = i * 997 % 1_000_000;
        input.add(
            GenericRow.of(
                i % 16384,
                i % 11 == 0 ? null : Timestamp.fromEpochMillis(millis, nanos / 100 * 100),
                i % 7 == 0 ? null : Timestamp.fromEpochMillis(millis, nanos),
                i % 5 == 0 ? null : Timestamp.fromEpochMillis(millis, nanos / 10 * 10)));
      }
      for (String format : List.of("parquet", "orc")) {
        double[][] times = new double[2][samples];
        for (int iteration = 0; iteration < warmup + samples; iteration++) {
          List<String> expected = null;
          for (int offset = 0; offset < 2; offset++) {
            boolean nativeWriter = (iteration + offset) % 2 == 1;
            var table =
                PaimonMergeEngineTest.table(
                    Map.of("file.format", format, "write-only", "true"), type);
            try (var writer =
                new PaimonMergeEngineTest.Writer(
                    table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 4096)) {
              long start = System.nanoTime();
              writer.write(input);
              writer.commit(1);
              double elapsed = (System.nanoTime() - start) / 1e9;
              if (iteration >= warmup) times[nativeWriter ? 1 : 0][iteration - warmup] = elapsed;
            }
            if (nativeWriter) NativeAppendSinkWriteTest.assertNativeFiles(table);
            var actual = PaimonTestTables.readRows(table, type);
            if (expected == null) expected = actual;
            else assertEquals(expected, actual);
            table.fileIO().delete(table.location(), true);
          }
        }
        Arrays.sort(times[0]);
        Arrays.sort(times[1]);
        System.out.printf(
            "PAIMON_TIMESTAMP format=%s rows=%d keys=16384 warmup=%d samples=%d"
                + " stock_s=%.6f[%.6f,%.6f] native_s=%.6f[%.6f,%.6f] speedup=%.2fx%n",
            format,
            count,
            warmup,
            samples,
            times[0][samples / 2],
            times[0][0],
            times[0][samples - 1],
            times[1][samples / 2],
            times[1][0],
            times[1][samples - 1],
            times[0][samples / 2] / times[1][samples / 2]);
      }
    }
  }
}
