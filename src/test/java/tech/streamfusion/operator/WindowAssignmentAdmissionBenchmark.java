package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.compat.FlinkOutput;

/** Java/RowData gate paired with the Rust/Arrow window_assignment example. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowAssignmentAdmissionBenchmark {
  @Test
  void consumeReleasedAssignmentOnPreparedRowsAndExportMatchingArrow() throws Exception {
    int count = Integer.getInteger("assignment.rows", 131072);
    int warmups = Integer.getInteger("assignment.warmup", 3);
    int runs = Integer.getInteger("assignment.runs", 7);
    long offset = Long.getLong("assignment.offsetMillis", -1000L);
    assertTrue(count > 0 && warmups > 0 && runs >= 3);
    var type = RowType.of(new BigIntType(), new TimestampType(3));
    var outputType =
        RowType.of(
            new BigIntType(),
            new TimestampType(3),
            new TimestampType(3),
            new TimestampType(3),
            new TimestampType(3));
    List<RowData> rows = new ArrayList<>();
    List<StreamRecord<RowData>> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RowData row =
          GenericRowData.of(
              (long) i,
              i % 7 == 0 ? null : TimestampData.fromEpochMillis(-12000L + i % 4096 * 10L));
      rows.add(row);
      records.add(new StreamRecord<>(row));
    }
    for (String shape : System.getProperty("assignment.shapes", "TUMBLE,HOP,CUMULATE").split(",")) {
      Path path =
          Path.of(
              System.getProperty("assignment.filePrefix", "target/assignment-admission")
                  + "-"
                  + shape
                  + ".arrow");
      Files.createDirectories(path.toAbsolutePath().getParent());
      try (var allocator = new RootAllocator();
          var root = RowDataArrowConverter.write(rows, type, allocator);
          var channel =
              FileChannel.open(
                  path,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING,
                  StandardOpenOption.WRITE);
          var writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      for (int trial = 0; trial < warmups + runs; trial++) {
        long[] digest = new long[2];
        try (var harness = new OneInputStreamOperatorTestHarness<>(operator(shape, offset))) {
          harness.setup(new RowDataSerializer(outputType));
          var output = AbstractStreamOperator.class.getDeclaredField("output");
          output.setAccessible(true);
          output.set(
              harness.getOperator(),
              new FlinkOutput<RowData>(null) {
                @Override
                public void collect(StreamRecord<RowData> record) {
                  RowData row = record.getValue();
                  digest[0]++;
                  digest[1] += row.getLong(0);
                  for (int c = 1; c < 5; c++) digest[1] += row.getTimestamp(c, 3).getMillisecond();
                }
              });
          harness.open();
          long start = System.nanoTime();
          for (var record : records) harness.processElement(record);
          double seconds = (System.nanoTime() - start) / 1e9;
          assertTrue(digest[0] > 0);
          System.out.printf(
              Locale.ROOT,
              "[assignment-admission] shape=%s offset=%d input_rows=%d trial=%d warmup=%s"
                  + " seconds=%.9f output_rows=%d checksum=%d%n",
              shape,
              offset,
              count,
              trial,
              trial < warmups,
              seconds,
              digest[0],
              digest[1]);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static OneInputStreamOperator<RowData, RowData> operator(String shape, long offset)
      throws Exception {
    String prefix;
    Class<?> operator;
    try {
      operator =
          Class.forName(
              "org.apache.flink.table.runtime.operators.window.tvf.operator.AlignedWindowTableFunctionOperator");
      prefix = "org.apache.flink.table.runtime.operators.window.groupwindow.assigners.";
    } catch (ClassNotFoundException oldRelease) {
      operator =
          Class.forName(
              "org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator");
      prefix = "org.apache.flink.table.runtime.operators.window.assigners.";
    }
    String name =
        shape.equals("TUMBLE")
            ? "TumblingWindowAssigner"
            : shape.equals("HOP") ? "SlidingWindowAssigner" : "CumulativeWindowAssigner";
    Class<?> factory = Class.forName(prefix + name);
    Object assigner =
        shape.equals("TUMBLE")
            ? factory.getMethod("of", Duration.class).invoke(null, Duration.ofSeconds(9))
            : factory
                .getMethod("of", Duration.class, Duration.class)
                .invoke(null, Duration.ofSeconds(9), Duration.ofSeconds(3));
    assigner =
        factory.getMethod("withOffset", Duration.class).invoke(assigner, Duration.ofMillis(offset));
    return (OneInputStreamOperator<RowData, RowData>)
        operator.getConstructors()[0].newInstance(assigner, 1, ZoneOffset.UTC);
  }
}
