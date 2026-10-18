package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;

public class NativeIcebergRangeSinkTest {
  @TempDir Path temp;
  private static final List<Counter> UPDATES = new CopyOnWriteArrayList<>();
  private static final java.util.concurrent.atomic.AtomicInteger FIRST =
      new java.util.concurrent.atomic.AtomicInteger();
  private static final java.util.concurrent.atomic.AtomicInteger SECOND =
      new java.util.concurrent.atomic.AtomicInteger();
  private static final java.util.concurrent.atomic.AtomicBoolean CONTINUE =
      new java.util.concurrent.atomic.AtomicBoolean();

  static Stream<Arguments> cases() {
    return Stream.of(1, 2, 3)
        .flatMap(format -> Stream.of("Map", "Sketch").map(type -> Arguments.of(format, type)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void javaRangeStatisticsAndRollingKeepArrowPayloads(int format, String type) throws Exception {
    UPDATES.clear();
    FIRST.set(0);
    SECOND.set(0);
    CONTINUE.set(false);
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "cluster", Types.IntegerType.get()),
            Types.NestedField.optional(3, "payload", Types.StringType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.builderFor(schema).bucket("id", 4).build(),
                Map.of(
                    "format-version", Integer.toString(format), "write.distribution-mode", "range"),
                temp.resolve("range").toUri().toString());
    table.replaceSortOrder().asc("cluster").commit();
    var configuration =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "taskmanager.memory.task.off-heap.size",
                "1 gb",
                "metrics.reporters",
                "range",
                "metrics.reporter.range.factory.class",
                WitnessFactory.class.getName()));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(2);
    env.setMaxParallelism(16);
    env.enableCheckpointing(100);
    var input =
        env.fromSequence(0, 1_000_000)
            .flatMap(new PhasedInput())
            .returns(ArrowBatchTypeInformation.INSTANCE);
    NativeIcebergSink.append(
        input,
        table,
        TableLoader.fromHadoopTable(table.location()),
        configuration,
        Map.of(
            "distribution-mode",
            "range",
            "range-distribution-statistics-type",
            type,
            "target-file-size-bytes",
            "1"),
        "range-native");
    var job = env.executeAsync("native-iceberg-range");
    boolean stopped = false;
    try {
      await(
          () ->
              FIRST.get() == 2
                  && UPDATES.size() == 2
                  && UPDATES.stream().allMatch(counter -> counter.getCount() > 0));
      CONTINUE.set(true);
      await(() -> SECOND.get() == 2);
      job.stopWithSavepoint(
              true,
              temp.resolve("savepoints").toUri().toString(),
              org.apache.flink.core.execution.SavepointFormatType.CANONICAL)
          .get(30, java.util.concurrent.TimeUnit.SECONDS);
      job.getJobExecutionResult().get(30, java.util.concurrent.TimeUnit.SECONDS);
      stopped = true;
    } finally {
      if (!stopped) {
        job.cancel().get(30, java.util.concurrent.TimeUnit.SECONDS);
        job.getJobExecutionResult()
            .handle((result, failure) -> null)
            .get(30, java.util.concurrent.TimeUnit.SECONDS);
      }
    }
    table.refresh();
    var ids = new java.util.TreeSet<Integer>();
    try (var records = IcebergGenerics.read(table).build()) {
      for (var row : records) {
        int id = (Integer) row.getField("id");
        assertTrue(ids.add(id));
        assertEquals(id % 13 == 0 ? null : (id / 100) % 7, row.getField("cluster"));
        assertEquals(
            id % 11 == 0 ? null : "payload-" + id + "-é🐍",
            row.getField("payload") == null ? null : row.getField("payload").toString());
      }
    }
    assertEquals(
        java.util.stream.IntStream.range(0, 15060)
            .boxed()
            .collect(java.util.stream.Collectors.toSet()),
        ids);
    assertTrue(
        UPDATES.stream().mapToLong(Counter::getCount).sum() > 0,
        "host checkpoint statistics must actually update the range partitioner");
    NativeIcebergSqlTest.assertNativeDataFiles(table, "range " + type);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  static void clearRangeWitnesses() {
    UPDATES.clear();
  }

  static long rangeUpdates() {
    return UPDATES.stream().mapToLong(Counter::getCount).sum();
  }

  private static void await(java.util.function.BooleanSupplier ready) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
    while (!ready.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
    assertTrue(ready.getAsBoolean(), "range input/statistics did not advance");
  }

  private static final class PhasedInput
      extends org.apache.flink.api.common.functions.RichFlatMapFunction<Long, ArrowBatch> {
    private int next = -1;
    private int step;

    @Override
    public void flatMap(Long ignored, org.apache.flink.util.Collector<ArrowBatch> output)
        throws Exception {
      if (next < 0) {
        next = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        step = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
      }
      if (next >= 60 || (next >= 30 && !CONTINUE.get())) {
        Thread.sleep(5);
        return;
      }
      Thread.sleep(50);
      var id = new IntVector("id", NativeAllocator.SHARED);
      var cluster = new IntVector("cluster", NativeAllocator.SHARED);
      var payload = new VarCharVector("payload", NativeAllocator.SHARED);
      var root = VectorSchemaRoot.of(id, cluster, payload);
      try {
        root.allocateNew();
        for (int row = 0; row < 251; row++) {
          int value = next * 251 + row;
          id.setSafe(row, value);
          if (value % 13 == 0) cluster.setNull(row);
          else cluster.setSafe(row, (value / 100) % 7);
          if (value % 11 == 0) payload.setNull(row);
          else
            payload.setSafe(
                row,
                ("payload-" + value + "-é🐍").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        root.getFieldVectors().forEach(vector -> vector.setValueCount(251));
        root.setRowCount(251);
      } catch (Throwable failure) {
        root.close();
        throw failure;
      }
      output.collect(new ArrowBatch(root));
      int before = next;
      next += step;
      if (before < 30 && next >= 30) FIRST.incrementAndGet();
      if (next >= 60) SECOND.incrementAndGet();
    }
  }

  public static final class WitnessFactory implements MetricReporterFactory {
    @Override
    public MetricReporter createMetricReporter(Properties properties) {
      return new MetricReporter() {
        public void open(MetricConfig configuration) {}

        public void close() {}

        public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
          if (name.equals("nativeRangeStatisticsUpdates")) UPDATES.add((Counter) metric);
        }

        public void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {}
      };
    }
  }
}
