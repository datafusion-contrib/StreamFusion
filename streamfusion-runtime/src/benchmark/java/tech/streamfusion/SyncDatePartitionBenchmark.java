package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.ZoneId;
import java.util.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.operator.*;
import tech.streamfusion.planner.NativePlanner;

/**
 * Prepared RowData/Arrow comparison; native admission timing is in examples/sync_date_partition.rs.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SyncDatePartitionBenchmark {
  private static volatile long consumed;
  static final String EXPRESSIONS =
      "SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 0, 8) AS ds, "
          + "SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 9, 2) AS hh, "
          + "LPAD(CAST((CAST(SUBSTR(FROM_UNIXTIME(n, 'yyyyMMddHHmm'), 11, 2) AS INT) "
          + "/ 15) * 15 AS STRING), 2, '0') AS mm";

  @Test
  void preparedDatePartitions() throws Exception {
    int count = Integer.getInteger("sync.rows", 131072);
    int width = Integer.getInteger("sync.width", 0);
    int repeat = Integer.getInteger("sync.repeat", 1);
    int warmups = Integer.getInteger("sync.warmups", 5);
    int runs = Integer.getInteger("sync.runs", 9);
    int batchSize = Integer.getInteger("sync.batchRows", 1024);
    String mode = System.getProperty("sync.mode", "stock");
    assertTrue(count > 0 && width >= 0 && repeat > 0 && runs > 0 && batchSize > 0);
    var fields = new ArrayList<org.apache.flink.table.api.DataTypes.Field>();
    fields.add(FIELD("n", BIGINT()));
    for (int i = 0; i < width; i++) fields.add(FIELD("c" + i, STRING()));
    var type = ROW(fields.toArray(new org.apache.flink.table.api.DataTypes.Field[0]));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    table.getConfig().set("table.exec.legacy-cast-behaviour", "enabled");
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            env.fromElements(Row.withPositions(width + 1)).returns(ExternalTypeInfo.<Row>of(type)),
            Schema.newBuilder().fromRowDataType(type).build()));
    var stock =
        findCalc(
            table
                .toChangelogStream(table.sqlQuery("SELECT *, " + EXPRESSIONS + " FROM src"))
                .getTransformation());
    var inputType = ((InternalTypeInfo<?>) stock.getInputs().get(0).getOutputType()).toRowType();
    var outputType = ((InternalTypeInfo<?>) stock.getOutputType()).toRowType();
    var scan = NativePlanner.install(table);
    var nativeCalc =
        findCalc(
            table
                .toChangelogStream(table.sqlQuery("SELECT *, " + EXPRESSIONS + " FROM src"))
                .getTransformation());
    assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
    if (!mode.equals("previous")) {
      assertTrue(
          scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
          scan.explainSummary());
    }
    System.out.println("[sync-date-route] " + scan.explainSummary());
    var serializer = new RowDataSerializer(inputType);
    var rows = new ArrayList<RowData>(count);
    for (int i = 0; i < count; i++) {
      var row = new GenericRowData(width + 1);
      if (i % 19 != 0) row.setField(0, 1730000000L + (i / repeat) % 86400);
      for (int j = 0; j < width; j++)
        if ((i + j) % 23 != 0)
          row.setField(j + 1, StringData.fromString("field-" + j + "-value-" + i % 127));
      rows.add(serializer.toBinaryRow(row).copy());
    }
    try (var arrow = RowDataArrowConverter.write(rows, inputType, NativeAllocator.SHARED)) {
      String export = System.getProperty("sync.export");
      if (export != null) {
        try (var channel =
                FileChannel.open(
                    Path.of(export),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            var writer = new ArrowFileWriter(arrow, null, channel)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
        Object operator =
            ((SimpleOperatorFactory<?>) nativeCalc.getOperatorFactory()).getOperator();
        var program = new LinkedHashMap<String, Object>();
        for (String name :
            List.of(
                "kinds",
                "payload",
                "childCounts",
                "longs",
                "doubles",
                "strings",
                "projectionRoots",
                "conditionRoot",
                "outputNames")) {
          Field field = NativeCalcOperator.class.getDeclaredField(name);
          field.setAccessible(true);
          program.put(name, field.get(operator));
        }
        new ObjectMapper().writeValue(Path.of(export + ".plan.json").toFile(), program);
      }
      boolean nativeEngine = !mode.equals("stock");
      var selected = nativeEngine ? nativeCalc : stock;
      byte[] factory = InstantiationUtil.serializeObject(selected.getOperatorFactory());
      Long expected = null;
      for (int trial = 0; trial < warmups + runs; trial++) {
        var digest = new Digest(width + 1);
        try (var harness = harness(factory, outputType, nativeEngine, digest)) {
          if (nativeEngine)
            harness.processElement(
                new StreamRecord<>(new ArrowBatch(arrow.slice(0, Math.min(count, batchSize)))));
          else
            for (int i = 0; i < Math.min(count, batchSize); i++)
              harness.processElement(new StreamRecord<>(rows.get(i)));
          digest.hash = 0;
          digest.count = 0;
          var prepared = new ArrayList<StreamRecord<Object>>();
          if (nativeEngine)
            for (int start = 0; start < count; start += batchSize)
              prepared.add(
                  new StreamRecord<>(
                      new ArrowBatch(arrow.slice(start, Math.min(batchSize, count - start)))));
          else for (var row : rows) prepared.add(new StreamRecord<>(row));
          long clock = System.nanoTime();
          for (var record : prepared) harness.processElement(record);
          double seconds = (System.nanoTime() - clock) / 1e9;
          consumed ^= digest.hash;
          assertEquals(count, digest.count);
          if (expected == null) expected = digest.hash;
          else assertEquals(expected.longValue(), digest.hash);
          System.out.printf(
              Locale.ROOT,
              "[sync-date-trial] mode=%s width=%d repeat=%d rows=%d trial=%d warmup=%s seconds=%.9f"
                  + " checksum=%d%n",
              mode,
              width,
              repeat,
              count,
              trial,
              trial < warmups,
              seconds,
              digest.hash);
        }
      }
    }
  }

  private static OneInputTransformation<?, ?> findCalc(Transformation<?> root) {
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(root);
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      if (node instanceof OneInputTransformation && node.getName().contains("Calc"))
        return (OneInputTransformation<?, ?>) node;
      pending.addAll(node.getInputs());
    }
    throw new AssertionError("Calc missing");
  }

  private static final class Digest {
    final int start;
    final boolean discard = Boolean.getBoolean("sync.discard");
    long hash;
    int count;

    Digest(int start) {
      this.start = start;
    }

    void add(RowData row) {
      if (discard) {
        count++;
        return;
      }
      for (int field = start; field < start + 3; field++) {
        long value = -1;
        if (!row.isNullAt(field)) {
          value = 0;
          for (byte b : row.getString(field).toBytes()) value = value * 31 + (b & 255);
        }
        hash += value;
      }
      count++;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static OneInputStreamOperatorTestHarness<Object, Object> harness(
      byte[] factory, RowType outputType, boolean nativeEngine, Digest digest) throws Exception {
    StreamOperatorFactory operator =
        InstantiationUtil.deserializeObject(
            factory, SyncDatePartitionBenchmark.class.getClassLoader());
    OneInputStreamOperatorTestHarness harness =
        new OneInputStreamOperatorTestHarness(operator, 128, 1, 0);
    harness.setup(nativeEngine ? new ArrowBatchSerializer() : new RowDataSerializer(outputType));
    Field field = AbstractStreamOperator.class.getDeclaredField("output");
    field.setAccessible(true);
    field.set(
        harness.getOperator(),
        new FlinkOutput<Object>(null) {
          @Override
          public void collect(StreamRecord<Object> record) {
            if (!nativeEngine) digest.add((RowData) record.getValue());
            else
              try (var root = ((ArrowBatch) record.getValue()).root()) {
                if (digest.discard) {
                  digest.count += root.getRowCount();
                  return;
                }
                var reader = ArrowConversion.createArrowReader(root, outputType);
                for (int i = 0; i < root.getRowCount(); i++) digest.add(reader.read(i));
              }
          }
        });
    harness.open();
    return harness;
  }
}
