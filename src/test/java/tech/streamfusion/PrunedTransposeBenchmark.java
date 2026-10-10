package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.compat.RichMapFunction;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.RowDataToArrowOperator;
import tech.streamfusion.planner.NativePlanner;

/** Row-fed SQL diagnostic; the source deliberately has no projection pushdown. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
public class PrunedTransposeBenchmark {
  static final String QUERY = "SELECT id + 1 AS id, amount FROM src WHERE id >= 0";

  @Test
  void copyAllocationAndRetainedPayload() throws Exception {
    var mx =
        (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
    mx.setThreadAllocatedMemoryEnabled(true);
    long thread = Thread.currentThread().getId();
    var source =
        (RowType)
            org.apache.flink.table.api.DataTypes.ROW(
                    org.apache.flink.table.api.DataTypes.FIELD(
                        "id", org.apache.flink.table.api.DataTypes.INT()),
                    org.apache.flink.table.api.DataTypes.FIELD(
                        "amount", org.apache.flink.table.api.DataTypes.DECIMAL(20, 2)),
                    org.apache.flink.table.api.DataTypes.FIELD(
                        "unused_text", org.apache.flink.table.api.DataTypes.STRING()),
                    org.apache.flink.table.api.DataTypes.FIELD(
                        "unused_bytes", org.apache.flink.table.api.DataTypes.BYTES()))
                .getLogicalType();
    RowType pruned = new RowType(source.getFields().subList(0, 2));
    for (boolean binary : new boolean[] {false, true}) {
      for (int bytes : new int[] {0, 1024, 65536}) {
        var rows = new ReusingWideRows(source, binary, bytes);
        rows.initialize();
        long allocated = 0;
        long retained = 0;
        for (int trial = -3; trial < 5; trial++) {
          var operator = new RowDataToArrowOperator(pruned, 1024, false, source);
          try (var harness = new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(operator)) {
            harness.setup(new ArrowBatchSerializer());
            harness.open();
            long beforeArrow = tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory();
            long before = mx.getThreadAllocatedBytes(thread);
            for (int i = 0; i < 512; i++)
              harness.processElement(new StreamRecord<>(rows.map((long) i)));
            long used = mx.getThreadAllocatedBytes(thread) - before;
            if (trial >= 0) allocated += used;
            retained = tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory()
                - beforeArrow;
            harness.endInput();
            for (Object record : harness.getOutput()) {
              if (record instanceof StreamRecord<?>) {
                StreamRecord<?> stream = ((StreamRecord<?>) record);
                ((ArrowBatch) stream.getValue()).root().close();
              }
            }
          }
        }
        System.out.printf(
            "PRUNED_ENTRY binary=%s bytes=%d allocatedBytesPerRow=%.1f retainedArrowBytes=%d%n",
            binary, bytes, allocated / (5.0 * 512), retained);
      }
    }
  }

  @Test
  void wideRows() throws Exception {
    int rows = Integer.getInteger("prune.rows", 100_000);
    int warmup = Integer.getInteger("prune.warmup", 2);
    int runs = Integer.getInteger("prune.runs", 5);
    for (boolean binary : new boolean[] {false, true}) {
      for (int bytes : new int[] {0, 1024, 65536}) {
        var planTable = environment(binary, bytes, rows);
        planTable.executeSql(
            "CREATE TABLE sink (id INT, amount DECIMAL(20,2)) WITH ('connector'='blackhole')");
        String plan = NativePlanner.explain(planTable, "INSERT INTO sink " + QUERY);
        assertTrue(plan.contains("RowDataToArrow(fields=[2]"), plan);
        assertTrue(plan.contains("NativeCalc") && plan.contains("ArrowToRowData"), plan);
        double[][] times = new double[2][runs];
        for (int trial = -warmup; trial < runs; trial++) {
          for (int engine = 0; engine < 2; engine++) {
            boolean nativeEnabled = (engine + Math.max(0, trial)) % 2 == 1;
            var table = environment(binary, bytes, rows);
            table.executeSql(
                "CREATE TABLE sink (id INT, amount DECIMAL(20,2)) WITH ('connector'='blackhole')");
            var scan = nativeEnabled ? NativePlanner.install(table) : null;
            var result = table.executeSql("INSERT INTO sink " + QUERY);
            long millis =
                result.getJobClient().orElseThrow().getJobExecutionResult().get().getNetRuntime();
            if (nativeEnabled)
              assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
            if (trial >= 0) times[nativeEnabled ? 1 : 0][trial] = millis / 1000.0;
          }
        }
        for (double[] sample : times) Arrays.sort(sample);
        System.out.printf(
            "PRUNED binary=%s bytes=%d rows=%d host=%.6f native=%.6f%n",
            binary, bytes, rows, times[0][runs / 2], times[1][runs / 2]);
      }
    }
  }

  static TableEnvironment environment(boolean binary, int bytes, int rows) {
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.getConfig().set("parallelism.default", "1");
    table.executeSql(
        "CREATE TABLE src (id INT, amount DECIMAL(20,2), unused_text STRING, unused_bytes BYTES) "
            + "WITH ('connector'='pruned-wide-rows', 'rows'='"
            + rows
            + "', 'bytes'='"
            + bytes
            + "', 'binary'='"
            + binary
            + "')");
    return table;
  }

  public static final class Factory implements DynamicTableSourceFactory {
    @Override
    public String factoryIdentifier() {
      return "pruned-wide-rows";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
      return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
      return Set.of();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
      var options = context.getCatalogTable().getOptions();
      RowType type =
          (RowType)
              context
                  .getCatalogTable()
                  .getResolvedSchema()
                  .toPhysicalRowDataType()
                  .getLogicalType();
      return new WideRows(
          type,
          Boolean.parseBoolean(options.get("binary")),
          Integer.parseInt(options.get("bytes")),
          Integer.parseInt(options.get("rows")));
    }
  }

  private static final class WideRows implements ScanTableSource {
    private final RowType type;
    private final boolean binary;
    private final int bytes;
    private final int rows;

    private WideRows(RowType type, boolean binary, int bytes, int rows) {
      this.type = type;
      this.binary = binary;
      this.bytes = bytes;
      this.rows = rows;
    }

    public RowType type() {
      return type;
    }

    public boolean binary() {
      return binary;
    }

    public int bytes() {
      return bytes;
    }

    public int rows() {
      return rows;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
      return new DataStreamScanProvider() {
        @Override
        public boolean isBounded() {
          return true;
        }

        @Override
        public DataStream<RowData> produceDataStream(
            ProviderContext context, StreamExecutionEnvironment env) {
          return env.fromSequence(0, rows - 1L)
              .map(new ReusingWideRows(type, binary, bytes))
              .returns(InternalTypeInfo.of(type))
              .name("Reusing full-width rows");
        }
      };
    }

    @Override
    public DynamicTableSource copy() {
      return this;
    }

    @Override
    public String asSummaryString() {
      return "ReusingWideRows";
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      WideRows that = (WideRows) other;
      return java.util.Objects.equals(type, that.type)
          && binary == that.binary
          && bytes == that.bytes
          && rows == that.rows;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(type);
      result = 31 * result + Boolean.hashCode(binary);
      result = 31 * result + bytes;
      result = 31 * result + rows;
      return result;
    }

    @Override
    public String toString() {
      return "WideRows[type="
          + type
          + ", binary="
          + binary
          + ", bytes="
          + bytes
          + ", rows="
          + rows
          + "]";
    }
  }

  private static final class ReusingWideRows extends RichMapFunction<Long, RowData> {
    private final RowType type;
    private final boolean binary;
    private final int bytes;
    private transient GenericRowData generic;
    private transient BinaryRowData encoded;

    private ReusingWideRows(RowType type, boolean binary, int bytes) {
      this.type = type;
      this.binary = binary;
      this.bytes = bytes;
    }

    @Override
    protected void initialize() {
      byte[] payload = new byte[bytes];
      Arrays.fill(payload, (byte) 'x');
      generic =
          GenericRowData.of(
              0,
              DecimalData.fromBigDecimal(new BigDecimal("123456789012345678.90"), 20, 2),
              BinaryStringData.fromBytes(payload),
              payload);
      if (binary) encoded = new RowDataSerializer(type).toBinaryRow(generic).copy();
    }

    @Override
    public RowData map(Long id) {
      if (binary) {
        encoded.setInt(0, id.intValue());
        return encoded;
      }
      generic.setField(0, id.intValue());
      return generic;
    }
  }
}
