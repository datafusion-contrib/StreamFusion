package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.*;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ScalarKeyAdmissionBenchmark {
  private static volatile long consumed;

  static Stream<String> types() {
    return FlinkScalarKeySqlHarnessTest.types();
  }

  @ParameterizedTest
  @MethodSource("types")
  void releasedKeyProjectionAndHashOnPreparedRows(String type) throws Exception {
    int count = Integer.getInteger("scalar.keys.rows", 262144);
    int keys = Integer.getInteger("scalar.keys.cardinality", 4096);
    int width = Integer.getInteger("scalar.keys.width", 64);
    int warmups = Integer.getInteger("scalar.keys.warmups", 5);
    int runs = Integer.getInteger("scalar.keys.runs", 9);
    var logical =
        DataTypes.ROW(
            DataTypes.FIELD("k", FlinkScalarKeySqlHarnessTest.keyType(type)),
            DataTypes.FIELD("shard", DataTypes.INT()),
            DataTypes.FIELD("v", DataTypes.BIGINT()));
    var schema = (RowType) logical.getLogicalType();
    var serializer = new RowDataSerializer(schema);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            Thread.currentThread().getContextClassLoader(),
            new int[] {0, 1},
            InternalTypeInfo.of(schema));
    var input = new ArrayList<RowData>(count);
    for (int i = 0; i < count; i++)
      input.add(
          serializer
              .toBinaryRow(
                  GenericRowData.of(
                      i % 11 == 0 ? null : internalKey(type, i % keys, keys, width),
                      i % 3,
                      (long) i))
              .copy());
    String export = System.getProperty("scalar.keys.export");
    if (export != null) {
      Path path = Path.of(export + "-" + type + ".arrow");
      try (var root = RowDataArrowConverter.write(input, schema, NativeAllocator.SHARED);
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
      ByteBuffer hashes = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
      for (RowData row : input) hashes.putInt(selector.getKey(row).hashCode());
      Files.write(Path.of(path + ".hashes.bin"), hashes.array());
    }
    Long expected = null;
    for (int trial = 0; trial < warmups + runs; trial++) {
      long hash = 0;
      long start = System.nanoTime();
      for (RowData row : input) hash += selector.getKey(row).hashCode();
      consumed ^= hash;
      double seconds = (System.nanoTime() - start) / 1e9;
      if (expected == null) expected = hash;
      else assertEquals(expected.longValue(), hash);
      System.out.printf(
          Locale.ROOT,
          "[scalar-key-admission] type=%s trial=%d warmup=%s rows=%d keys=%d width=%d seconds=%.9f"
              + " checksum=%d%n",
          type,
          trial,
          trial < warmups,
          count,
          keys,
          width,
          seconds,
          hash);
    }
  }

  private static Object internalKey(String type, int id, int keys, int width) {
    Object value = key(type, id, keys, width);
    if (value instanceof LocalTime) return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000);
    if (value instanceof Period) return (int) ((Period) value).toTotalMonths();
    if (value instanceof Duration) return ((Duration) value).toMillis();
    return value;
  }

  private static Object key(String type, int id, int keys, int width) {
    switch (type) {
      case "float":
        return id % 13 == 0
            ? (Float) FlinkScalarKeySqlHarnessTest.key(type, id)
            : (float) (id - keys / 2) * .25f;
      case "double":
        return id % 13 == 0
            ? (Double) FlinkScalarKeySqlHarnessTest.key(type, id)
            : (id - keys / 2) * .25;
      case "tinyint":
        return (byte) (id - 128);
      case "smallint":
        return (short) (id - keys / 2);
      case "time":
      case "time0":
        return LocalTime.ofNanoOfDay((id * 1001L % 86_400_000) * 1_000_000L);
      case "months":
        return Period.ofMonths(id - keys / 2);
      case "millis":
        return Duration.ofMillis((id - keys / 2) * 1001L);
      case "binary":
      case "varbinary":
        byte[] bytes = new byte[id % 17 == 0 ? 0 : width + id % 3];
        for (int j = 0; j < bytes.length; j++) bytes[j] = (byte) (id >>> (j % 4 * 8));
        return bytes;
      default:
        throw new IllegalArgumentException(type);
    }
  }
}
