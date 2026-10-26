package tech.streamfusion.starrocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.RowDataSerializationSchema;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Stock Flink's side of the small encoding comparison: the released connector's row serializer over
 * prebuilt binary rows, producing the UTF-8 bytes its stream-load SDK buffers. The native side is
 * timed inside Rust by the {@code starrocks_encode} Criterion suite over the same logical batches.
 * A conversion-inclusive diagnostic also times the production Java entry: Arrow export, JNI,
 * encoding and the per-row hand-off to the connector, including the SDK's UTF-8 bytes.
 */
@EnabledIfEnvironmentVariable(named = "SF_STARROCKS_BENCH", matches = "true")
class StarRocksEncodeBenchmark {
  private static final String COMMENT =
      "Unicode é and a longer payload that resembles a real description field";
  private static final RowType TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(false),
            new IntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new DecimalType(18, 2),
            new DoubleType(),
            new DateType(),
            new TimestampType(3),
            new BooleanType(),
            new VarCharType(VarCharType.MAX_LENGTH)
          },
          new String[] {
            "id", "quantity", "name", "amount", "score", "day", "event_time", "active", "comment"
          });

  @Test
  @Timeout(1800)
  void stockRowSerializationAndConversionInclusiveNativeEntry() throws Exception {
    assertEquals(
        "release",
        System.getProperty("sf.starrocks.benchmark.profile"),
        "Use the bench Maven profile");
    int trials = Integer.getInteger("sf.starrocks.benchmark.trials", 7);
    var lines = new ArrayList<String>();
    lines.add("format,mode,rows,engine,trial,nanos_per_row");
    for (String format : List.of("csv", "json"))
      for (boolean upsert : List.of(false, true))
        for (int rows : List.of(1024, 8192)) {
          List<RowData> input = rows(rows, upsert);
          StarRocksSinkOptions options = options(format, upsert);
          RowDataSerializationSchema stock = stock(options);
          TableSchema.Builder schema = TableSchema.builder();
          Map<String, StarRocksDataType> targets = new HashMap<>();
          for (int i = 0; i < TYPE.getFieldCount(); i++) {
            String name = TYPE.getFieldNames().get(i);
            schema.field(name, TypeConversions.fromLogicalToDataType(TYPE.getTypeAt(i)));
            targets.put(name, StarRocksDataType.VARCHAR);
          }
          StarRocksEncoding encoding = StarRocksEncoding.of(options, schema.build(), TYPE, targets);
          try (BufferAllocator allocator = new RootAllocator();
              NativeStarRocksEncoder encoder =
                  new NativeStarRocksEncoder(encoding, new UnregisteredMetricsGroup());
              VectorSchemaRoot prebuilt =
                  RowDataArrowConverter.write(input, TYPE, allocator, true)) {
            ArrowBatch shared = new ArrowBatch(prebuilt);
            shared.shareAcross(Integer.MAX_VALUE);
            var engines =
                new ArrayList<String>(
                    List.of("stock-java-serializer", "native-jni-entry", "native-from-rowdata"));
            Map<String, Work> work =
                Map.of(
                    "stock-java-serializer", () -> stockBytes(stock, input),
                    "native-jni-entry", () -> nativeBytes(encoder, shared),
                    "native-from-rowdata",
                        () ->
                            nativeBytes(
                                encoder,
                                new ArrowBatch(
                                    RowDataArrowConverter.write(input, TYPE, allocator, true))));
            long expected = stockBytes(stock, input);
            for (String engine : engines) assertEquals(expected, work.get(engine).run(), engine);
            String label = format + "," + (upsert ? "upsert" : "append") + "," + rows;
            long batches = Math.max(1, 2_000_000 / rows);
            for (int warmup = 0; warmup < 3; warmup++)
              for (String engine : engines) time(work.get(engine), batches);
            for (int trial = 0; trial < trials; trial++) {
              java.util.Collections.rotate(engines, 1);
              for (String engine : engines)
                lines.add(
                    label
                        + ","
                        + engine
                        + ","
                        + trial
                        + ","
                        + time(work.get(engine), batches) / ((double) batches * rows));
            }
          }
        }
    Path output =
        Path.of(System.getProperty("sf.starrocks.benchmark.output", "target/starrocks-benchmark"));
    Files.createDirectories(output);
    Files.write(output.resolve("encode-java.csv"), lines);
    lines.forEach(System.out::println);
  }

  private interface Work {
    long run() throws Exception;
  }

  private static double time(Work work, long batches) throws Exception {
    long sink = 0;
    long started = System.nanoTime();
    for (long batch = 0; batch < batches; batch++) sink += work.run();
    long elapsed = System.nanoTime() - started;
    assertTrue(sink > 0);
    return elapsed;
  }

  /** The native rows of a batch as the bytes its stream-load SDK writes into a load body. */
  private static long nativeBytes(NativeStarRocksEncoder encoder, ArrowBatch batch)
      throws Exception {
    long bytes = 0;
    encoder.begin(batch);
    try {
      for (StarRocksRowData row; (row = encoder.next()) != null; )
        bytes += row.getRow().getBytes(StandardCharsets.UTF_8).length + 1;
    } finally {
      encoder.end();
    }
    return bytes - 1;
  }

  /** The stock serializer's rows as the bytes its stream-load SDK writes into a load body. */
  private static long stockBytes(RowDataSerializationSchema stock, List<RowData> rows) {
    long bytes = 0;
    for (RowData row : rows) {
      StarRocksRowData serialized = stock.serialize(row);
      if (serialized != null)
        bytes += serialized.getRow().getBytes(StandardCharsets.UTF_8).length + 1;
    }
    return bytes - 1;
  }

  private static List<RowData> rows(int count, boolean upsert) {
    var serializer = new RowDataSerializer(TYPE);
    var rows = new ArrayList<RowData>(count);
    for (int row = 0; row < count; row++) {
      GenericRowData generic =
          GenericRowData.of(
              (long) row,
              row % 11 == 0 ? null : row * 7,
              row % 13 == 0 ? null : StringData.fromString("customer-" + row),
              DecimalData.fromBigDecimal(BigDecimal.valueOf(row * 1_234L - 50_000L, 2), 18, 2),
              row / 3.0,
              19_000 + row % 400,
              TimestampData.fromEpochMillis(1_700_000_000_000L + row * 1_337L),
              row % 2 == 0,
              row % 3 == 0 ? null : StringData.fromString(COMMENT));
      if (upsert) generic.setRowKind(RowKind.values()[row % 4]);
      rows.add(serializer.toBinaryRow(generic).copy());
    }
    return rows;
  }

  private static StarRocksSinkOptions options(String format, boolean upsert) {
    StarRocksSinkOptions options =
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
            .withProperty("load-url", "127.0.0.1:8030")
            .withProperty("database-name", "db")
            .withProperty("table-name", "t")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", format)
            .build();
    if (upsert) options.enableUpsertDelete();
    return options;
  }

  private static RowDataSerializationSchema stock(StarRocksSinkOptions options) {
    String[] names = TYPE.getFieldNames().toArray(String[]::new);
    Map<String, StarRocksDataType> targets = new HashMap<>();
    Arrays.stream(names).forEach(name -> targets.put(name, StarRocksDataType.VARCHAR));
    TableSchema.Builder schema = TableSchema.builder();
    for (int i = 0; i < names.length; i++)
      schema.field(names[i], TypeConversions.fromLogicalToDataType(TYPE.getTypeAt(i)));
    var transformer = new StarRocksTableRowTransformer(InternalTypeInfo.of(TYPE));
    transformer.setStarRocksColumns(targets);
    transformer.setTableSchema(schema.build());
    var serialization =
        new RowDataSerializationSchema(
            "db",
            "t",
            options.supportUpsertDelete(),
            options.getIgnoreUpdateBefore(),
            StarRocksSerializerFactory.createSerializer(options, names),
            transformer);
    serialization.open(null, null);
    return serialization;
  }
}
