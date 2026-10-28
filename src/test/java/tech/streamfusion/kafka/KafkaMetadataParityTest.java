package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.json.CanalJsonFormatProvider;
import tech.streamfusion.format.json.JsonFormatProvider;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Pins the native decode of Kafka connector metadata to Flink's own record deserializer, record by
 * record: the deserializer the Kafka table factory builds for the same table, given the same
 * consumer records. Records are built directly, so the cases no modern broker produces are covered
 * too — a {@code NoTimestampType} record and one without a leader epoch. Header maps are compared
 * in their internal entry order, which Flink's {@code HashMap} fixes and downstream map functions
 * observe.
 */
@Tag("streamfusion-kafka")
class KafkaMetadataParityTest {

  @Test
  void connectorMetadataMatchesFlinksRecordDeserializer() throws Exception {
    List<String> keys =
        List.of(
            "topic", "partition", "headers", "leader-epoch", "offset", "timestamp",
            "timestamp-type");
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (id BIGINT, name STRING, tp STRING METADATA FROM 'topic',"
            + " part INT METADATA FROM 'partition',"
            + " hdrs MAP<STRING, BYTES> METADATA FROM 'headers',"
            + " epoch INT METADATA FROM 'leader-epoch', off BIGINT METADATA FROM 'offset',"
            + " ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',"
            + " tst STRING METADATA FROM 'timestamp-type') WITH ('connector' = 'kafka',"
            + " 'topic' = 'orders', 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')");
    ObjectIdentifier identifier = ObjectIdentifier.of("default_catalog", "default_database", "t");
    ResolvedCatalogTable table =
        (ResolvedCatalogTable)
            ((TableEnvironmentImpl) tEnv)
                .getCatalogManager()
                .getTable(identifier)
                .orElseThrow()
                .getResolvedTable();
    DynamicTableSource source =
        FactoryUtil.createDynamicTableSource(
            null,
            identifier,
            table,
            Map.of(),
            new Configuration(),
            Thread.currentThread().getContextClassLoader(),
            false);
    Map<String, DataType> readable = ((SupportsReadingMetadata) source).listReadableMetadata();
    List<DataTypes.Field> fields = new ArrayList<>();
    fields.add(DataTypes.FIELD("id", DataTypes.BIGINT()));
    fields.add(DataTypes.FIELD("name", DataTypes.STRING()));
    for (String key : keys) {
      fields.add(DataTypes.FIELD(key, readable.get(key)));
    }
    DataType produced = DataTypes.ROW(fields.toArray(new DataTypes.Field[0])).notNull();
    ((SupportsReadingMetadata) source).applyReadableMetadata(keys, produced);
    DataStream<?> stream =
        ((DataStreamScanProvider)
                ((ScanTableSource) source)
                    .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
            .produceDataStream(
                name -> Optional.empty(), StreamExecutionEnvironment.getExecutionEnvironment());
    Source<?, ?, ?> kafkaSource =
        ((SourceTransformation<?, ?, ?>) stream.getTransformation()).getSource();
    Field schemaField = KafkaSource.class.getDeclaredField("deserializationSchema");
    schemaField.setAccessible(true);
    @SuppressWarnings("unchecked")
    KafkaRecordDeserializationSchema<RowData> flink =
        (KafkaRecordDeserializationSchema<RowData>) schemaField.get(kafkaSource);
    flink.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(
                Thread.currentThread().getContextClassLoader());
          }
        });
    RowType rowType = (RowType) produced.getLogicalType();
    RowType valueType = RowType.of(rowType.getTypeAt(0), rowType.getTypeAt(1));
    NativeKafkaBatchDecoder nativeDecoder =
        new NativeKafkaBatchDecoder(
            rowType,
            new JsonFormatProvider()
                .createDecoder(
                    new NativeFormatContext(
                        valueType,
                        valueType,
                        Map.of(
                            "connector", "kafka",
                            "format", "json",
                            NativeFormatOptions.METADATA_COLUMNS,
                                "2:topic,3:partition,4:headers,5:leader-epoch,6:offset,"
                                    + "7:timestamp,8:timestamp-type"),
                        false)),
            false,
            keys,
            -1,
            null);
    BiFunction<String, String, Header> header =
        (key, value) ->
            new RecordHeader(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    List<Header> manyHeaders = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      manyHeaders.add(header.apply("attribute-" + i, "v" + i));
    }
    manyHeaders.add(header.apply("attribute-7", "again"));
    List<ConsumerRecord<byte[], byte[]>> records =
        List.of(
            new ConsumerRecord<>(
                "orders", 3, 10L, 1_000L, TimestampType.CREATE_TIME, -1, -1, null,
                "{\"id\":1,\"name\":\"a\"}".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(
                    List.of(
                        header.apply("trace", "t1"),
                        header.apply("tenant", null),
                        header.apply("trace", "t2"),
                        header.apply("source", "s"))),
                Optional.of(5)),
            new ConsumerRecord<>(
                "orders", 3, 11L, -1L, TimestampType.NO_TIMESTAMP_TYPE, -1, -1, null,
                "{\"id\":2,\"name\":null}".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(),
                Optional.empty()),
            new ConsumerRecord<>(
                "orders", 3, 12L, 2_000L, TimestampType.LOG_APPEND_TIME, -1, -1, null, null,
                new RecordHeaders(List.of(header.apply("dropped", "x"))),
                Optional.of(6)),
            new ConsumerRecord<>(
                "orders", 3, 13L, 3_000L, TimestampType.LOG_APPEND_TIME, -1, -1, null,
                "{\"id\":3,\"name\":\"d\"}".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(manyHeaders),
                Optional.of(6)));

    List<RowData> flinkRows = new ArrayList<>();
    Collector<RowData> collector =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            flinkRows.add(row);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, collector);
    }
    Function<RowData, List<Object>> render =
        row -> {
          List<Object> values = new ArrayList<>();
          for (int i = 0; i < rowType.getFieldCount(); i++) {
            Object value = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
            if (value instanceof MapData) {
              MapData map = (MapData) value;
              List<String> entries = new ArrayList<>();
              for (int entry = 0; entry < map.size(); entry++) {
                ArrayData mapValues = map.valueArray();
                entries.add(
                    map.keyArray().getString(entry)
                        + "="
                        + (mapValues.isNullAt(entry)
                            ? "null"
                            : StringUtils.byteToHexString(mapValues.getBinary(entry))));
              }
              values.add(entries);
            } else {
              values.add(value == null ? null : value.toString());
            }
          }
          return values;
        };
    List<List<Object>> expected = new ArrayList<>();
    for (RowData row : flinkRows) {
      expected.add(render.apply(row));
    }
    List<List<Object>> actual = new ArrayList<>();
    try (nativeDecoder;
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, rowType)) {
        actual.add(render.apply(row));
      }
    }

    assertEquals(3, expected.size(), "Flink emits a row per message, none for the tombstone");
    assertEquals(expected, actual);
  }

  @Test
  void canalIncludeFiltersKeepMetadataWithTheRowsTheyAdmit() throws Exception {
    List<String> keys = List.of("offset", "timestamp");
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE c (id BIGINT, name STRING, off BIGINT METADATA FROM 'offset',"
            + " ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp') WITH ('connector' = 'kafka',"
            + " 'topic' = 'binlog', 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'format' = 'canal-json',"
            + " 'canal-json.table.include' = 'orders')");
    ObjectIdentifier identifier = ObjectIdentifier.of("default_catalog", "default_database", "c");
    ResolvedCatalogTable table =
        (ResolvedCatalogTable)
            ((TableEnvironmentImpl) tEnv)
                .getCatalogManager()
                .getTable(identifier)
                .orElseThrow()
                .getResolvedTable();
    DynamicTableSource source =
        FactoryUtil.createDynamicTableSource(
            null,
            identifier,
            table,
            Map.of(),
            new Configuration(),
            Thread.currentThread().getContextClassLoader(),
            false);
    Map<String, DataType> readable = ((SupportsReadingMetadata) source).listReadableMetadata();
    DataType produced =
        DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("offset", readable.get("offset")),
                DataTypes.FIELD("timestamp", readable.get("timestamp")))
            .notNull();
    ((SupportsReadingMetadata) source).applyReadableMetadata(keys, produced);
    DataStream<?> stream =
        ((DataStreamScanProvider)
                ((ScanTableSource) source)
                    .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
            .produceDataStream(
                name -> Optional.empty(), StreamExecutionEnvironment.getExecutionEnvironment());
    Source<?, ?, ?> kafkaSource =
        ((SourceTransformation<?, ?, ?>) stream.getTransformation()).getSource();
    Field schemaField = KafkaSource.class.getDeclaredField("deserializationSchema");
    schemaField.setAccessible(true);
    @SuppressWarnings("unchecked")
    KafkaRecordDeserializationSchema<RowData> flink =
        (KafkaRecordDeserializationSchema<RowData>) schemaField.get(kafkaSource);
    flink.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(
                Thread.currentThread().getContextClassLoader());
          }
        });
    RowType rowType = (RowType) produced.getLogicalType();
    RowType valueType = RowType.of(rowType.getTypeAt(0), rowType.getTypeAt(1));
    NativeKafkaBatchDecoder nativeDecoder =
        new NativeKafkaBatchDecoder(
            rowType,
            new CanalJsonFormatProvider()
                .createDecoder(
                    new NativeFormatContext(
                        valueType,
                        valueType,
                        Map.of(
                            "connector", "kafka",
                            "format", "canal-json",
                            "canal-json.table.include", "orders",
                            NativeFormatOptions.METADATA_COLUMNS, "2:offset,3:timestamp"),
                        false)),
            false,
            keys,
            -1,
            null);
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    String[] messages = {
      "{\"data\":[{\"id\":1,\"name\":\"a\"}],\"table\":\"orders\",\"type\":\"INSERT\"}",
      "{\"data\":[{\"id\":2,\"name\":\"b\"}],\"table\":\"users\",\"type\":\"INSERT\"}",
      "{\"data\":[{\"id\":1,\"name\":\"a2\"}],\"old\":[{\"name\":\"a\"}],"
          + "\"table\":\"orders\",\"type\":\"UPDATE\"}",
      "{\"data\":[{\"id\":3,\"name\":\"c\"},{\"id\":4,\"name\":\"d\"}],"
          + "\"table\":\"orders\",\"type\":\"INSERT\"}",
    };
    for (int i = 0; i < messages.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "binlog", 0, 20L + i, 1_000L * (i + 1), TimestampType.CREATE_TIME, -1, -1, null,
              messages[i].getBytes(StandardCharsets.UTF_8), new RecordHeaders(),
              Optional.of(1)));
    }

    List<RowData> flinkRows = new ArrayList<>();
    Collector<RowData> collector =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            flinkRows.add(row);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, collector);
    }
    Function<RowData, List<Object>> render =
        row -> {
          List<Object> values = new ArrayList<>();
          values.add(row.getRowKind().shortString());
          for (int i = 0; i < rowType.getFieldCount(); i++) {
            Object value = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
            values.add(value == null ? null : value.toString());
          }
          return values;
        };
    List<List<Object>> expected = new ArrayList<>();
    for (RowData row : flinkRows) {
      expected.add(render.apply(row));
    }
    List<List<Object>> actual = new ArrayList<>();
    try (nativeDecoder;
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, rowType)) {
        actual.add(render.apply(row));
      }
    }

    assertEquals(5, expected.size(), "the users row drops; the update and the array fan out");
    assertEquals(expected, actual);
  }
}
