package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.csv.CsvFormatProvider;
import tech.streamfusion.format.json.JsonFormatProvider;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.KeyedDecodeSpec;

/**
 * Pins the keyed decode to Flink's own Kafka record deserializer, record by record: the
 * deserializer Flink's table factory builds for the table, and the split reader's native decode
 * the planner resolves from the same options, given the same consumer records. Flink pairs each
 * value row of a record with each key row, value-major; a record whose key yields no row emits
 * nothing; and under {@code value.fields-include = ALL} the value's copy of a key field wins. The
 * cases cover each way the split reader hands a batch to the native decode: the contiguous byte
 * buffer (JSON), Arrow binary input (CSV), and the buffer that carries connector metadata.
 */
@Tag("streamfusion-kafka")
class KeyedDecodeParityTest {

  @Test
  void jsonKeysMatchFlinksRecordDeserializer() throws Exception {
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k_region STRING, name STRING, k_id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k_id;k_region',"
            + " 'key.fields-prefix' = 'k_',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    String[][] values = {
      {"{\"id\":1,\"region\":\"eu\"}", "{\"name\":\"a\"}"},
      // A null or empty JSON key yields no key row, so neither record emits.
      {null, "{\"name\":\"b\"}"},
      {"", "{\"name\":\"c\"}"},
      {"{\"id\":4,\"region\":\"eu\"}", null},
      {"{\"id\":5}", "{\"name\":\"f\"}"},
      {"{\"id\":6,\"region\":\"eu\"}", "not json"},
      {"{\"id\":7,\"region\":\"eu\",\"id\":8}", "{\"name\":\"g\",\"extra\":[1]}"},
      {"{\"id\":\" 9 \",\"region\":10}", "{\"name\":\"h\"}"},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1] == null ? null : values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < physical.getFieldCount(); i++) {
              Object value =
                  RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(value == null ? null : value.toString());
            }
            expected.add(fields);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            true)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < physical.getFieldCount(); i++) {
          Object value = RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(value == null ? null : value.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(4, expected.size(), "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  // Flink 1.18 rejects top-level arrays, failing a strict key format.
  @Test
  void jsonArraysFanOutOnEitherSideOfTheKey() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k_region STRING, name STRING, k_id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k_id;k_region',"
            + " 'key.fields-prefix' = 'k_',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    String[][] values = {
      {
        "[{\"id\":2,\"region\":\"us\"},{\"id\":3,\"region\":\"ap\"}]",
        "[{\"name\":\"d\"},{\"name\":\"e\"}]"
      },
      {"[]", "{\"name\":\"f\"}"},
      {"{\"id\":4,\"region\":\"eu\"}", "[]"},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1] == null ? null : values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < physical.getFieldCount(); i++) {
              Object value =
                  RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(value == null ? null : value.toString());
            }
            expected.add(fields);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            true)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < physical.getFieldCount(); i++) {
          Object value = RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(value == null ? null : value.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(4, expected.size(), "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  // Flink 2.x collects a null key row for a null key array element, and its key/value merge trips
  // over it inside the value deserializer's catch: a skipping value keeps the pairs its first row
  // made with the key rows ahead of the null and drops the rest of the record, while a record whose
  // value yields no row never reaches it.
  @Test
  void nullKeyElementsTripFlinksMergeWhenValuesSkipErrors() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    String[][] values = {
      {"[{\"k\":1},null,{\"k\":2}]", "[{\"id\":10},{\"id\":11}]"},
      {"[null]", "{\"id\":12}"},
      {"[{\"k\":3},null]", null},
      {"[null]", "[]"},
      {"[null]", "[null]"},
      {"{\"k\":4}", "{\"id\":13}"},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1] == null ? null : values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < physical.getFieldCount(); i++) {
              Object value =
                  RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(value == null ? null : value.toString());
            }
            expected.add(fields);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            true)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < physical.getFieldCount(); i++) {
          Object value = RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(value == null ? null : value.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(List.of(List.of("1", "10"), List.of("4", "13")), expected);
    assertEquals(expected, actual);
  }

  // A strict value fails the record on its null key row, as Flink's merge does; a record whose
  // value yields no row still never reaches it.
  @Test
  void aNullKeyElementFailsLikeFlinkWhenValuesAreStrict() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    ConsumerRecord<byte[], byte[]> tombstone =
        new ConsumerRecord<>(
            "t", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
            "[{\"k\":1},null]".getBytes(StandardCharsets.UTF_8),
            null,
            new RecordHeaders(), Optional.empty());
    ConsumerRecord<byte[], byte[]> paired =
        new ConsumerRecord<>(
            "t", 0, 1L, 0L, TimestampType.CREATE_TIME, -1, -1,
            "[{\"k\":1},null]".getBytes(StandardCharsets.UTF_8),
            "{\"id\":10}".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), Optional.empty());
    List<RowData> flinkTombstoneRows = new ArrayList<>();
    Collector<RowData> tombstoneRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            flinkTombstoneRows.add(row);
          }

          @Override
          public void close() {}
        };
    Collector<RowData> pairedRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {}

          @Override
          public void close() {}
        };

    flink.deserialize(tombstone, tombstoneRows);
    assertThrows(Throwable.class, () -> flink.deserialize(paired, pairedRows));
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            false)),
                true,
                List.of(),
                -1,
                null)) {
      assertNull(nativeDecoder.decode(List.of(tombstone)).batch());
      assertThrows(RuntimeException.class, () -> nativeDecoder.decode(List.of(paired)));
    }
    assertEquals(List.of(), flinkTombstoneRows);
  }

  // A strict value's null element is a null value row, which Flink's merge fails on beside a key
  // row; a record whose key yields no row never pairs it and emits nothing.
  @Test
  void aNullValueElementFailsLikeFlinkOnlyBesideAKeyRow() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    ConsumerRecord<byte[], byte[]> keyless =
        new ConsumerRecord<>(
            "t", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
            null,
            "[{\"id\":10},null,{\"id\":11}]".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), Optional.empty());
    ConsumerRecord<byte[], byte[]> paired =
        new ConsumerRecord<>(
            "t", 0, 1L, 0L, TimestampType.CREATE_TIME, -1, -1,
            "{\"k\":1}".getBytes(StandardCharsets.UTF_8),
            "[{\"id\":10},null]".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), Optional.empty());
    List<RowData> flinkKeylessRows = new ArrayList<>();
    Collector<RowData> keylessRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            flinkKeylessRows.add(row);
          }

          @Override
          public void close() {}
        };
    Collector<RowData> pairedRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {}

          @Override
          public void close() {}
        };

    flink.deserialize(keyless, keylessRows);
    assertThrows(Throwable.class, () -> flink.deserialize(paired, pairedRows));
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            false)),
                true,
                List.of(),
                -1,
                null)) {
      assertNull(nativeDecoder.decode(List.of(keyless)).batch());
      assertThrows(RuntimeException.class, () -> nativeDecoder.decode(List.of(paired)));
    }
    assertEquals(List.of(), flinkKeylessRows);
  }

  // A null key row still counts as a key row: a strict value's null row beside it fails the
  // record in Flink's merge, and natively.
  @Test
  void aNullValueElementBesideANullKeyElementFailsLikeFlink() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    ConsumerRecord<byte[], byte[]> nullPair =
        new ConsumerRecord<>(
            "t", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
            "[null]".getBytes(StandardCharsets.UTF_8),
            "[null]".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), Optional.empty());
    List<RowData> flinkRows = new ArrayList<>();
    Collector<RowData> rows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            flinkRows.add(row);
          }

          @Override
          public void close() {}
        };

    assertThrows(Throwable.class, () -> flink.deserialize(nullPair, rows));
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            false)),
                true,
                List.of(),
                -1,
                null)) {
      assertThrows(RuntimeException.class, () -> nativeDecoder.decode(List.of(nullPair)));
    }
    assertEquals(List.of(), flinkRows);
  }

  // Where Flink's collector path collects a skipped key object as a null row, the key stays on
  // Flink (see KeyedDecodeSpecTest); elsewhere the null is dropped, and so is its record.
  @Test
  void skippingJsonKeysDropTheirRecordsLikeFlink() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        tech.streamfusion.compat.JsonRuntimeCompat.COLLECTS_SKIPPED_OBJECTS_AS_NULL);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k_region STRING, name STRING, k_id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k_id;k_region',"
            + " 'key.fields-prefix' = 'k_',"
            + " 'key.json.ignore-parse-errors' = 'true',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    String[][] values = {
      {"{\"id\":1,\"region\":\"eu\"}", "{\"name\":\"a\"}"},
      {"not json", "{\"name\":\"b\"}"},
      {"{\"id\": ", "{\"name\":\"c\"}"},
      {"[{\"id\":2,\"region\":\"us\"}]", "{\"name\":\"d\"}"},
      {"{\"id\":\"x\",\"region\":\"ap\"}", "{\"name\":\"e\"}"},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1] == null ? null : values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < physical.getFieldCount(); i++) {
              Object value =
                  RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(value == null ? null : value.toString());
            }
            expected.add(fields);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            true)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < physical.getFieldCount(); i++) {
          Object value = RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(value == null ? null : value.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(2, expected.size(), "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  @Test
  void theValueCopyOfAKeyFieldWinsUnderAll() throws Exception {
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (id BIGINT, name STRING) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'id', 'format' = 'json')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    List<ConsumerRecord<byte[], byte[]>> records =
        List.of(
            new ConsumerRecord<>(
                "t", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "{\"id\":1}".getBytes(StandardCharsets.UTF_8),
                "{\"id\":99,\"name\":\"a\"}".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(
                "t", 0, 1L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "{\"id\":2}".getBytes(StandardCharsets.UTF_8),
                "{\"name\":\"b\"}".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(), Optional.empty()));

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            expected.add(
                List.of(
                    String.valueOf(row.isNullAt(0) ? null : row.getLong(0)),
                    String.valueOf(row.isNullAt(1) ? null : row.getString(1))));
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            false)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        actual.add(
            List.of(
                String.valueOf(row.isNullAt(0) ? null : row.getLong(0)),
                String.valueOf(row.isNullAt(1) ? null : row.getString(1))));
      }
    }

    assertEquals(List.of(List.of("99", "a"), List.of("null", "b")), expected);
    assertEquals(expected, actual);
  }

  @Test
  void csvKeysMatchFlinksRecordDeserializer() throws Exception {
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (region STRING, id BIGINT, name STRING, amount BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'csv', 'key.fields' = 'id;region',"
            + " 'key.csv.field-delimiter' = '|', 'key.csv.ignore-parse-errors' = 'true',"
            + " 'value.format' = 'csv', 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    String[][] values = {
      {"1|eu", "a,10"},
      // A bad field under the key's ignore-parse-errors nulls just that field.
      {"x|us", "b,20"},
      // A null CSV key yields no key row, so the record emits nothing.
      {null, "c,30"},
      {"4|ap", null},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1] == null ? null : values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < physical.getFieldCount(); i++) {
              Object value =
                  RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(value == null ? null : value.toString());
            }
            expected.add(fields);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new CsvFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            false)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < physical.getFieldCount(); i++) {
          Object value = RowData.createFieldGetter(physical.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(value == null ? null : value.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(2, expected.size(), "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  @Test
  void rawKeysKeepEveryRecordIncludingANullKey() throws Exception {
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k STRING, id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'raw', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    String[][] values = {
      {"a", "{\"id\":1}"},
      // Raw is the one key format whose null key still yields a key row: a NULL key column.
      {null, "{\"id\":2}"},
      // Flink 1.18 rejects a top-level array value, which the lenient value format then skips.
      {"", "[{\"id\":3},{\"id\":4}]"},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            expected.add(
                java.util.Arrays.asList(
                    row.isNullAt(0) ? null : row.getString(0).toString(),
                    row.isNullAt(1) ? null : String.valueOf(row.getLong(1))));
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            true)),
                true,
                List.of(),
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, physical)) {
        actual.add(
            java.util.Arrays.asList(
                row.isNullAt(0) ? null : row.getString(0).toString(),
                row.isNullAt(1) ? null : String.valueOf(row.getLong(1))));
      }
    }

    assertEquals(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS ? 4 : 2,
        expected.size(),
        "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  @Test
  void keyedTablesCarryEachRecordsMetadata() throws Exception {
    List<String> metadataKeys = List.of("partition", "offset", "timestamp");
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k STRING, id BIGINT, part INT METADATA FROM 'partition',"
            + " off BIGINT METADATA FROM 'offset', ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')"
            + " WITH ('connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    DataType produced =
        DataTypes.ROW(
                DataTypes.FIELD("k", DataTypes.STRING()),
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("partition", readable.get("partition")),
                DataTypes.FIELD("offset", readable.get("offset")),
                DataTypes.FIELD("timestamp", readable.get("timestamp")))
            .notNull();
    ((SupportsReadingMetadata) source).applyReadableMetadata(metadataKeys, produced);
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    Map<String, String> formatOptions = new HashMap<>(spec.optionsWithMarkers());
    formatOptions.put(NativeFormatOptions.METADATA_COLUMNS, "2:partition,3:offset,4:timestamp");
    String[][] values = {
      {"{\"k\":\"a\"}", "{\"id\":1}"},
      // No key row: the record and its metadata contribute nothing.
      {null, "{\"id\":2}"},
      // Fans out on Flink 2.2; Flink 1.18 rejects the array and the lenient value format skips it.
      {"{\"k\":\"c\"}", "[{\"id\":3},{\"id\":4}]"},
      {"{\"k\":\"d\"}", null},
      {"{\"k\":\"e\"}", "{\"id\":5}"},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "t", 2, 40L + i, 1_000L * i, TimestampType.CREATE_TIME, -1, -1,
              values[i][0] == null ? null : values[i][0].getBytes(StandardCharsets.UTF_8),
              values[i][1] == null ? null : values[i][1].getBytes(StandardCharsets.UTF_8),
              new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
              Object value =
                  RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(value == null ? null : value.toString());
            }
            expected.add(fields);
          }

          @Override
          public void close() {}
        };
    for (ConsumerRecord<byte[], byte[]> record : records) {
      flink.deserialize(record, flinkRows);
    }
    List<List<String>> actual = new ArrayList<>();
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                rowType,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(), spec.valueRowType(), formatOptions, true)),
                true,
                metadataKeys,
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, rowType)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          Object value = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(value == null ? null : value.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(
        tech.streamfusion.compat.JsonRuntimeCompat.ACCEPTS_ARRAY_ROOTS ? 4 : 2,
        expected.size(),
        "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  @Test
  void aBadKeyFailsLikeFlinkWithoutIgnoreParseErrors() throws Exception {
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 't',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'json', 'key.fields' = 'k',"
            + " 'value.format' = 'json', 'value.json.ignore-parse-errors' = 'true',"
            + " 'value.fields-include' = 'EXCEPT_KEY')");
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
    RowType physical =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    KeyedDecodeSpec spec = KeyedDecodeSpec.resolve(table.getOptions(), physical);
    assertNotNull(spec);
    // The value format skips bad messages, but the key format doesn't: the bad key fails.
    List<ConsumerRecord<byte[], byte[]>> records =
        List.of(
            new ConsumerRecord<>(
                "t", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "{\"k\":\"not a number\"}".getBytes(StandardCharsets.UTF_8),
                "{\"id\":1}".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders(), Optional.empty()));
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {}

          @Override
          public void close() {}
        };

    assertThrows(Exception.class, () -> flink.deserialize(records.get(0), flinkRows));
    try (NativeKafkaBatchDecoder nativeDecoder =
            new NativeKafkaBatchDecoder(
                physical,
                new JsonFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(),
                            spec.valueRowType(),
                            spec.optionsWithMarkers(),
                            true)),
                true,
                List.of(),
                -1,
                null)) {
      assertThrows(RuntimeException.class, () -> nativeDecoder.decode(records));
    }
  }
}
