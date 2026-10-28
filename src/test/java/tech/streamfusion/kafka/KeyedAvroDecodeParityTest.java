package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatOptions;
import tech.streamfusion.format.avro.AvroFormatProvider;
import tech.streamfusion.format.avroconfluent.AvroConfluentFormatProvider;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.planner.KeyedDecodeSpec;

/**
 * Pins keyed tables with Avro keys to Flink's own Kafka record deserializer, record by record,
 * through the split reader's native decode. Confluent-framed keys and values each fetch their
 * writer schemas from the same local registry (the one read endpoint, {@code GET
 * /schemas/ids/<id>}) under producer record names the reader resolves by field name, with an
 * evolved value schema mid-batch. A null key yields no key row for Avro, so its record emits
 * nothing, while raw keeps a NULL key column; a null value emits nothing.
 */
@Tag("streamfusion-avro-confluent")
class KeyedAvroDecodeParityTest {

  private static final Schema KEY =
      SchemaBuilder.record("OrderKey")
          .namespace("com.example")
          .fields()
          .requiredLong("id")
          .requiredString("region")
          .endRecord();

  private static final Schema VALUE_V1 =
      SchemaBuilder.record("Order")
          .namespace("com.example")
          .fields()
          .requiredString("name")
          .requiredString("extra")
          .endRecord();

  private static final Schema VALUE_V2 =
      SchemaBuilder.record("OrderV2")
          .namespace("com.example")
          .fields()
          .requiredString("name")
          .endRecord();

  private static final int KEY_ID = 1;
  private static final int VALUE_V1_ID = 2;
  private static final int VALUE_V2_ID = 3;

  private static HttpServer registry;

  @BeforeAll
  static void startRegistry() throws Exception {
    Map<Integer, Schema> schemas = Map.of(KEY_ID, KEY, VALUE_V1_ID, VALUE_V1, VALUE_V2_ID, VALUE_V2);
    registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    registry.createContext(
        "/schemas/ids/",
        exchange -> {
          int id =
              Integer.parseInt(
                  exchange.getRequestURI().getPath().substring("/schemas/ids/".length()));
          Schema schema = schemas.get(id);
          if (schema == null) {
            exchange.sendResponseHeaders(404, -1);
            return;
          }
          String quoted = schema.toString().replace("\\", "\\\\").replace("\"", "\\\"");
          byte[] body = ("{\"schema\":\"" + quoted + "\"}").getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    registry.start();
  }

  @AfterAll
  static void stopRegistry() {
    registry.stop(0);
  }

  @Test
  void confluentKeysMatchFlinksRecordDeserializer() throws Exception {
    String url = "http://localhost:" + registry.getAddress().getPort();
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k_region STRING, name STRING, k_id BIGINT) WITH ("
            + " 'connector' = 'kafka', 'topic' = 'orders',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'avro-confluent', 'key.avro-confluent.url' = '" + url + "',"
            + " 'key.fields' = 'k_id;k_region', 'key.fields-prefix' = 'k_',"
            + " 'value.format' = 'avro-confluent', 'value.avro-confluent.url' = '" + url + "',"
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
    BiFunction<Integer, GenericRecord, byte[]> framed =
        (id, record) -> {
          try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(id).array());
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
          } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        };
    Function<Object[], GenericRecord> key =
        fields -> {
          GenericRecord record = new GenericData.Record(KEY);
          record.put("id", fields[0]);
          record.put("region", fields[1]);
          return record;
        };
    GenericRecord a = new GenericData.Record(VALUE_V1);
    a.put("name", "a");
    a.put("extra", "dropped");
    GenericRecord b = new GenericData.Record(VALUE_V1);
    b.put("name", "b");
    b.put("extra", "dropped");
    GenericRecord c = new GenericData.Record(VALUE_V2);
    c.put("name", "c");
    byte[][][] values = {
      {framed.apply(KEY_ID, key.apply(new Object[] {1L, "eu"})), framed.apply(VALUE_V1_ID, a)},
      // A null Avro key yields no key row, so the record emits nothing.
      {null, framed.apply(VALUE_V1_ID, b)},
      {framed.apply(KEY_ID, key.apply(new Object[] {2L, "us"})), null},
      {framed.apply(KEY_ID, key.apply(new Object[] {3L, "ap"})), framed.apply(VALUE_V2_ID, c)},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "orders", 0, i, 0L, TimestampType.CREATE_TIME, -1, -1, values[i][0], values[i][1],
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
                new AvroConfluentFormatProvider()
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

    assertEquals(
        List.of(List.of("eu", "a", "1"), List.of("ap", "c", "3")), expected, "Flink's rows");
    assertEquals(expected, actual);
  }

  @Test
  void confluentKeyedTablesCarryEachRecordsMetadata() throws Exception {
    String url = "http://localhost:" + registry.getAddress().getPort();
    List<String> metadataKeys = List.of("offset", "timestamp");
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (id BIGINT, region STRING, name STRING,"
            + " off BIGINT METADATA FROM 'offset', ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp')"
            + " WITH ('connector' = 'kafka', 'topic' = 'orders',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'key.format' = 'avro-confluent', 'key.avro-confluent.url' = '" + url + "',"
            + " 'key.fields' = 'id;region',"
            + " 'value.format' = 'avro-confluent', 'value.avro-confluent.url' = '" + url + "',"
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
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("region", DataTypes.STRING()),
                DataTypes.FIELD("name", DataTypes.STRING()),
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
    formatOptions.put(NativeFormatOptions.METADATA_COLUMNS, "3:offset,4:timestamp");
    BiFunction<Integer, GenericRecord, byte[]> framed =
        (id, record) -> {
          try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(id).array());
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
          } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        };
    GenericRecord key = new GenericData.Record(KEY);
    key.put("id", 7L);
    key.put("region", "eu");
    GenericRecord value = new GenericData.Record(VALUE_V2);
    value.put("name", "a");
    byte[][][] values = {
      {framed.apply(KEY_ID, key), framed.apply(VALUE_V2_ID, value)},
      {null, framed.apply(VALUE_V2_ID, value)},
      {framed.apply(KEY_ID, key), framed.apply(VALUE_V2_ID, value)},
    };
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      records.add(
          new ConsumerRecord<>(
              "orders", 0, 20L + i, 5_000L + i, TimestampType.LOG_APPEND_TIME, -1, -1,
              values[i][0], values[i][1], new RecordHeaders(), Optional.empty()));
    }

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
              Object field =
                  RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
              fields.add(field == null ? null : field.toString());
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
                new AvroConfluentFormatProvider()
                    .createDecoder(
                        new NativeFormatContext(
                            spec.valueRowType(), spec.valueRowType(), formatOptions, false)),
                true,
                metadataKeys,
                -1,
                null);
        VectorSchemaRoot root = nativeDecoder.decode(records).batch().root()) {
      for (RowData row : RowDataArrowConverter.read(root, rowType)) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          Object field = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
          fields.add(field == null ? null : field.toString());
        }
        actual.add(fields);
      }
    }

    assertEquals(2, expected.size(), "Flink's rows: " + expected);
    assertEquals(expected, actual);
  }

  @Test
  void bareAvroKeysMatchFlinksRecordDeserializer() throws Exception {
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, name STRING) WITH ("
            + " 'connector' = 'kafka', 'topic' = 'orders',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'key.format' = 'avro', 'key.fields' = 'k',"
            + " 'value.format' = 'avro', 'value.fields-include' = 'EXCEPT_KEY')");
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
    // Bare Avro keys and values are written with the schemas Flink derives from their fields.
    Schema keySchema =
        AvroSchemaConverter.convertToSchema(
            RowType.of(
                    new org.apache.flink.table.types.logical.LogicalType[] {
                      new org.apache.flink.table.types.logical.BigIntType()
                    },
                    new String[] {"k"})
                .copy(false));
    Schema valueSchema = AvroSchemaConverter.convertToSchema(spec.valueRowType().copy(false));
    Function<GenericRecord, byte[]> datum =
        record -> {
          try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
          } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        };
    GenericRecord keyOne = new GenericData.Record(keySchema);
    keyOne.put("k", 1L);
    GenericRecord keyWithoutValue = new GenericData.Record(keySchema);
    keyWithoutValue.put("k", null);
    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("name", "a");
    List<ConsumerRecord<byte[], byte[]>> records =
        List.of(
            new ConsumerRecord<>(
                "orders", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
                datum.apply(keyOne), datum.apply(value), new RecordHeaders(), Optional.empty()),
            new ConsumerRecord<>(
                "orders", 0, 1L, 0L, TimestampType.CREATE_TIME, -1, -1,
                datum.apply(keyWithoutValue), datum.apply(value), new RecordHeaders(),
                Optional.empty()),
            // A null bare-Avro key yields no key row, so the record emits nothing.
            new ConsumerRecord<>(
                "orders", 0, 2L, 0L, TimestampType.CREATE_TIME, -1, -1,
                null, datum.apply(value), new RecordHeaders(), Optional.empty()));

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            expected.add(
                java.util.Arrays.asList(
                    row.isNullAt(0) ? null : String.valueOf(row.getLong(0)),
                    row.isNullAt(1) ? null : row.getString(1).toString()));
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
                new AvroFormatProvider()
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
            java.util.Arrays.asList(
                row.isNullAt(0) ? null : String.valueOf(row.getLong(0)),
                row.isNullAt(1) ? null : row.getString(1).toString()));
      }
    }

    assertEquals(java.util.Arrays.asList(List.of("1", "a"), java.util.Arrays.asList(null, "a")), expected, "Flink's rows");
    assertEquals(expected, actual);
  }

  @Test
  void rawKeysOverConfluentValuesMatchFlinksRecordDeserializer() throws Exception {
    String url = "http://localhost:" + registry.getAddress().getPort();
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
    tEnv.executeSql(
        "CREATE TABLE t (k STRING, name STRING) WITH ("
            + " 'connector' = 'kafka', 'topic' = 'orders',"
            + " 'properties.bootstrap.servers' = 'localhost:9092',"
            + " 'scan.startup.mode' = 'earliest-offset', 'key.format' = 'raw', 'key.fields' = 'k',"
            + " 'value.format' = 'avro-confluent', 'value.avro-confluent.url' = '" + url + "',"
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
    GenericRecord value = new GenericData.Record(VALUE_V2);
    value.put("name", "b");
    ByteArrayOutputStream framed = new ByteArrayOutputStream();
    framed.write(0);
    framed.write(ByteBuffer.allocate(4).putInt(VALUE_V2_ID).array());
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(framed, null);
    new GenericDatumWriter<GenericRecord>(VALUE_V2).write(value, encoder);
    encoder.flush();
    List<ConsumerRecord<byte[], byte[]>> records =
        List.of(
            new ConsumerRecord<>(
                "orders", 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "k1".getBytes(StandardCharsets.UTF_8), framed.toByteArray(), new RecordHeaders(),
                Optional.empty()),
            // Raw keeps the record with a NULL key column.
            new ConsumerRecord<>(
                "orders", 0, 1L, 0L, TimestampType.CREATE_TIME, -1, -1,
                null, framed.toByteArray(), new RecordHeaders(), Optional.empty()));

    List<List<String>> expected = new ArrayList<>();
    Collector<RowData> flinkRows =
        new Collector<>() {
          @Override
          public void collect(RowData row) {
            expected.add(
                java.util.Arrays.asList(
                    row.isNullAt(0) ? null : String.valueOf(row.getString(0)),
                    row.isNullAt(1) ? null : row.getString(1).toString()));
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
                new AvroConfluentFormatProvider()
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
            java.util.Arrays.asList(
                row.isNullAt(0) ? null : String.valueOf(row.getString(0)),
                row.isNullAt(1) ? null : row.getString(1).toString()));
      }
    }

    assertEquals(java.util.Arrays.asList(List.of("k1", "b"), java.util.Arrays.asList(null, "b")), expected, "Flink's rows");
    assertEquals(expected, actual);
  }
}
