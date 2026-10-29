package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeIcebergNestedTest {
  @TempDir Path temp;

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3})
  void structsListsMapsRoundTripWithNullsAndNestedTemporalValues(int version) throws Exception {
    var table = table("nested-" + version, version);
    write(table);
    var expected = binaryRows(table.schema(), rows());
    assertEquals(expected, stockRows(table));
    assertEquals(expected, nativeRows(table));
    try (var files = table.newScan().includeColumnStats().planFiles()) {
      var file = files.iterator().next().file();
      assertEquals(1L, file.nanValueCounts().get(table.schema().findField("profile.f").fieldId()));
      assertEquals(
          1L,
          file.nanValueCounts()
              .get(table.schema().findField("items.element.vals.element").fieldId()));
      assertTrue(file.lowerBounds().containsKey(table.schema().findField("profile.n").fieldId()));
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.api.Test
  void nestedFieldIdsSurviveRenameReorderPromotionAndMaintenance() throws Exception {
    var table = table("evolution", 3);
    write(table);
    table
        .updateSchema()
        .renameColumn("profile.label", "display")
        .updateColumn("profile.n", Types.LongType.get())
        .addColumn("profile", "added", Types.StringType.get())
        .updateColumn("attrs.value.n", Types.LongType.get())
        .addColumn("attrs.value", "extra", Types.StringType.get())
        .renameColumn("items.element.name", "label")
        .addColumn("items.element", "extra", Types.IntegerType.get())
        .moveFirst("profile.label")
        .commit();
    var expected = stockRows(table);
    assertEquals(expected, nativeRows(table));
    var projection = table.schema().select("id", "profile.display", "profile.n", "attrs.value.n");
    List<org.apache.iceberg.expressions.Expression> filters =
        List.of(org.apache.iceberg.expressions.Expressions.greaterThan("id", 1));
    var projected = stockRows(table, projection, filters);
    assertEquals(1, projected.size());
    assertEquals(projected, nativeRows(table, projection, filters));
    assertFalse(
        NativeIcebergMaintenance.rewriteDataFiles(
                environment(), table, Map.of("rewrite-all", "true"), false)
            .isEmpty());
    table.refresh();
    assertEquals(expected, stockRows(table));
    assertEquals(expected, nativeRows(table));
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.api.Test
  void nestedSortKeysRespectNullParentsAndKeepPayloads() throws Exception {
    var table = table("nested-sort", 3);
    write(table);
    table.replaceSortOrder().desc("profile.n", org.apache.iceberg.NullOrder.NULLS_FIRST).commit();
    var input = rows();
    var expected = binaryRows(table.schema(), List.of(input.get(1), input.get(2), input.get(0)));
    NativeIcebergMaintenance.rewriteDataFiles(
        environment(), table, Map.of("rewrite-all", "true"), true);
    table.refresh();
    assertEquals(expected, stockRows(table));
    assertEquals(expected, nativeRows(table));
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) assertEquals(table.sortOrder().orderId(), task.file().sortOrderId());
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "none,2", "hash,2", "range,2", "none,3", "hash,3", "range,3"
  })
  void nestedPayloadsKeepPartitionRoutingAndRewrite(String distribution, int version)
      throws Exception {
    var schema = schema();
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.builderFor(schema).identity("id").build(),
                Map.of("format-version", Integer.toString(version)),
                temp.resolve("partitioned").toString());
    if (distribution.equals("range")) table.replaceSortOrder().asc("id").commit();
    write(table, Map.of("distribution-mode", distribution));
    var expected = binaryRows(schema, rows());
    assertEquals(multiset(expected), multiset(stockRows(table)));
    assertEquals(multiset(expected), multiset(nativeRows(table)));
    NativeIcebergSqlTest.assertNativeDataFiles(table, "nested partitioned " + distribution);
    NativeIcebergMaintenance.rewriteDataFiles(
        environment(), table, Map.of("rewrite-all", "true"), false);
    table.refresh();
    assertEquals(multiset(expected), multiset(stockRows(table)));
    assertEquals(multiset(expected), multiset(nativeRows(table)));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {2, 3})
  void nestedPayloadsKeepHostUpsertDeleteSemantics(int version) throws Exception {
    var original = schema();
    var keyed = new Schema(original.columns(), java.util.Set.of(1));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                keyed,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", Integer.toString(version)),
                temp.resolve("keyed").toString());
    write(table, Map.of("distribution-mode", "hash", "upsert-enabled", "true"));
    write(table, Map.of("distribution-mode", "hash", "upsert-enabled", "true"));
    var expected = binaryRows(keyed, rows());
    assertEquals(multiset(expected), multiset(stockRows(table)));
    assertEquals(multiset(expected), multiset(nativeRows(table)));
    NativeIcebergSqlTest.assertNativeDataFiles(table, "nested upsert payload");
  }

  @org.junit.jupiter.api.Test
  void unverifiedNestedContractsAreRejectedBeforeExecution() throws Exception {
    var table = table("admission", 3);
    var options = Map.of("streaming", "true");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergSourcePlan.validate(
                table,
                new Configuration(),
                options,
                table.schema(),
                List.of(org.apache.iceberg.expressions.Expressions.equal("profile.n", 10))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergSourcePlan.validate(
                table,
                new Configuration(),
                options,
                table.schema(),
                List.of(org.apache.iceberg.expressions.Expressions.isNull("profile"))));
    table
        .updateProperties()
        .set(
            org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING,
            org.apache.iceberg.mapping.NameMappingParser.toJson(
                org.apache.iceberg.mapping.MappingUtil.create(table.schema())))
        .commit();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergSourcePlan.validate(
                table, new Configuration(), options, table.schema(), List.of()));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRewriteFiles.validateTable(table, false));
    table
        .updateProperties()
        .remove(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING)
        .commit();
    table.updateSpec().addField("profile.n").commit();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergSourcePlan.validate(
                table, new Configuration(), options, table.schema(), List.of()));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRewriteFiles.validateTable(table, false));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {2, 3})
  void nestedPayloadsKeepPositionDeletesAndVectorsDuringRewrite(int version) throws Exception {
    var table = table("nested-position", version);
    write(table);
    String path;
    try (var tasks = table.newScan().planFiles()) {
      path = tasks.iterator().next().file().location();
    }
    var delta = table.newRowDelta();
    if (version == 3) {
      var writer =
          new org.apache.iceberg.deletes.BaseDVFileWriter(
              org.apache.iceberg.io.OutputFileFactory.builderFor(table, 0, 0)
                  .format(org.apache.iceberg.FileFormat.PUFFIN)
                  .build(),
              ignored -> null);
      try (writer) {
        writer.delete(path, 2, table.spec(), null);
      }
      writer.result().deleteFiles().forEach(delta::addDeletes);
    } else {
      var factory =
          new org.apache.iceberg.data.GenericFileWriterFactory.Builder(table)
              .deleteFileFormat(org.apache.iceberg.FileFormat.PARQUET)
              .build();
      var writer =
          factory.newPositionDeleteWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/positions.parquet")),
              table.spec(),
              null);
      try (writer) {
        writer.write(
            org.apache.iceberg.deletes.PositionDelete.<org.apache.iceberg.data.Record>create()
                .set(path, 2));
      }
      delta.addDeletes(writer.toDeleteFile());
    }
    delta.commit();
    var expected = stockRows(table);
    assertEquals(2, expected.size());
    assertEquals(multiset(expected), multiset(nativeRows(table)));
    NativeIcebergMaintenance.rewriteDataFiles(
        environment(), table, Map.of("rewrite-all", "true"), false);
    table.refresh();
    assertEquals(multiset(expected), multiset(stockRows(table)));
    assertEquals(multiset(expected), multiset(nativeRows(table)));
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private Table table(String name, int version) {
    return new HadoopTables(new org.apache.hadoop.conf.Configuration())
        .create(
            schema(), PartitionSpec.unpartitioned(),
            Map.of("format-version", Integer.toString(version)), temp.resolve(name).toString());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "2,false,false",
    "2,true,true",
    "3,false,true",
    "3,true,false"
  })
  void nestedEqualityKeysKeepNullMatchingAndLaterInserts(
      int version, boolean composite, boolean sort) throws Exception {
    var table = table("nested-equality", version);
    write(table);
    var ids =
        composite
            ? new int[] {
              table.schema().findField("id").fieldId(),
              table.schema().findField("profile.n").fieldId()
            }
            : new int[] {table.schema().findField("profile.n").fieldId()};
    var keys =
        composite ? table.schema().select("id", "profile.n") : table.schema().select("profile.n");
    var factory =
        new org.apache.iceberg.data.GenericFileWriterFactory.Builder(table)
            .dataFileFormat(org.apache.iceberg.FileFormat.PARQUET)
            .deleteFileFormat(org.apache.iceberg.FileFormat.PARQUET)
            .equalityFieldIds(ids)
            .equalityDeleteRowSchema(keys)
            .build();
    var writer =
        factory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/equalities.parquet")),
            table.spec(),
            null);
    try (writer) {
      for (int id = 0; id < 2; id++) {
        var row = org.apache.iceberg.data.GenericRecord.create(keys);
        if (composite) row.setField("id", id);
        var profile =
            org.apache.iceberg.data.GenericRecord.create(keys.findType("profile").asStructType());
        profile.setField("n", 10);
        row.setField("profile", id == 0 ? profile : null);
        writer.write(row);
      }
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    assertEquals(1, stockRows(table).size());
    assertEquals(stockRows(table), nativeRows(table));
    var idProjection = table.schema().select("id");
    assertEquals(
        binaryRows(idProjection, List.of(GenericRowData.of(2))),
        nativeRows(table, idProjection, List.of()));
    write(table);
    var expected = stockRows(table);
    assertEquals(4, expected.size());
    assertEquals(multiset(expected), multiset(nativeRows(table)));
    assertEquals(multiset(expected), multiset(nativeRows(table, table.schema(), List.of(), true)));
    var lineage = org.apache.iceberg.MetadataColumns.schemaWithRowLineage(table.schema());
    var expectedLineage =
        version == 3 ? stockRows(table, lineage, List.of()) : List.<BinaryRowData>of();
    if (sort) table.replaceSortOrder().asc("id").commit();
    NativeIcebergMaintenance.rewriteDataFiles(
        environment(), table, Map.of("rewrite-all", "true"), sort);
    table.refresh();
    assertEquals(multiset(expected), multiset(stockRows(table)));
    assertEquals(multiset(expected), multiset(nativeRows(table)));
    if (version == 3) {
      assertEquals(multiset(expectedLineage), multiset(stockRows(table, lineage, List.of())));
      assertEquals(multiset(expectedLineage), multiset(nativeRows(table, lineage, List.of())));
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static Map<BinaryRowData, Long> multiset(List<BinaryRowData> rows) {
    return rows.stream()
        .collect(
            java.util.stream.Collectors.groupingBy(
                row -> row, java.util.stream.Collectors.counting()));
  }

  private static Schema schema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(
            2,
            "profile",
            Types.StructType.of(
                Types.NestedField.required(3, "n", Types.IntegerType.get()),
                Types.NestedField.optional(4, "label", Types.StringType.get()),
                Types.NestedField.optional(5, "f", Types.DoubleType.get()),
                Types.NestedField.optional(6, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(7, "clock", Types.TimeType.get()))),
        Types.NestedField.optional(
            8,
            "items",
            Types.ListType.ofOptional(
                9,
                Types.StructType.of(
                    Types.NestedField.optional(10, "name", Types.StringType.get()),
                    Types.NestedField.optional(
                        11, "vals", Types.ListType.ofOptional(12, Types.DoubleType.get()))))),
        Types.NestedField.optional(
            13,
            "attrs",
            Types.MapType.ofOptional(
                14,
                15,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.optional(16, "n", Types.IntegerType.get()),
                    Types.NestedField.optional(17, "clock", Types.TimeType.get())))));
  }

  static List<RowData> rows() {
    var values = new LinkedHashMap<Object, Object>();
    values.put(StringData.fromString("a"), GenericRowData.of(42, 123));
    values.put(StringData.fromString("b"), null);
    return List.of(
        GenericRowData.of(
            0,
            GenericRowData.of(
                10,
                StringData.fromString("α🐍"),
                Double.NaN,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("1969-12-31T23:59:59.999999")),
                1),
            new GenericArrayData(
                new Object[] {
                  GenericRowData.of(
                      StringData.fromString("x"),
                      new GenericArrayData(new Object[] {Double.NaN, null, 1.0})),
                  null,
                  GenericRowData.of(null, new GenericArrayData(new Object[0]))
                }),
            new GenericMapData(values)),
        GenericRowData.of(1, null, null, null),
        GenericRowData.of(
            2,
            GenericRowData.of(
                20,
                null,
                -0.0,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("9999-12-31T23:59:59.999999")),
                86_399_999),
            new GenericArrayData(new Object[0]),
            new GenericMapData(Map.of())));
  }

  private static StreamExecutionEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
    env.setParallelism(1);
    return env;
  }

  private static void write(Table table) throws Exception {
    write(table, Map.of("distribution-mode", "none"));
  }

  private static void write(Table table, Map<String, String> options) throws Exception {
    var type = FlinkSchemaUtil.convert(table.schema());
    var env = environment();
    var input =
        env.fromSequence(0, 0)
            .map(
                ignored ->
                    new ArrowBatch(
                        RowDataArrowConverter.write(rows(), type, NativeAllocator.SHARED)))
            .returns(ArrowBatchTypeInformation.INSTANCE);
    NativeIcebergSink.append(
        input, table, TableLoader.fromHadoopTable(table.location()), new Configuration(), options);
    env.execute("iceberg-nested-data-files");
    table.refresh();
  }

  private static List<BinaryRowData> binaryRows(Schema schema, List<RowData> rows) {
    var serializer = new RowDataSerializer(FlinkSchemaUtil.convert(schema));
    return rows.stream().map(row -> serializer.toBinaryRow(row).copy()).toList();
  }

  private static List<BinaryRowData> stockRows(Table table) throws Exception {
    return stockRows(table, table.schema(), List.of());
  }

  private static List<BinaryRowData> stockRows(
      Table table, Schema projection, List<org.apache.iceberg.expressions.Expression> filters)
      throws Exception {
    var result = new ArrayList<BinaryRowData>();
    var serializer = new RowDataSerializer(FlinkSchemaUtil.convert(projection));
    try (var tasks = table.newScan().planFiles();
        var input =
            new DataIterator<>(
                new RowDataFileScanTaskReader(table.schema(), projection, null, false, filters),
                new BaseCombinedScanTask(
                    java.util.stream.StreamSupport.stream(tasks.spliterator(), false).toList()),
                table.io(),
                table.encryption())) {
      while (input.hasNext()) result.add(serializer.toBinaryRow(input.next()).copy());
    }
    return result;
  }

  private static List<BinaryRowData> nativeRows(Table table) throws Exception {
    return nativeRows(table, table.schema(), List.of());
  }

  private static List<BinaryRowData> nativeRows(
      Table table, Schema projection, List<org.apache.iceberg.expressions.Expression> filters)
      throws Exception {
    return nativeRows(table, projection, filters, false);
  }

  private static List<BinaryRowData> nativeRows(
      Table table,
      Schema projection,
      List<org.apache.iceberg.expressions.Expression> filters,
      boolean rewrite)
      throws Exception {
    var result = new ArrayList<BinaryRowData>();
    var type = FlinkSchemaUtil.convert(projection);
    var serializer = new RowDataSerializer(type);
    var requested =
        rewrite && org.apache.iceberg.TableUtil.supportsRowLineage(table)
            ? org.apache.iceberg.MetadataColumns.schemaWithRowLineage(projection)
            : projection;
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var input =
            new IcebergNativeTaskReader(
                task,
                table.schema(),
                requested.columns().stream().map(f -> f.fieldId()).toList(),
                table.io(),
                rewrite ? 4096 : 2,
                0,
                filters,
                false,
                null,
                null,
                rewrite,
                requested)) {
          org.apache.arrow.vector.VectorSchemaRoot next;
          while ((next = input.next()) != null) {
            try (var root = next) {
              var reader = ArrowConversion.createArrowReader(root, type);
              for (int i = 0; i < root.getRowCount(); i++)
                result.add(serializer.toBinaryRow(reader.read(i)).copy());
            }
          }
        }
      }
    }
    return result;
  }
}
