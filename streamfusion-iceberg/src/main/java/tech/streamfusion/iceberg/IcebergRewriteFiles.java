package tech.streamfusion.iceberg;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import tech.streamfusion.operator.ArrowBatch;

/** Executes an Iceberg-selected group without changing its planning or commit contract. */
final class IcebergRewriteFiles {
  private IcebergRewriteFiles() {}

  static void validate(Table table, RewriteFileGroup group, boolean sort) {
    validateTable(table, sort);
    validatePartitionSpec(table, table.specs().get(group.outputSpecId()));
    for (var task : group.fileScanTasks()) {
      if (!IcebergArrowReaderFunction.nativeTask(task, table.schema(), List.of()))
        throw new IllegalArgumentException(
            "Unverified native rewrite file task: " + task.file().location());
    }
  }

  static void validateTable(Table table, boolean sort) {
    if (!IcebergSinkAdapter.compatible())
      throw new IllegalArgumentException("Unverified Iceberg maintenance version");
    NativeIcebergSink.validate(table, new Configuration(), java.util.Map.of());
    validatePartitionSpec(table, table.spec());
    for (var field : table.schema().columns()) {
      if (field.initialDefault() != null)
        throw new IllegalArgumentException("Non-null initial rewrite defaults are not admitted");
    }
    if (sort) {
      for (var field : table.sortOrder().fields()) {
        if (!field.transform().isIdentity())
          throw new IllegalArgumentException(
              "Native rewrite sorting requires identity sort fields");
      }
    }
  }

  private static void validatePartitionSpec(Table table, org.apache.iceberg.PartitionSpec spec) {
    for (var field : spec.fields()) {
      var type = table.schema().findType(field.sourceId());
      if (type == null)
        throw new IllegalArgumentException(
            "Output partition field is absent from the current schema");
      if (type.typeId() == org.apache.iceberg.types.Type.TypeID.TIME)
        throw new IllegalArgumentException(
            "Flink TIME partition routing cannot preserve microseconds");
    }
  }

  static Set<DataFile> rewrite(Table table, RewriteFileGroup group, int taskId, int attemptId)
      throws Exception {
    return rewrite(
        table, group, taskId, attemptId, false, 64L << 20, System.getProperty("java.io.tmpdir"));
  }

  static Set<DataFile> rewrite(
      Table table,
      RewriteFileGroup group,
      int taskId,
      int attemptId,
      boolean sort,
      long sortMemory,
      String spillDirectory)
      throws Exception {
    validate(table, group, sort);
    Schema writeSchema =
        TableUtil.supportsRowLineage(table)
            ? MetadataColumns.schemaWithRowLineage(table.schema())
            : table.schema();
    var type = FlinkSchemaUtil.convert(writeSchema);
    var factory =
        new RowDataTaskWriterFactory(
            () -> table,
            type,
            group.inputSplitSize(),
            FileFormat.PARQUET,
            table.properties(),
            null,
            false,
            writeSchema,
            table.specs().get(group.outputSpecId()));
    var conf =
        new org.apache.iceberg.flink.FlinkWriteConf(table, java.util.Map.of(), new Configuration());
    var settings =
        IcebergParquetSettings.translate(
            org.apache.iceberg.flink.sink.SinkUtil.writeProperties(
                FileFormat.PARQUET, conf, table));
    var adapter =
        new IcebergTaskWriterFactory(
            factory,
            type,
            NativeIcebergSink.dataWriterDecorator(
                SchemaParser.toJson(writeSchema),
                settings,
                org.apache.iceberg.MetricsConfig.forTable(table),
                sort ? table.sortOrder() : null),
            null);
    adapter.initialize(taskId, attemptId);
    var writer = adapter.create();
    var arrowSchema = IcebergNativeTaskReader.arrowSchema(writeSchema, true);
    try (writer) {
      try (var sorter =
          sort && table.sortOrder().isSorted()
              ? new IcebergRewriteSort(
                  arrowSchema, table.schema(), table.sortOrder(), sortMemory, spillDirectory)
              : null) {
        List<Integer> projection = writeSchema.columns().stream().map(f -> f.fieldId()).toList();
        for (var task : group.fileScanTasks()) {
          try (var reader =
              new IcebergNativeTaskReader(
                  task,
                  table.schema(),
                  projection,
                  table.io(),
                  4096,
                  0,
                  List.of(),
                  false,
                  table.properties().get(TableProperties.DEFAULT_NAME_MAPPING),
                  null,
                  true)) {
            org.apache.arrow.vector.VectorSchemaRoot root;
            while ((root = reader.next()) != null) {
              if (sorter == null) writer.write(new ArrowBatch(root));
              else sorter.push(root);
            }
          }
        }
        if (sorter != null) {
          sorter.finish();
          org.apache.arrow.vector.VectorSchemaRoot root;
          while ((root = sorter.next()) != null) writer.write(new ArrowBatch(root));
        }
        return new LinkedHashSet<>(List.of(writer.complete().dataFiles()));
      } catch (Throwable failure) {
        try {
          writer.abort();
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }
  }
}
