package tech.streamfusion.iceberg;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;

/**
 * Stock Iceberg IO; sorting uses the same generated comparator and external sorter as Flink SQL.
 */
final class IcebergStockRewrite {
  private static final ConcurrentHashMap<String, Ordering> ORDERINGS = new ConcurrentHashMap<>();

  static final class Statistics {
    long spillFiles;
    long spillBytes;
  }

  static Set<DataFile> rewrite(
      Table table,
      RewriteFileGroup group,
      boolean sort,
      long sortMemory,
      String spillDirectory,
      Statistics statistics)
      throws Exception {
    var schema =
        org.apache.iceberg.TableUtil.supportsRowLineage(table)
            ? MetadataColumns.schemaWithRowLineage(table.schema())
            : table.schema();
    var type = FlinkSchemaUtil.convert(schema);
    var factory =
        new RowDataTaskWriterFactory(
            () -> table,
            type,
            group.inputSplitSize(),
            FileFormat.PARQUET,
            table.properties(),
            null,
            false,
            schema,
            table.specs().get(group.outputSpecId()));
    factory.initialize(0, 0);
    if (sort)
      IcebergSinkAdapter.field(RowDataTaskWriterFactory.class, "fileWriterFactory")
          .set(
              factory,
              new org.apache.iceberg.flink.sink.FlinkFileWriterFactory.Builder(table)
                  .dataFileFormat(FileFormat.PARQUET)
                  .dataSchema(schema)
                  .dataFlinkType(type)
                  .writerProperties(table.properties())
                  .dataSortOrder(table.sortOrder())
                  .build());
    var writer = factory.create();
    try (writer;
        var input =
            new DataIterator<>(
                new RowDataFileScanTaskReader(table.schema(), schema, null, false, List.of()),
                new BaseCombinedScanTask(group.fileScanTasks()),
                table.io(),
                table.encryption())) {
      try {
        if (sort) {
          var key =
              org.apache.iceberg.SchemaParser.toJson(schema)
                  + org.apache.iceberg.SortOrderParser.toJson(table.sortOrder());
          var order = ORDERINGS.computeIfAbsent(key, ignored -> new Ordering(table, type));
          var memory = MemoryManagerBuilder.newBuilder().setMemorySize(sortMemory).build();
          try (var io = new IOManagerAsync(spillDirectory)) {
            var sorter =
                new BinaryExternalSorter(
                    new Object(),
                    memory,
                    sortMemory,
                    io,
                    new RowDataSerializer(type),
                    new BinaryRowDataSerializer(type.getFieldCount()),
                    order.normalized.newInstance(IcebergStockRewrite.class.getClassLoader()),
                    order.comparator.newInstance(IcebergStockRewrite.class.getClassLoader()),
                    128,
                    org.apache.flink.table.api.config.ExecutionConfigOptions
                        .TABLE_EXEC_SPILL_COMPRESSION_ENABLED
                        .defaultValue(),
                    64 << 10,
                    true);
            try {
              sorter.startThreads();
              while (input.hasNext()) sorter.write(input.next());
              var sorted = sorter.getIterator();
              BinaryRowData row = new BinaryRowData(type.getFieldCount());
              while ((row = sorted.next(row)) != null) writer.write(row);
              statistics.spillFiles = sorter.getNumSpillFiles();
              statistics.spillBytes = sorter.getSpillInBytes();
            } finally {
              sorter.close();
              if (!memory.verifyEmpty())
                throw new IllegalStateException("Stock sort leaked memory");
            }
          } finally {
            memory.shutdown();
          }
        } else {
          while (input.hasNext()) writer.write(input.next());
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

  private static final class Ordering {
    final GeneratedNormalizedKeyComputer normalized;
    final GeneratedRecordComparator comparator;

    Ordering(Table table, RowType type) {
      var spec = SortSpec.builder();
      for (var field : table.sortOrder().fields()) {
        int index = -1;
        for (int i = 0; i < table.schema().columns().size(); i++)
          if (table.schema().columns().get(i).fieldId() == field.sourceId()) index = i;
        if (!field.transform().isIdentity() || index < 0)
          throw new IllegalArgumentException(
              "Benchmark stock sort requires top-level identity fields");
        spec.addField(
            index,
            field.direction() == org.apache.iceberg.SortDirection.ASC,
            field.nullOrder() == org.apache.iceberg.NullOrder.NULLS_LAST);
      }
      var generator =
          new SortCodeGenerator(
              new Configuration(), IcebergStockRewrite.class.getClassLoader(), type, spec.build());
      normalized = generator.generateNormalizedKeyComputer("IcebergMaintenanceNormalizedKey");
      comparator = generator.generateRecordComparator("IcebergMaintenanceComparator");
    }
  }
}
