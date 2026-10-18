package tech.streamfusion.iceberg;

import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.sink.SinkUtil;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import tech.streamfusion.operator.ArrowBatch;

/** Normal streaming sink with native encoding confined to Java-selected Parquet data files. */
public final class NativeIcebergSink {
  private NativeIcebergSink() {}

  public static DataStreamSink<Void> append(
      DataStream<ArrowBatch> input,
      Table table,
      org.apache.iceberg.flink.TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options) {
    return append(input, table, loader, configuration, options, null);
  }

  /** Supplies stable host operator UIDs for savepoint restoration across rescaling. */
  public static DataStreamSink<Void> append(
      DataStream<ArrowBatch> input,
      Table table,
      org.apache.iceberg.flink.TableLoader loader,
      ReadableConfig configuration,
      Map<String, String> options,
      String uidPrefix) {
    validate(table, configuration, options);
    var conf = new FlinkWriteConf(table, options, configuration);
    String schema = SchemaParser.toJson(table.schema());
    var settings =
        IcebergParquetSettings.translate(SinkUtil.writeProperties(FileFormat.PARQUET, conf, table));
    var metrics = MetricsConfig.forTable(table);
    return IcebergSinkAdapter.append(
        input,
        table,
        loader,
        configuration,
        options,
        uidPrefix,
        dataWriterDecorator(schema, settings, metrics));
  }

  static IcebergTaskWriterFactory.Decorator dataWriterDecorator(
      String schema, IcebergParquetSettings settings, MetricsConfig metrics) {
    return (stock, context) ->
        new FileWriterFactory<>() {
          @Override
          public DataWriter<RowData> newDataWriter(
              EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            return new DataWriter<>(
                new NativeIcebergAppender(
                    context, file.encryptingOutputFile(), schema, settings, metrics),
                FileFormat.PARQUET,
                file.encryptingOutputFile().location(),
                spec,
                partition,
                file.keyMetadata());
          }

          @Override
          public EqualityDeleteWriter<RowData> newEqualityDeleteWriter(
              EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            return stock.newEqualityDeleteWriter(file, spec, partition);
          }

          @Override
          public PositionDeleteWriter<RowData> newPositionDeleteWriter(
              EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            return stock.newPositionDeleteWriter(file, spec, partition);
          }
        };
  }

  public static void validate(
      Table table, ReadableConfig configuration, Map<String, String> options) {
    if (org.apache.iceberg.TableUtil.formatVersion(table) > 3) {
      throw new IllegalArgumentException("Native Iceberg format version is not verified");
    }
    if (!(table.encryption() instanceof org.apache.iceberg.encryption.PlaintextEncryptionManager)) {
      throw new IllegalArgumentException("Native Iceberg encryption is not verified");
    }
    var conf = new FlinkWriteConf(table, options, configuration);
    if ((!table.schema().identifierFieldIds().isEmpty() || conf.upsertMode())
        && org.apache.iceberg.TableUtil.formatVersion(table) < 2) {
      throw new IllegalArgumentException("Native keyed writes require format-v2/v3");
    }
    if (conf.upsertMode() && table.schema().identifierFieldIds().isEmpty()) {
      throw new IllegalArgumentException("Native upsert writes require table identifier fields");
    }
    if (conf.dataFileFormat() != FileFormat.PARQUET)
      throw new IllegalArgumentException("Iceberg native data files require Parquet");
    validateSchema(table.schema());
    IcebergParquetSettings.translate(SinkUtil.writeProperties(FileFormat.PARQUET, conf, table));
    if (table.io() instanceof org.apache.iceberg.hadoop.HadoopFileIO) {
      var hadoop = ((org.apache.iceberg.hadoop.HadoopFileIO) table.io()).conf();
      for (var entry : hadoop) {
        if (entry.getKey().startsWith("parquet.") || entry.getKey().startsWith("write.parquet.")) {
          throw new IllegalArgumentException(
              "Unverified Hadoop Parquet override: " + entry.getKey());
        }
      }
    }
  }

  static void validateSchema(org.apache.iceberg.Schema schema) {
    if (schema.columns().isEmpty())
      throw new IllegalArgumentException("Native Iceberg writes require data columns");
    for (var field : schema.columns()) {
      switch (field.type().typeId()) {
        case INTEGER:
        case LONG:
        case STRING:
        case BOOLEAN:
        case BINARY:
        case DATE:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case TIME:
        case TIMESTAMP:
          break;
        default:
          throw new IllegalArgumentException("Unverified native Iceberg type: " + field.type());
      }
    }
  }
}
