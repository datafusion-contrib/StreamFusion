package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.operators.sink.InitContextBase;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.SinkUtil;
import org.apache.iceberg.io.FileWriterFactory;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Changes only dynamic payloads and data-file factories; all committing work is delegated. */
@SuppressWarnings({"rawtypes", "unchecked"})
final class IcebergDynamicSinkBridge
    implements Sink<IcebergDynamicBatch>,
        SupportsCommitter<Object>,
        SupportsPreWriteTopology<IcebergDynamicBatch>,
        SupportsPreCommitTopology<Object, Object>,
        SupportsPostCommitTopology<Object> {
  private final Sink delegate;

  IcebergDynamicSinkBridge(Sink delegate) {
    this.delegate = delegate;
  }

  @Override
  public SinkWriter<IcebergDynamicBatch> createWriter(WriterInitContext init) throws IOException {
    if (!(init instanceof InitContextBase host))
      throw new IllegalArgumentException("Unverified Sink V2 runtime context");
    var runtime = (StreamingRuntimeContext) host.getRuntimeContext();
    TaskOffHeapMemory.initialize(runtime.getTaskManagerRuntimeInfo().getConfiguration());
    TaskOffHeapMemory.registerMetrics(init.metricGroup());
    var writer = (CommittingSinkWriter<Object, Object>) delegate.createWriter(init);
    var context = new ArrowWriteContext();
    try {
      var field = IcebergSinkAdapter.field(writer.getClass(), "taskWriterFactories");
      Map<Object, RowDataTaskWriterFactory> cache = (Map) field.get(writer);
      Map<String, String> options =
          (Map) IcebergSinkAdapter.field(writer.getClass(), "commonWriteProperties").get(writer);
      var configuration =
          (org.apache.flink.configuration.Configuration)
              IcebergSinkAdapter.field(writer.getClass(), "flinkConfig").get(writer);
      var nativeFactories = init.metricGroup().counter("nativeDynamicFactories");
      var stockFactories = init.metricGroup().counter("stockDynamicFactories");
      field.set(
          writer,
          new AbstractMap<Object, RowDataTaskWriterFactory>() {
            @Override
            public Set<Entry<Object, RowDataTaskWriterFactory>> entrySet() {
              return cache.entrySet();
            }

            @Override
            public RowDataTaskWriterFactory computeIfAbsent(
                Object key, Function<? super Object, ? extends RowDataTaskWriterFactory> create) {
              return cache.computeIfAbsent(
                  key,
                  missing -> {
                    var factory = create.apply(missing);
                    try {
                      Table table =
                          ((Supplier<Table>)
                                  IcebergSinkAdapter.field(
                                          RowDataTaskWriterFactory.class, "tableSupplier")
                                      .get(factory))
                              .get();
                      Schema schema =
                          (Schema)
                              IcebergSinkAdapter.field(RowDataTaskWriterFactory.class, "schema")
                                  .get(factory);
                      final IcebergParquetSettings settings;
                      try {
                        NativeIcebergSink.validate(table, configuration, options);
                        var conf = new FlinkWriteConf(table, options, configuration);
                        settings =
                            IcebergParquetSettings.translate(
                                SinkUtil.writeProperties(FileFormat.PARQUET, conf, table));
                      } catch (IllegalArgumentException unsupported) {
                        stockFactories.inc();
                        return factory;
                      }
                      var files =
                          IcebergSinkAdapter.field(
                              RowDataTaskWriterFactory.class, "fileWriterFactory");
                      files.set(
                          factory,
                          NativeIcebergSink.dataWriterDecorator(
                                  SchemaParser.toJson(schema),
                                  settings,
                                  MetricsConfig.forTable(table))
                              .decorate((FileWriterFactory<RowData>) files.get(factory), context));
                      nativeFactories.inc();
                      return factory;
                    } catch (IllegalAccessException failure) {
                      throw new IllegalStateException(
                          "Cannot decorate released dynamic writer factory", failure);
                    }
                  });
            }
          });
    } catch (IllegalAccessException failure) {
      try {
        writer.close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw new IOException("Cannot install released dynamic writer adapter", failure);
    }
    return new CommittingSinkWriter<>() {
      @Override
      public void write(IcebergDynamicBatch input, Context hostContext)
          throws IOException, InterruptedException {
        tech.streamfusion.operator.ColumnarRecordMetrics.countIngested(
            init.metricGroup(), input.batch.rowCount());
        var schema = input.target.schema();
        var spec = input.target.spec(schema);
        try (var root = input.batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, FlinkSchemaUtil.convert(schema));
          var kinds =
              (org.apache.arrow.vector.TinyIntVector)
                  root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          context.begin(root);
          try {
            for (int i = 0; i < root.getRowCount(); i++) {
              RowData row = reader.read(i);
              row.setRowKind(
                  kinds == null
                      ? org.apache.flink.types.RowKind.INSERT
                      : org.apache.flink.types.RowKind.fromByteValue(kinds.get(i)));
              context.position(i, row);
              writer.write(
                  IcebergDynamicRecords.record(input.target, schema, spec, row), hostContext);
            }
            context.drain();
          } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            context.fail(failure);
            throw failure;
          } finally {
            context.clear();
          }
        }
      }

      @Override
      public Collection<Object> prepareCommit() throws IOException, InterruptedException {
        return writer.prepareCommit();
      }

      @Override
      public void flush(boolean end) throws IOException, InterruptedException {
        writer.flush(end);
      }

      @Override
      public void writeWatermark(org.apache.flink.api.common.eventtime.Watermark watermark)
          throws IOException, InterruptedException {
        writer.writeWatermark(watermark);
      }

      @Override
      public void close() throws Exception {
        writer.close();
      }
    };
  }

  @Override
  public DataStream<IcebergDynamicBatch> addPreWriteTopology(
      DataStream<IcebergDynamicBatch> input) {
    return input.keyBy(
        batch -> batch.target.writerKey(), org.apache.flink.api.common.typeinfo.Types.INT);
  }

  @Override
  public Committer<Object> createCommitter(CommitterInitContext context) throws IOException {
    return ((SupportsCommitter<Object>) delegate).createCommitter(context);
  }

  @Override
  public SimpleVersionedSerializer<Object> getCommittableSerializer() {
    return ((SupportsCommitter<Object>) delegate).getCommittableSerializer();
  }

  @Override
  public SimpleVersionedSerializer<Object> getWriteResultSerializer() {
    return ((SupportsPreCommitTopology<Object, Object>) delegate).getWriteResultSerializer();
  }

  @Override
  public DataStream<CommittableMessage<Object>> addPreCommitTopology(
      DataStream<CommittableMessage<Object>> results) {
    return ((SupportsPreCommitTopology<Object, Object>) delegate).addPreCommitTopology(results);
  }

  @Override
  public void addPostCommitTopology(DataStream<CommittableMessage<Object>> results) {
    if (delegate instanceof SupportsPostCommitTopology<?> topology)
      ((SupportsPostCommitTopology<Object>) topology).addPostCommitTopology(results);
  }
}
