package tech.streamfusion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;

/** Benchmark sink that persists every change as a Parquet row with its {@code RowKind}. */
public final class ChangelogParquetTableFactory implements DynamicTableSinkFactory {

  private static final ConfigOption<String> PATH =
      ConfigOptions.key("path").stringType().noDefaultValue();

  @Override
  public String factoryIdentifier() {
    return "changelog-parquet";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(PATH);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    RowType inputType =
        (RowType)
            context
                .getCatalogTable()
                .getResolvedSchema()
                .toPhysicalRowDataType()
                .getLogicalType();
    String path = context.getCatalogTable().getOptions().get(PATH.key());
    return new ChangelogParquetSink(inputType, path);
  }

  private static final class ChangelogParquetSink implements DynamicTableSink {
    private final RowType inputType;
    private final String path;

    private ChangelogParquetSink(RowType inputType, String path) {
      this.inputType = inputType;
      this.path = path;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
      return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      RowType outputType = outputType(inputType);
      FileSink<RowData> sink =
          FileSink.forBulkFormat(
                  new Path(path),
                  ParquetRowDataBuilder.createWriterFactory(
                      outputType, new Configuration(), true))
              .build();
      return (DataStreamSinkProvider)
          (providerContext, input) -> {
            SingleOutputStreamOperator<RowData> changes =
                input
                    .map(new AppendRowKind(inputType))
                    .returns(InternalTypeInfo.of(outputType))
                    .name("Append Parquet changelog row kind");
            providerContext
                .generateUid("changelog-parquet-row-kind")
                .ifPresent(changes::uid);
            DataStreamSink<RowData> files =
                changes.sinkTo(sink).name("Changelog Parquet files");
            providerContext.generateUid("changelog-parquet-files").ifPresent(files::uid);
            return files;
          };
    }

    @Override
    public DynamicTableSink copy() {
      return new ChangelogParquetSink(inputType, path);
    }

    @Override
    public String asSummaryString() {
      return "ChangelogParquet";
    }
  }

  private static RowType outputType(RowType inputType) {
    List<RowType.RowField> fields = new ArrayList<>(inputType.getFieldCount() + 1);
    fields.add(new RowType.RowField("_row_kind", new VarCharType(false, 2)));
    fields.addAll(inputType.getFields());
    return new RowType(fields);
  }

  private static final class AppendRowKind extends RichMapFunction<RowData, RowData> {
    private final RowType inputType;
    private transient RowData.FieldGetter[] getters;

    private AppendRowKind(RowType inputType) {
      this.inputType = inputType;
    }

    @Override
    public void open(OpenContext openContext) {
      getters = new RowData.FieldGetter[inputType.getFieldCount()];
      for (int i = 0; i < getters.length; i++) {
        getters[i] = RowData.createFieldGetter(inputType.getTypeAt(i), i);
      }
    }

    @Override
    public RowData map(RowData input) {
      GenericRowData output = new GenericRowData(input.getArity() + 1);
      output.setField(0, StringData.fromString(input.getRowKind().shortString()));
      for (int i = 0; i < input.getArity(); i++) {
        output.setField(i + 1, getters[i].getFieldOrNull(input));
      }
      return output;
    }
  }
}
