package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import tech.streamfusion.compat.RichSinkFunction;
import tech.streamfusion.compat.SinkFunctionProvider;

/**
 * Checkpointed result sink for recovery parity tests. The optional drop-update-before mode requests
 * the same changelog edge as Flink's blackhole sink while capturing each retained event.
 */
public final class RecoveryResultTableFactory implements DynamicTableSinkFactory {
  private static final ConfigOption<Boolean> DROP_UPDATE_BEFORE =
      ConfigOptions.key("drop-update-before").booleanType().defaultValue(false);
  private static final String RESULTS = "streamfusion-recovery-results";

  static List<List<Object>> results(JobExecutionResult job, DataType type) throws Exception {
    List<byte[]> encoded = job.getAccumulatorResult(RESULTS);
    if (Boolean.getBoolean("sf.recoveryResults.profile")) {
      System.out.println(
          "[recovery-result] job="
              + job.getJobID()
              + " keys="
              + job.getAllAccumulatorResults().keySet()
              + " rows="
              + (encoded == null ? -1 : encoded.size())
              + " heapUsed="
              + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
              + " maxHeap="
              + Runtime.getRuntime().maxMemory());
    }
    var serializer = new RowDataSerializer((RowType) type.getLogicalType());
    var converter = DataStructureConverters.getConverter(type);
    converter.open(RecoveryResultTableFactory.class.getClassLoader());
    List<List<Object>> result = new ArrayList<>();
    for (byte[] bytes : encoded) {
      Row row =
          (Row) converter.toExternal(serializer.deserialize(new DataInputDeserializer(bytes)));
      List<Object> fields = new ArrayList<>();
      fields.add(row.getKind().shortString());
      for (int i = 0; i < row.getArity(); i++) {
        fields.add(NativeParity.comparableValue(row.getField(i)));
      }
      result.add(fields);
    }
    return result;
  }

  @Override
  public String factoryIdentifier() {
    return "recovery-results";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(DROP_UPDATE_BEFORE);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    return new CaptureSink(
        context.getPhysicalRowDataType(),
        Boolean.parseBoolean(
            context
                .getCatalogTable()
                .getOptions()
                .getOrDefault(DROP_UPDATE_BEFORE.key(), "false")));
  }

  private static final class CaptureSink implements DynamicTableSink {
    private final DataType type;
    private final boolean dropUpdateBefore;

    private CaptureSink(DataType type, boolean dropUpdateBefore) {
      this.type = type;
      this.dropUpdateBefore = dropUpdateBefore;
    }

    public DataType type() {
      return type;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requested) {
      if (!dropUpdateBefore) return requested;
      var mode = ChangelogMode.newBuilder();
      for (var kind : requested.getContainedKinds()) {
        if (kind != RowKind.UPDATE_BEFORE) mode.addContainedKind(kind);
      }
      return mode.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return SinkFunctionProvider.of(new CaptureFunction(type));
    }

    @Override
    public DynamicTableSink copy() {
      return new CaptureSink(type, dropUpdateBefore);
    }

    @Override
    public String asSummaryString() {
      return "RecoveryResults";
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      CaptureSink that = (CaptureSink) other;
      return java.util.Objects.equals(type, that.type) && dropUpdateBefore == that.dropUpdateBefore;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(type);
      result = 31 * result + Boolean.hashCode(dropUpdateBefore);
      return result;
    }

    @Override
    public String toString() {
      return "CaptureSink[type=" + type + "]";
    }
  }

  static final class CaptureFunction extends RichSinkFunction<RowData>
      implements CheckpointedFunction {
    private final RowDataSerializer serializer;
    private final List<RowData> rows = new ArrayList<>();
    private transient ListState<RowData> state;

    CaptureFunction(DataType type) {
      serializer = new RowDataSerializer((RowType) type.getLogicalType());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("recovery-results", serializer));
      if (context.isRestored()) {
        for (RowData row : state.get()) rows.add(serializer.copy(row));
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(rows);
    }

    @Override
    public void invoke(RowData value, Context context) {
      rows.add(serializer.copy(value));
    }

    @Override
    public void finish() throws Exception {
      var results = new ListAccumulator<byte[]>();
      for (RowData value : rows) {
        var output = new DataOutputSerializer(64);
        serializer.serialize(value, output);
        results.add(output.getCopyOfBuffer());
      }
      getRuntimeContext().addAccumulator(RESULTS, results);
      if (Boolean.getBoolean("sf.recoveryResults.profile")) {
        System.out.println(
            "[recovery-capture] task="
                + getRuntimeContext().getTaskNameWithSubtasks()
                + " rows="
                + rows.size()
                + " bytes="
                + results.getLocalValue().stream().mapToLong(value -> value.length).sum()
                + " heapUsed="
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
                + " maxHeap="
                + Runtime.getRuntime().maxMemory());
      }
    }
  }
}
