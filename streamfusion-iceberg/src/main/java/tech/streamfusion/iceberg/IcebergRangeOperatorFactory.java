package tech.streamfusion.iceberg;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.flink.sink.shuffle.DataStatisticsOperatorFactory;
import org.apache.iceberg.flink.sink.shuffle.StatisticsOrRecord;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;

/** Keeps Iceberg's statistics operator/coordinator and partitioner on borrowed Arrow rows. */
final class IcebergRangeOperatorFactory extends AbstractStreamOperatorFactory<ArrowBatch>
    implements OneInputStreamOperatorFactory<ArrowBatch, ArrowBatch>,
        CoordinatedOperatorFactory<ArrowBatch> {
  private final DataStatisticsOperatorFactory statistics;
  private final Partitioner<StatisticsOrRecord> partitioner;
  private final RowType rowType;
  private final int writers;

  IcebergRangeOperatorFactory(
      DataStatisticsOperatorFactory statistics,
      Partitioner<StatisticsOrRecord> partitioner,
      RowType rowType,
      int writers) {
    this.statistics = statistics;
    this.partitioner = partitioner;
    this.rowType = rowType;
    this.writers = writers;
    setChainingStrategy(statistics.getChainingStrategy());
  }

  @Override
  public OperatorCoordinator.Provider getCoordinatorProvider(String name, OperatorID id) {
    return statistics.getCoordinatorProvider(name, id);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T extends StreamOperator<ArrowBatch>> T createStreamOperator(
      StreamOperatorParameters<ArrowBatch> parameters) {
    final Partitioner<StatisticsOrRecord> selected;
    try {
      selected = InstantiationUtil.clone(partitioner);
    } catch (Exception failure) {
      throw new IllegalStateException("Cannot copy the host range partitioner", failure);
    }
    int[][] destinations = new int[1][];
    int[] position = new int[1];
    org.apache.flink.metrics.Counter[] updates = new org.apache.flink.metrics.Counter[1];
    Output<StreamRecord<StatisticsOrRecord>> bridge =
        (Output)
            Proxy.newProxyInstance(
                Output.class.getClassLoader(),
                new Class<?>[] {Output.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("collect") && args.length == 1) {
                    var record = (StatisticsOrRecord) ((StreamRecord<?>) args[0]).getValue();
                    int channel = selected.partition(record, writers);
                    if (!record.hasRecord() && updates[0] != null) updates[0].inc();
                    if (record.hasRecord()) {
                      if (destinations[0] == null)
                        throw new IllegalStateException(
                            "Host range operator retained a borrowed payload");
                      destinations[0][position[0]] = channel;
                    }
                    return null;
                  }
                  return invoke(parameters.getOutput(), method, args);
                });
    var hostParameters =
        new StreamOperatorParameters<>(
            parameters.getContainingTask(),
            parameters.getStreamConfig(),
            bridge,
            parameters::getProcessingTimeService,
            parameters.getOperatorEventDispatcher(),
            parameters.getMailboxExecutor());
    OneInputStreamOperator<RowData, StatisticsOrRecord> host =
        statistics.createStreamOperator(hostParameters);
    updates[0] = host.getMetricGroup().counter("nativeRangeStatisticsUpdates");
    return (T)
        Proxy.newProxyInstance(
            OneInputStreamOperator.class.getClassLoader(),
            new Class<?>[] {OneInputStreamOperator.class},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "equals" -> proxy == args[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "toString" -> "Iceberg Arrow range operator";
                  default -> throw new IllegalStateException(method.getName());
                };
              }
              if (method.getName().equals("open"))
                NativeAllocator.initializeFor((AbstractStreamOperator<?>) host);
              if (method.getName().equals("processElement")) {
                var input = (StreamRecord<ArrowBatch>) args[0];
                ColumnarRecordMetrics.countIngested(
                    host.getMetricGroup(), input.getValue().rowCount());
                long route;
                try (var root = input.getValue().root()) {
                  destinations[0] = new int[root.getRowCount()];
                  var reader = ArrowConversion.createArrowReader(root, rowType);
                  try {
                    for (int row = 0; row < root.getRowCount(); row++) {
                      position[0] = row;
                      host.processElement(new StreamRecord<>(reader.read(row)));
                    }
                    route = IcebergArrowRouting.capture(root, destinations[0]);
                  } finally {
                    destinations[0] = null;
                  }
                }
                IcebergArrowRouting.emit(
                    route,
                    new StreamRecord<>(input.getValue()),
                    parameters.getOutput(),
                    host.getMetricGroup());
                return null;
              }
              return invoke(host, method, args);
            });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException failure) {
      throw failure.getCause();
    }
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader loader) {
    return OneInputStreamOperator.class;
  }
}
