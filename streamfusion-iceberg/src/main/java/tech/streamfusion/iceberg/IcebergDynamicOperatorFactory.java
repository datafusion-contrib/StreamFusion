package tech.streamfusion.iceberg;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ColumnarRecordMetrics;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Runs the host generator/updater immediately on borrowed views, gathering only Arrow positions.
 */
final class IcebergDynamicOperatorFactory extends AbstractStreamOperatorFactory<IcebergDynamicBatch>
    implements OneInputStreamOperatorFactory<Object, IcebergDynamicBatch> {
  private final StreamOperatorFactory<?> original;
  private final RowType inputType;

  IcebergDynamicOperatorFactory(StreamOperatorFactory<?> original, RowType inputType) {
    this.original = original;
    this.inputType = inputType;
    setChainingStrategy(original.getChainingStrategy());
  }

  private record Route(String sideOutput, IcebergDynamicBatch.Target target) {}

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T extends StreamOperator<IcebergDynamicBatch>> T createStreamOperator(
      StreamOperatorParameters<IcebergDynamicBatch> parameters) {
    var routes = new LinkedHashMap<Route, Integer>();
    var schemas = new java.util.IdentityHashMap<org.apache.iceberg.Schema, String>();
    var specs = new java.util.IdentityHashMap<org.apache.iceberg.PartitionSpec, String>();
    int[][] labels = new int[1][];
    int[] position = new int[1];
    RowType[] currentType = new RowType[1];
    Output bridge =
        (Output)
            Proxy.newProxyInstance(
                Output.class.getClassLoader(),
                new Class<?>[] {Output.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("collect")) {
                    if (labels[0] == null)
                      throw new IllegalStateException("Host retained a borrowed dynamic row");
                    String side = args.length == 2 ? ((OutputTag<?>) args[0]).getId() : null;
                    Object value = ((StreamRecord<?>) args[args.length - 1]).getValue();
                    var target = IcebergDynamicRecords.target(value, schemas, specs);
                    if (!routes.containsKey(new Route(side, target))
                        && !FlinkSchemaUtil.convert(target.schema()).equals(currentType[0]))
                      throw new IllegalArgumentException(
                          "Dynamic Arrow schema conversion is not admitted");
                    if (labels[0][position[0]] >= 0)
                      throw new IllegalStateException(
                          "Dynamic Arrow routing requires one output per input row");
                    labels[0][position[0]] =
                        routes.computeIfAbsent(new Route(side, target), key -> routes.size());
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
    OneInputStreamOperator host =
        (OneInputStreamOperator) original.createStreamOperator(hostParameters);
    return (T)
        Proxy.newProxyInstance(
            OneInputStreamOperator.class.getClassLoader(),
            new Class<?>[] {OneInputStreamOperator.class},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "equals" -> proxy == args[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "toString" -> "Iceberg Arrow dynamic control operator";
                  default -> throw new IllegalStateException(method.getName());
                };
              }
              if (method.getName().equals("open"))
                NativeAllocator.initializeFor((AbstractStreamOperator<?>) host);
              if (!method.getName().equals("processElement")) return invoke(host, method, args);
              StreamRecord input = (StreamRecord) args[0];
              IcebergDynamicBatch dynamic =
                  inputType == null ? (IcebergDynamicBatch) input.getValue() : null;
              ArrowBatch batch = dynamic == null ? (ArrowBatch) input.getValue() : dynamic.batch;
              var schema = dynamic == null ? null : dynamic.target.schema();
              var spec = dynamic == null ? null : dynamic.target.spec(schema);
              currentType[0] = dynamic == null ? inputType : FlinkSchemaUtil.convert(schema);
              ColumnarRecordMetrics.countIngested(host.getMetricGroup(), batch.rowCount());
              routes.clear();
              schemas.clear();
              specs.clear();
              long route;
              try (var root = batch.root()) {
                labels[0] = new int[root.getRowCount()];
                java.util.Arrays.fill(labels[0], -1);
                var reader = ArrowConversion.createArrowReader(root, currentType[0]);
                var kinds =
                    (org.apache.arrow.vector.TinyIntVector)
                        root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
                try {
                  for (int row = 0; row < root.getRowCount(); row++) {
                    position[0] = row;
                    RowData view = reader.read(row);
                    view.setRowKind(
                        kinds == null
                            ? org.apache.flink.types.RowKind.INSERT
                            : org.apache.flink.types.RowKind.fromByteValue(kinds.get(row)));
                    Object payload =
                        dynamic == null
                            ? view
                            : IcebergDynamicRecords.record(dynamic.target, schema, spec, view);
                    host.processElement(input.copy(payload));
                    if (labels[0][row] < 0)
                      throw new IllegalStateException("Dynamic router dropped an input row");
                  }
                  route = IcebergArrowRouting.capture(root, labels[0]);
                } finally {
                  labels[0] = null;
                }
              }
              var ordered = new ArrayList<>(routes.keySet());
              Output<StreamRecord<ArrowBatch>> routed =
                  (Output)
                      Proxy.newProxyInstance(
                          Output.class.getClassLoader(),
                          new Class<?>[] {Output.class},
                          (outProxy, outMethod, outArgs) -> {
                            if (!outMethod.getName().equals("collect"))
                              throw new IllegalStateException(outMethod.getName());
                            StreamRecord<ArrowBatch> record = (StreamRecord<ArrowBatch>) outArgs[0];
                            var selected = ordered.get(record.getValue().keyGroup());
                            var value =
                                new IcebergDynamicBatch(selected.target(), record.getValue());
                            if (selected.sideOutput() == null)
                              parameters.getOutput().collect(((StreamRecord) record).copy(value));
                            else
                              parameters
                                  .getOutput()
                                  .collect(
                                      new OutputTag<>(
                                          selected.sideOutput(), IcebergDynamicBatch.TYPE),
                                      ((StreamRecord) record).copy(value));
                            return null;
                          });
              IcebergArrowRouting.emit(route, (StreamRecord) input, routed, host.getMetricGroup());
              routes.clear();
              schemas.clear();
              specs.clear();
              return null;
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
  @SuppressWarnings("rawtypes")
  public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader loader) {
    return OneInputStreamOperator.class;
  }
}
