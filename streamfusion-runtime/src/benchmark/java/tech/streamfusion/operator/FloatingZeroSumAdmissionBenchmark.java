package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.DistinctInfo;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Accumulator admission only; complete grouped/window jobs retain their full production costs. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class FloatingZeroSumAdmissionBenchmark {
  private static volatile long consumed;

  static Stream<String> types() {
    return Stream.of(System.getProperty("sumzero.types", "float,double").split(","));
  }

  @ParameterizedTest
  @MethodSource("types")
  void preparedRowDataFeedsTheReleasedGeneratedAccumulator(String type) throws Exception {
    boolean narrow = type.equals("float");
    if (!narrow && !type.equals("double")) throw new IllegalArgumentException(type);
    int count = Integer.getInteger("sumzero.rows", 1048576);
    int warmups = Integer.getInteger("sumzero.warmups", 5);
    int runs = Integer.getInteger("sumzero.runs", 9);
    var inputType = RowType.of(narrow ? new FloatType() : new DoubleType());
    var serializer = new RowDataSerializer(inputType);
    List<RowData> input = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Object value;
      if (narrow) value = i % 7 == 0 ? null : Float.valueOf((i % 127 - 63) * 0.25f);
      else value = i % 7 == 0 ? null : Double.valueOf((i % 127 - 63) * 0.25);
      input.add(serializer.toBinaryRow(GenericRowData.of(value)).copy());
    }
    String export = System.getProperty("sumzero.export");
    if (export != null) {
      try (var allocator = new RootAllocator();
          var root = RowDataArrowConverter.write(input, inputType, allocator);
          var channel =
              FileChannel.open(
                  Path.of(export + "-" + type + ".arrow"),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.TRUNCATE_EXISTING,
                  StandardOpenOption.WRITE);
          var writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    }
    var handler = handler(narrow);
    try {
      Long expected = null;
      for (int trial = 0; trial < warmups + runs; trial++) {
        handler.resetAccumulators();
        long start = System.nanoTime();
        for (RowData row : input) handler.accumulate(row);
        RowData result = handler.getValue();
        long bits =
            narrow
                ? Integer.toUnsignedLong(Float.floatToRawIntBits(result.getFloat(0)))
                : Double.doubleToRawLongBits(result.getDouble(0));
        consumed ^= bits;
        double seconds = (System.nanoTime() - start) / 1e9;
        if (expected == null) expected = bits;
        else assertEquals(expected.longValue(), bits);
        System.out.printf(
            Locale.ROOT,
            "[sumzero-trial] type=%s trial=%d warmup=%s seconds=%.9f bits=%d rows=%d"
                + " layout=binary%n",
            type,
            trial,
            trial < warmups,
            seconds,
            bits,
            count);
      }
    } finally {
      handler.close();
    }
  }

  private static AggsHandleFunction handler(boolean narrow) throws Exception {
    var table =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.newInstance().inStreamingMode().build());
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var factory = planner.getTypeFactory();
    var loader = Thread.currentThread().getContextClassLoader();
    LogicalType inputType = narrow ? new FloatType() : new DoubleType();
    DeclarativeAggregateFunction function =
        narrow
            ? new Sum0AggFunction.FloatSum0AggFunction()
            : new Sum0AggFunction.DoubleSum0AggFunction();
    var builder =
        planner
            .createRelBuilder()
            .values(
                factory.buildRelNodeRowType(
                    JavaScalaConversionUtil.toScala(List.of("v")),
                    JavaScalaConversionUtil.toScala(List.of(inputType))));
    var call =
        AggregateCall.create(
            SqlStdOperatorTable.SUM0,
            false,
            List.of(0),
            -1,
            factory.createFieldTypeFromLogicalType(function.getResultType().getLogicalType()),
            "z");
    var info =
        new AggregateInfo(
            call,
            function,
            0,
            new int[] {0},
            new DataType[] {narrow ? DataTypes.FLOAT() : DataTypes.DOUBLE()},
            function.getAggBufferTypes(),
            new DataViewSpec[0],
            function.getResultType(),
            false);
    var generated =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(table.getConfig(), loader),
                builder,
                JavaScalaConversionUtil.toScala(List.of(inputType)),
                false)
            .needAccumulate()
            .generateAggsHandler(
                "FloatingSumZeroAdmission",
                new AggregateInfoList(
                    new AggregateInfo[] {info}, scala.Option.empty(), false, new DistinctInfo[0]));
    var handler = generated.newInstance(loader);
    handler.open(new PerKeyStateDataViewStore(null));
    return handler;
  }
}
