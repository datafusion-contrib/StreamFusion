package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.SumWithRetractAggFunction;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.DistinctInfo;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkOverDecimalAccumulatorContractTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void generatedOverArithmeticRestoresTheSumAndCountInsteadOfTheAverage(boolean retract)
      throws Exception {
    var handler = handler(12, 2, retract);
    try {
      handler.resetAccumulators();
      assertTrue(handler.getValue().isNullAt(0));
      assertEquals(
          BigDecimal.ZERO.setScale(2), handler.getValue().getDecimal(1, 38, 2).toBigDecimal());
      assertTrue(handler.getValue().isNullAt(2));
      handler.accumulate(row("1.25", 12, 2));
      handler.accumulate(row("2.50", 12, 2));
      handler.accumulate(row(null, 12, 2));
      assertEquals(
          new BigDecimal("1.875000"), handler.getValue().getDecimal(2, 38, 6).toBigDecimal());
      var functions = functions(12, 2, retract);
      List<LogicalType> types = new ArrayList<>();
      for (var function : functions) {
        for (DataType type : function.getAggBufferTypes()) types.add(type.getLogicalType());
      }
      var copy =
          new RowDataSerializer(RowType.of(types.toArray(new LogicalType[0])))
              .toBinaryRow(handler.getAccumulators())
              .copy();
      handler.close();
      handler = handler(12, 2, retract);
      handler.setAccumulators(copy);
      handler.accumulate(row("-0.25", 12, 2));
      assertEquals(new BigDecimal("3.50"), handler.getValue().getDecimal(0, 38, 2).toBigDecimal());
      assertEquals(new BigDecimal("3.50"), handler.getValue().getDecimal(1, 38, 2).toBigDecimal());
      assertEquals(
          new BigDecimal("1.166667"), handler.getValue().getDecimal(2, 38, 6).toBigDecimal());
    } finally {
      handler.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void generatedDecimalOverflowDistinguishesSumFromSum0AndAverage(boolean retract)
      throws Exception {
    var handler = handler(38, 0, retract);
    try {
      handler.resetAccumulators();
      handler.accumulate(row("99999999999999999999999999999999999999", 38, 0));
      handler.accumulate(row("1", 38, 0));
      for (int i = 0; i < 3; i++) assertTrue(handler.getValue().isNullAt(i));
      handler.accumulate(row("7", 38, 0));
      assertEquals(new BigDecimal("7"), handler.getValue().getDecimal(0, 38, 0).toBigDecimal());
      assertTrue(handler.getValue().isNullAt(1));
      assertTrue(handler.getValue().isNullAt(2));
      if (retract) {
        handler.retract(row("99999999999999999999999999999999999999", 38, 0));
        assertEquals(
            new BigDecimal("-99999999999999999999999999999999999992"),
            handler.getValue().getDecimal(0, 38, 0).toBigDecimal());
        assertTrue(handler.getValue().isNullAt(1));
        assertTrue(handler.getValue().isNullAt(2));
      }
    } finally {
      handler.close();
    }
  }

  private static GenericRowData row(String value, int precision, int scale) {
    return GenericRowData.of(
        value == null ? null : DecimalData.fromBigDecimal(new BigDecimal(value), precision, scale));
  }

  static DeclarativeAggregateFunction[] functions(int precision, int scale, boolean retract) {
    var type = new DecimalType(precision, scale);
    return new DeclarativeAggregateFunction[] {
      retract
          ? new SumWithRetractAggFunction.DecimalSumWithRetractAggFunction(type)
          : new SumAggFunction.DecimalSumAggFunction(type),
      new Sum0AggFunction.DecimalSum0AggFunction(type),
      new AvgAggFunction.DecimalAvgAggFunction(type)
    };
  }

  private static AggsHandleFunction handler(int precision, int scale, boolean retract)
      throws Exception {
    var result =
        generated(precision, scale, retract)
            .newInstance(Thread.currentThread().getContextClassLoader());
    result.open(new PerKeyStateDataViewStore(null));
    return result;
  }

  static GeneratedAggsHandleFunction generated(int precision, int scale, boolean retract) {
    var table =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.newInstance().inStreamingMode().build());
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var factory = planner.getTypeFactory();
    var loader = Thread.currentThread().getContextClassLoader();
    var inputType = new DecimalType(precision, scale);
    var builder =
        planner
            .createRelBuilder()
            .values(
                factory.buildRelNodeRowType(
                    JavaScalaConversionUtil.toScala(List.of("d")),
                    JavaScalaConversionUtil.toScala(List.<LogicalType>of(inputType))));
    var functions = functions(precision, scale, retract);
    var operators =
        new org.apache.calcite.sql.SqlAggFunction[] {
          SqlStdOperatorTable.SUM, SqlStdOperatorTable.SUM0, SqlStdOperatorTable.AVG
        };
    var infos = new AggregateInfo[functions.length];
    for (int i = 0; i < functions.length; i++) {
      var function = functions[i];
      var call =
          AggregateCall.create(
              operators[i],
              false,
              List.of(0),
              -1,
              factory.createFieldTypeFromLogicalType(function.getResultType().getLogicalType()),
              "a" + i);
      infos[i] =
          new AggregateInfo(
              call,
              function,
              i,
              new int[] {0},
              new DataType[] {DataTypes.DECIMAL(precision, scale)},
              function.getAggBufferTypes(),
              new DataViewSpec[0],
              function.getResultType(),
              retract);
    }
    var generator =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(table.getConfig(), loader),
                builder,
                JavaScalaConversionUtil.toScala(List.<LogicalType>of(inputType)),
                false)
            .needAccumulate();
    if (retract) generator.needRetract();
    return generator.generateAggsHandler(
        "OverDecimalContract",
        new AggregateInfoList(infos, scala.Option.empty(), false, new DistinctInfo[0]));
  }
}
