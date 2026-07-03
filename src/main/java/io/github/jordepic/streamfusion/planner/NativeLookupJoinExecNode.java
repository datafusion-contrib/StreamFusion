package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeAsyncLookupJoinOperator;
import io.github.jordepic.streamfusion.operator.NativeLookupJoinOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;

/**
 * Wraps the native lookup-join operator into the plan. The row-level join core is Flink's own: this
 * node generates the same fetcher (key building over field references and constants, wrapping the
 * connector's real lookup function obtained via {@link LookupJoinUtil#getLookupFunction}), collector
 * (residual join condition + output assembly), pre-filter, and optional dimension-side calc that
 * {@code CommonExecLookupJoin} would, and assembles them into Flink's {@link LookupJoinRunner} (sync)
 * or {@link AsyncLookupJoinRunner} (async) for the native operator to drive over each Arrow probe
 * batch — so every admitted shape is byte-identical to the host while the batches stay Arrow.
 */
public class NativeLookupJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-lookup-join";

  private final RelOptTable temporalTable;
  private final RowType probeType;
  private final Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys;
  private final @Nullable List<RexNode> projectionOnTemporalTable;
  private final @Nullable RexNode filterOnTemporalTable;
  private final @Nullable RexNode preFilterCondition;
  private final @Nullable RexNode remainingJoinCondition;
  private final boolean leftOuterJoin;
  private final @Nullable FunctionCallUtil.AsyncOptions asyncOptions;

  public NativeLookupJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      RelOptTable temporalTable,
      RowType probeType,
      Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys,
      @Nullable List<RexNode> projectionOnTemporalTable,
      @Nullable RexNode filterOnTemporalTable,
      @Nullable RexNode preFilterCondition,
      @Nullable RexNode remainingJoinCondition,
      boolean leftOuterJoin,
      @Nullable FunctionCallUtil.AsyncOptions asyncOptions) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-lookup-join_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.temporalTable = temporalTable;
    this.probeType = probeType;
    this.lookupKeys = lookupKeys;
    this.projectionOnTemporalTable = projectionOnTemporalTable;
    this.filterOnTemporalTable = filterOnTemporalTable;
    this.preFilterCondition = preFilterCondition;
    this.remainingJoinCondition = remainingJoinCondition;
    this.leftOuterJoin = leftOuterJoin;
    this.asyncOptions = asyncOptions;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);

    ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
    DataTypeFactory dataTypeFactory =
        planner.getFlinkContext().getCatalogManager().getDataTypeFactory();
    RowType tableSourceRowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
    RowType resultRowType = (RowType) getOutputType();
    String tableName = String.join(".", temporalTable.getQualifiedName());

    List<FunctionCallUtil.FunctionParam> orderedKeys = new ArrayList<>(lookupKeys.size());
    for (int key : LookupJoinUtil.getOrderedLookupKeys(lookupKeys.keySet())) {
      orderedKeys.add(lookupKeys.get(key));
    }
    boolean async = asyncOptions != null;
    UserDefinedFunction lookupFunction =
        LookupJoinUtil.getLookupFunction(
            temporalTable,
            lookupKeys.keySet(),
            classLoader,
            async,
            ResultRetryStrategy.NO_RETRY_STRATEGY,
            false);

    // The dimension side the join sees: the calc's output when a projection/filter was pushed onto
    // the temporal table, the table's own row type otherwise.
    RelDataType projectionOutputRelDataType =
        projectionOnTemporalTable != null
            ? RexUtil.createStructType(
                ShortcutUtils.unwrapTypeFactory(planner), projectionOnTemporalTable)
            : null;
    RowType rightRowType =
        projectionOutputRelDataType != null
            ? (RowType) FlinkTypeFactory.toLogicalType(projectionOutputRelDataType)
            : tableSourceRowType;
    GeneratedFilterCondition generatedPreFilter =
        FilterCodeGenerator.generateFilterCondition(
            config, classLoader, preFilterCondition, probeType);
    GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc =
        projectionOnTemporalTable != null
            ? LookupJoinCodeGenerator.generateCalcMapFunction(
                config,
                classLoader,
                JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                filterOnTemporalTable,
                projectionOutputRelDataType,
                tableSourceRowType)
            : null;

    OneInputStreamOperator<ArrowBatch, ArrowBatch> operator;
    if (async) {
      FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
          generatedFetcher =
              LookupJoinCodeGenerator.generateAsyncLookupFunction(
                  config,
                  classLoader,
                  dataTypeFactory,
                  probeType,
                  tableSourceRowType,
                  resultRowType,
                  orderedKeys,
                  (AsyncTableFunction<Object>) lookupFunction,
                  tableName);
      GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture =
          LookupJoinCodeGenerator.generateTableAsyncCollector(
              config,
              classLoader,
              "TableFunctionResultFuture",
              probeType,
              rightRowType,
              JavaScalaConversionUtil.toScala(Optional.ofNullable(remainingJoinCondition)));
      DataStructureConverter<RowData, Object> fetcherConverter =
          (DataStructureConverter<RowData, Object>)
              (DataStructureConverter<?, ?>)
                  DataStructureConverters.getConverter(generatedFetcher.dataType());
      AsyncLookupJoinRunner runner =
          generatedCalc != null
              ? new AsyncLookupJoinWithCalcRunner(
                  generatedFetcher.tableFunc(),
                  fetcherConverter,
                  generatedCalc,
                  generatedResultFuture,
                  generatedPreFilter,
                  InternalSerializers.create(rightRowType),
                  leftOuterJoin,
                  asyncOptions.asyncBufferCapacity)
              : new AsyncLookupJoinRunner(
                  generatedFetcher.tableFunc(),
                  fetcherConverter,
                  generatedResultFuture,
                  generatedPreFilter,
                  InternalSerializers.create(rightRowType),
                  leftOuterJoin,
                  asyncOptions.asyncBufferCapacity);
      operator = new NativeAsyncLookupJoinOperator(runner, probeType, resultRowType);
    } else {
      GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
          LookupJoinCodeGenerator.generateSyncLookupFunction(
              config,
              classLoader,
              dataTypeFactory,
              probeType,
              tableSourceRowType,
              resultRowType,
              orderedKeys,
              (TableFunction<Object>) lookupFunction,
              tableName,
              planner.getExecEnv().getConfig().isObjectReuseEnabled());
      GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
          LookupJoinCodeGenerator.generateCollector(
              new CodeGeneratorContext(config, classLoader),
              probeType,
              rightRowType,
              resultRowType,
              JavaScalaConversionUtil.toScala(Optional.ofNullable(remainingJoinCondition)),
              JavaScalaConversionUtil.toScala(Optional.empty()),
              true);
      LookupJoinRunner runner =
          generatedCalc != null
              ? new LookupJoinWithCalcRunner(
                  generatedFetcher,
                  generatedCalc,
                  generatedCollector,
                  generatedPreFilter,
                  leftOuterJoin,
                  rightRowType.getFieldCount())
              : new LookupJoinRunner(
                  generatedFetcher,
                  generatedCollector,
                  generatedPreFilter,
                  leftOuterJoin,
                  rightRowType.getFieldCount());
      operator = new NativeLookupJoinOperator(runner, probeType, resultRowType);
    }

    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        operator,
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
