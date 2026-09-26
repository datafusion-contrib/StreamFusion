package tech.streamfusion.compat;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.*;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.functions.*;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.utils.*;
import org.apache.flink.table.runtime.collector.*;
import org.apache.flink.table.runtime.generated.*;
import org.apache.flink.table.runtime.operators.join.lookup.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/** The released host's lookup code generation and lifecycle APIs, selected at build time. */
public final class FlinkLookupCompat {
  private FlinkLookupCompat() {}

  public static boolean supportsTable(org.apache.calcite.plan.RelOptTable table) {
    return table instanceof org.apache.flink.table.planner.plan.schema.TableSourceTable;
  }

  public static LookupKeys lookupKeys(StreamPhysicalLookupJoin join) {
    Map<Integer, FunctionCallUtil.FunctionParam> keys = new HashMap<>();
    scala.collection.JavaConverters.mapAsJavaMapConverter(join.allLookupKeys())
        .asJava()
        .forEach((index, param) -> keys.put((Integer) index, param));
    return new LookupKeys(keys);
  }

  public static String unsupportedKeyShape(LookupKeys keys) {
    for (FunctionCallUtil.FunctionParam key : keys.values().values()) {
      if (!(key instanceof FunctionCallUtil.FieldRef)
          && !(key instanceof FunctionCallUtil.Constant)) {
        return "lookup join: unsupported lookup key shape " + key.getClass().getSimpleName();
      }
    }
    return null;
  }

  public static LookupAsyncOptions asyncOptions(StreamPhysicalLookupJoin join) {
    if (join.asyncOptions().isEmpty()) return null;
    var options = join.asyncOptions().get();
    return new LookupAsyncOptions(
        options.asyncBufferCapacity,
        options.asyncTimeout,
        options.keyOrdered,
        options.asyncOutputMode);
  }

  public static RexNode preFilter(StreamPhysicalLookupJoin join) {
    return join.finalPreFilterCondition().isDefined() ? join.finalPreFilterCondition().get() : null;
  }

  public static RexNode remainingCondition(StreamPhysicalLookupJoin join) {
    var condition = join.finalRemainingCondition();
    return condition.isDefined() ? condition.get() : null;
  }

  public static boolean preferCustomShuffle(StreamPhysicalLookupJoin join) {
    return join.preferCustomShuffle();
  }

  public static ExpandedLookupCalc expandCalc(RexProgram calc) {
    var expanded = FlinkRexUtil.expandRexProgram(calc);
    return new ExpandedLookupCalc(
        expanded._1(), expanded._2().isDefined() ? expanded._2().get() : null);
  }

  public static UserDefinedFunction lookupFunction(
      RelOptTable table,
      LookupKeys keys,
      ClassLoader classLoader,
      boolean async,
      ResultRetryStrategy retry,
      boolean customShuffle) {
    return LookupJoinUtil.getLookupFunction(
        table, keys.keySet(), classLoader, async, retry, customShuffle);
  }

  public static Transformation<RowData> customShuffle(
      PlannerBase planner,
      RelOptTable table,
      RowType probeType,
      LookupKeys keys,
      Transformation<RowData> rows,
      ChangelogMode mode,
      TransformationMetadata metadata) {
    return LookupJoinUtil.tryApplyCustomShufflePartitioner(
        planner, table, probeType, keys.values(), rows, mode, metadata);
  }

  private static List<FunctionCallUtil.FunctionParam> orderedKeys(LookupKeys keys) {
    List<FunctionCallUtil.FunctionParam> result = new ArrayList<>();
    for (int key : LookupJoinUtil.getOrderedLookupKeys(keys.keySet()))
      result.add(keys.values().get(key));
    return result;
  }

  public static GeneratedAsyncFetcher asyncFetcher(
      ReadableConfig config,
      ClassLoader classLoader,
      DataTypeFactory types,
      RowType probeType,
      RowType tableType,
      RowType resultType,
      LookupKeys keys,
      AsyncTableFunction<Object> function,
      String tableName) {
    var generated =
        LookupJoinCodeGenerator.generateAsyncLookupFunction(
            config,
            classLoader,
            types,
            probeType,
            tableType,
            resultType,
            orderedKeys(keys),
            function,
            tableName);
    return new GeneratedAsyncFetcher(generated.tableFunc(), generated.dataType());
  }

  public static GeneratedFunction<FlatMapFunction<RowData, RowData>> syncFetcher(
      ReadableConfig config,
      ClassLoader classLoader,
      DataTypeFactory types,
      RowType probeType,
      RowType tableType,
      RowType resultType,
      LookupKeys keys,
      TableFunction<Object> function,
      String tableName,
      boolean objectReuse) {
    return LookupJoinCodeGenerator.generateSyncLookupFunction(
        config,
        classLoader,
        types,
        probeType,
        tableType,
        resultType,
        orderedKeys(keys),
        function,
        tableName,
        objectReuse);
  }

  public static LookupJoinRunner syncRunner(
      GeneratedFunction<FlatMapFunction<RowData, RowData>> fetcher,
      GeneratedFunction<FlatMapFunction<RowData, RowData>> calc,
      GeneratedCollector<ListenableCollector<RowData>> collector,
      ReadableConfig config,
      ClassLoader classLoader,
      RexNode preFilter,
      RowType probeType,
      boolean leftOuter,
      int rightFields) {
    var filter =
        org.apache.flink.table.planner.codegen.FilterCodeGenerator.generateFilterCondition(
            config, classLoader, preFilter, probeType);
    return calc == null
        ? new LookupJoinRunner(fetcher, collector, filter, leftOuter, rightFields)
        : new LookupJoinWithCalcRunner(fetcher, calc, collector, filter, leftOuter, rightFields);
  }

  public static AsyncLookupJoinRunner asyncRunner(
      GeneratedFunction<AsyncFunction<RowData, Object>> fetcher,
      DataStructureConverter<RowData, Object> converter,
      GeneratedFunction<FlatMapFunction<RowData, RowData>> calc,
      GeneratedResultFuture<TableFunctionResultFuture<RowData>> collector,
      ReadableConfig config,
      ClassLoader classLoader,
      RexNode preFilter,
      RowType probeType,
      RowDataSerializer rightSerializer,
      boolean leftOuter,
      int capacity) {
    var filter =
        org.apache.flink.table.planner.codegen.FilterCodeGenerator.generateFilterCondition(
            config, classLoader, preFilter, probeType);
    return calc == null
        ? new AsyncLookupJoinRunner(
            fetcher, converter, collector, filter, rightSerializer, leftOuter, capacity)
        : new AsyncLookupJoinWithCalcRunner(
            fetcher, converter, calc, collector, filter, rightSerializer, leftOuter, capacity);
  }

  public static void open(Function function) throws Exception {
    FunctionUtils.openFunction(function, DefaultOpenContext.INSTANCE);
  }

  public static boolean preFilter(LookupJoinRunner runner, RowData probe) throws Exception {
    return runner.preFilter(FilterCondition.Context.INVALID_CONTEXT, probe);
  }

  public static ResultFuture<RowData> resultFuture(CompletableFuture<Collection<RowData>> future) {
    return new ResultFuture<>() {
      @Override
      public void complete(Collection<RowData> result) {
        future.complete(result);
      }

      @Override
      public void completeExceptionally(Throwable failure) {
        future.completeExceptionally(failure);
      }

      @Override
      public void complete(CollectionSupplier<RowData> supplier) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
