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
    return table instanceof org.apache.flink.table.planner.plan.schema.TableSourceTable
        || table
                instanceof
                org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable<?> legacy
            && legacy.tableSource()
                instanceof org.apache.flink.table.sources.LookupableTableSource<?>;
  }

  public static LookupKeys lookupKeys(StreamPhysicalLookupJoin join) {
    Map<Integer, LookupJoinUtil.LookupKey> keys = new HashMap<>();
    scala.collection.JavaConverters.mapAsJavaMapConverter(join.allLookupKeys())
        .asJava()
        .forEach((index, param) -> keys.put((Integer) index, param));
    return new LookupKeys(keys);
  }

  public static String unsupportedKeyShape(LookupKeys keys) {
    for (LookupJoinUtil.LookupKey key : keys.values().values()) {
      if (!(key instanceof LookupJoinUtil.FieldRefLookupKey)
          && !(key instanceof LookupJoinUtil.ConstantLookupKey)) {
        return "lookup join: unsupported lookup key shape " + key.getClass().getSimpleName();
      }
    }
    return null;
  }

  public static LookupAsyncOptions asyncOptions(StreamPhysicalLookupJoin join) {
    if (join.asyncOptions().isEmpty()) return null;
    var options = join.asyncOptions().get();
    return new LookupAsyncOptions(
        options.asyncBufferCapacity, options.asyncTimeout, false, options.asyncOutputMode);
  }

  public static RexNode preFilter(StreamPhysicalLookupJoin join) {
    return null;
  }

  public static RexNode remainingCondition(StreamPhysicalLookupJoin join) {
    var condition = join.remainingCondition();
    return condition.isDefined() ? condition.get() : null;
  }

  public static boolean preferCustomShuffle(StreamPhysicalLookupJoin join) {
    return false;
  }

  public static ExpandedLookupCalc expandCalc(RexProgram calc) {
    var expanded = FlinkRexUtil.expandRexProgram(calc);
    return new ExpandedLookupCalc(
        scala.collection.JavaConverters.seqAsJavaListConverter(expanded._1()).asJava(),
        expanded._2().isDefined() ? expanded._2().get() : null);
  }

  public static UserDefinedFunction lookupFunction(
      RelOptTable table,
      LookupKeys keys,
      ClassLoader classLoader,
      boolean async,
      ResultRetryStrategy retry,
      boolean customShuffle) {
    return LookupJoinUtil.getLookupFunction(table, keys.keySet(), classLoader, async, retry);
  }

  public static Transformation<RowData> customShuffle(
      PlannerBase planner,
      RelOptTable table,
      RowType probeType,
      LookupKeys keys,
      Transformation<RowData> rows,
      ChangelogMode mode,
      TransformationMetadata metadata) {
    throw new UnsupportedOperationException("Flink 1.18 has no custom lookup partitioner SPI");
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
            keys.values(),
            LookupJoinUtil.getOrderedLookupKeys(keys.keySet()),
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
        keys.values(),
        LookupJoinUtil.getOrderedLookupKeys(keys.keySet()),
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

    return calc == null
        ? new LookupJoinRunner(fetcher, collector, leftOuter, rightFields)
        : new LookupJoinWithCalcRunner(fetcher, calc, collector, leftOuter, rightFields);
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

    return calc == null
        ? new AsyncLookupJoinRunner(
            fetcher, converter, collector, rightSerializer, leftOuter, capacity)
        : new AsyncLookupJoinWithCalcRunner(
            fetcher, converter, calc, collector, rightSerializer, leftOuter, capacity);
  }

  public static void open(Function function) throws Exception {
    FunctionUtils.openFunction(function, new org.apache.flink.configuration.Configuration());
  }

  public static boolean preFilter(LookupJoinRunner runner, RowData probe) throws Exception {
    return true;
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
    };
  }
}
