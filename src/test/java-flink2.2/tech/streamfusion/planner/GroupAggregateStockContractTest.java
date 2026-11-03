package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;

/** Establishes physical released accumulator fields before designing a native projection. */
class GroupAggregateStockContractTest {
  static Stream<Arguments> cases() {
    return Stream.of("arithmetic", "hidden", "count", "star")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(mode, rocks, ttl))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void generatedRetractingAccumulatorsRetainCountsAndWriteAge(String mode, boolean rocks, long ttl)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "false");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.state.ttl", ttl + " ms");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public java.util.Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return java.util.Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready')");
    table.useCatalog("default_catalog");
    String fields =
        mode.equals("arithmetic")
            ? "COUNT(*) AS a,COUNT(v) AS b,SUM(v) AS c,AVG(v) AS d"
            : mode.equals("hidden")
                ? "SUM(v) AS c,AVG(v) AS d"
                : mode.equals("count") ? "COUNT(v) AS a" : "COUNT(*) AS a";
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,"
            + (mode.equals("arithmetic")
                ? "a BIGINT,b BIGINT,c BIGINT,d BIGINT"
                : mode.equals("hidden") ? "c BIGINT,d BIGINT" : "a BIGINT")
            + ") WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        CanonicalRegularJoinPlanTest.translate(
            planner,
            "INSERT INTO output_table SELECT k,"
                + fields
                + " FROM fixture.db.input_table GROUP BY k");
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var node =
        graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                            instanceof KeyedProcessOperator
                        && ((KeyedProcessOperator<?, ?, ?>)
                                    ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                        .getOperator())
                                .getUserFunction()
                            instanceof GroupAggFunction)
            .findFirst()
            .orElseThrow(() -> new AssertionError(graph.getStreamingPlanAsJSON()));
    @SuppressWarnings("unchecked")
    var operator =
        (KeyedProcessOperator<RowData, RowData, RowData>)
            ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    var function = operator.getUserFunction();
    LogicalType[] accTypes = (LogicalType[]) field(function, "accTypes");
    var selector = (RowDataKeySelector) node.getStatePartitioners()[0];
    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0)) {
      harness.setStateBackend(
          rocks ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
      harness.setup((TypeSerializer<RowData>) node.getTypeSerializerOut());
      harness.open();
      harness.setProcessingTime(1000);
      harness.setStateTtlProcessingTime(1000);
      harness.processElement(new StreamRecord<>(GenericRowData.of(1L, null)));
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              harness.getOperator().<RowData>getKeyedStateBackend();
      backend.setCurrentKey(selector.getKey(GenericRowData.of(1L, null)));
      assertAccumulator(function, accTypes, ttl, 1000, expected(mode, true));
      harness.setProcessingTime(1500);
      harness.setStateTtlProcessingTime(1500);
      harness.processElement(new StreamRecord<>(GenericRowData.of(1L, 7L)));
      harness.processElement(new StreamRecord<>(GenericRowData.of(1L, 3L)));
      var retract = GenericRowData.of(1L, 7L);
      retract.setRowKind(RowKind.DELETE);
      harness.processElement(new StreamRecord<>(retract));
      assertAccumulator(function, accTypes, ttl, 1500, expected(mode, false));
      harness.setProcessingTime(1700);
      harness.setStateTtlProcessingTime(1700);
      var cancel = GenericRowData.of(1L, 3L);
      cancel.setRowKind(RowKind.DELETE);
      harness.processElement(new StreamRecord<>(cancel));
      Long[] cancelled = expected(mode, true);
      if (mode.equals("hidden")) cancelled[0] = 0L;
      assertAccumulator(function, accTypes, ttl, 1700, cancelled);
    }
  }

  private static Long[] expected(String mode, boolean initial) {
    switch (mode) {
      case "arithmetic":
        // The optimizer shares COUNT(v) and SUM0(v) across the final SUM/AVG expressions.
        return initial ? new Long[] {1L, 0L, 0L} : new Long[] {2L, 1L, 3L};
      case "hidden":
        return initial ? new Long[] {null, 0L, 0L, 0L, 1L} : new Long[] {3L, 1L, 3L, 1L, 2L};
      case "count":
        return initial ? new Long[] {0L, 1L} : new Long[] {1L, 2L};
      case "star":
        return new Long[] {initial ? 1L : 2L};
      default:
        throw new IllegalArgumentException(mode);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void assertAccumulator(
      Object function, LogicalType[] types, long ttl, long time, Long[] expected) throws Exception {
    org.apache.flink.api.common.state.ValueState<?> state =
        (org.apache.flink.api.common.state.ValueState<?>) field(function, "accState");
    if (ttl > 0) state = (org.apache.flink.api.common.state.ValueState<?>) field(state, "original");
    Object raw = state.value();
    assertNotNull(raw);
    if (ttl > 0) {
      assertInstanceOf(TtlValue.class, raw);
      assertEquals(time, ((TtlValue<?>) raw).getLastAccessTimestamp());
      raw = ((TtlValue<?>) raw).getUserValue();
    }
    RowData row = (RowData) raw;
    assertEquals(
        expected.length,
        row.getArity(),
        "actual accumulator=" + row + "; types=" + Arrays.toString(types));
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) assertTrue(row.isNullAt(i), "physical accumulator " + i);
      else {
        assertFalse(row.isNullAt(i), "physical accumulator " + i);
        assertEquals(expected[i].longValue(), row.getLong(i), "physical accumulator " + i);
      }
    }
  }

  private static Object field(Object target, String name) throws Exception {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException ignored) {
      }
    }
    throw new NoSuchFieldException(name);
  }
}
