package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.CanonicalRowKeyFixture;
import tech.streamfusion.operator.NativeColumnarDeduplicateOperator;
import tech.streamfusion.operator.NativeColumnarKeepLastDeduplicateOperator;
import tech.streamfusion.state.StockBufferedDedupStateContract;
import tech.streamfusion.state.StockDedupStateContract;

class CanonicalDedupRowKeyPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of("proc-first", "proc-last", "row-first", "row-last")
            .flatMap(
                mode ->
                    Stream.of(false, true)
                        .flatMap(
                            mini ->
                                Stream.of(false, true)
                                    .flatMap(
                                        nested ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    compiled ->
                                                        Stream.of(false, true)
                                                            .map(
                                                                ttl ->
                                                                    Arguments.of(
                                                                        mode, mini, nested,
                                                                        compiled, ttl)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void capturedDeduplicationKeepsTheBinaryRowKeyBoundary(
      String mode, boolean mini, boolean nested, boolean compiled, boolean ttl) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", ttl ? "1 h" : "0 ms");
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(mini));
    config.setString("table.exec.mini-batch.size", "3");
    config.setString("table.exec.mini-batch.allow-latency", "1 h");
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k "
            + CanonicalRowKeyFixture.sql(nested)
            + ",v BIGINT,pt AS PROCTIME(),rt AS TO_TIMESTAMP_LTZ(v,3),"
            + "WATERMARK FOR rt AS rt - INTERVAL '1' HOUR) WITH"
            + " ('connector'='canonical-fixture','insert-only'='true')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k.i,v,k.i FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY "
            + (mode.startsWith("proc") ? "pt" : "rt")
            + (mode.endsWith("first") ? " ASC" : " DESC")
            + ") AS rn FROM input_table) WHERE rn=1";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = compiled ? plan.asJsonString() : "";
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(
                operator ->
                    operator instanceof NativeColumnarDeduplicateOperator
                        || operator instanceof NativeColumnarKeepLastDeduplicateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    boolean buffered =
        mode.equals("row-first") && !mini && tech.streamfusion.compat.CanonicalTestCompat.flink22();
    assertInstanceOf(
        buffered ? StockBufferedDedupStateContract.class : StockDedupStateContract.class,
        GroupAggregateStockTestFixture.field(owners.get(0), "stockStateContract"));
    if (buffered)
      assertEquals(true, GroupAggregateStockTestFixture.field(owners.get(0), "binaryRowKeys"));
    assertTrue(
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(tech.streamfusion.operator.RowDataToArrowOperator.class::isInstance)
            .anyMatch(
                operator -> {
                  try {
                    var schema =
                        org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(
                            java.nio.ByteBuffer.wrap(
                                (byte[])
                                    GroupAggregateStockTestFixture.field(operator, "arrowSchema")));
                    return schema.toJson().contains("streamfusion.nested-row-binary");
                  } catch (Exception failure) {
                    throw new AssertionError(failure);
                  }
                }),
        "The actual input boundary must carry binary nested rows");
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
