package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalKafkaSinkAdmissionTest {
  static Stream<Arguments> cases() {
    return Stream.of(
            "basic",
            "parallel",
            "not-null",
            "length",
            "partitioner",
            "upsert",
            "upsert-parallel",
            "materialized",
            "cdc",
            "buffer",
            "disabled")
        .flatMap(mode -> Stream.of(false, true).map(compiled -> Arguments.of(mode, compiled)));
  }

  @ParameterizedTest(name = "{0},compiled={1}")
  @MethodSource("cases")
  void ordinaryAndPortablePlansUseTheSameSinkAdmission(String mode, boolean compiled) {
    var environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    var table = StreamTableEnvironment.create(environment);
    table.getConfig().set("table.exec.uid.generation", "DISABLED");
    if (mode.equals("disabled"))
      table.getConfig().set("streamfusion.operator.kafkaSink.enabled", "false");
    if (mode.equals("length"))
      table.getConfig().set("table.exec.sink.type-length-enforcer", "TRIM_PAD");
    if (mode.equals("materialized"))
      table.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
    boolean upsert =
        mode.startsWith("upsert") || mode.equals("materialized") || mode.equals("buffer");
    boolean grouped = upsert || mode.equals("cdc");
    String source =
        mode.equals("length")
            ? "id BIGINT NOT NULL,v VARCHAR(3)"
            : mode.equals("not-null") ? "id BIGINT,v BIGINT" : "id BIGINT NOT NULL,v BIGINT";
    String sink =
        mode.equals("length") ? "id BIGINT NOT NULL,v VARCHAR(3)" : "id BIGINT NOT NULL,v BIGINT";
    if (upsert) sink += ",PRIMARY KEY(id) NOT ENFORCED";
    table.executeSql(
        "CREATE TABLE src(" + source + ") WITH ('connector'='datagen','number-of-rows'='1')");
    String options =
        upsert
            ? "'connector'='upsert-kafka','key.format'='json','value.format'='json'"
            : "'connector'='kafka','format'='"
                + (mode.equals("cdc") ? "debezium-json" : "json")
                + "'";
    options += ",'topic'='output','properties.bootstrap.servers'='broker:9092'";
    if (mode.equals("basic"))
      options +=
          ",'sink.delivery-guarantee'='exactly-once','sink.transactional-id-prefix'='canonical-test'";
    if (mode.equals("parallel") || mode.equals("upsert-parallel"))
      options += ",'sink.parallelism'='2'";
    if (mode.equals("partitioner")) options += ",'sink.partitioner'='round-robin'";
    if (mode.equals("buffer"))
      options += ",'sink.buffer-flush.max-rows'='2','sink.buffer-flush.interval'='1 s'";
    table.executeSql("CREATE TABLE output(" + sink + ") WITH (" + options + ")");
    NativePlanner.install(table);
    String query =
        grouped
            ? "INSERT INTO output SELECT id,COUNT(*) FROM src GROUP BY id"
            : "INSERT INTO output SELECT * FROM src";
    String explain;
    if (compiled) {
      var plan = table.compilePlanSql(query);
      assertFalse(plan.asJsonString().contains("tech.streamfusion"));
      explain = plan.explain(ExplainDetail.JSON_EXECUTION_PLAN);
    } else explain = table.explainSql(query, ExplainDetail.JSON_EXECUTION_PLAN);
    boolean expected =
        mode.equals("basic")
            || mode.equals("parallel")
            || mode.equals("upsert")
            || mode.equals("cdc");
    assertEquals(expected, explain.contains("native-kafka-serialization"), explain);
    if (expected) {
      assertTrue(explain.contains(": Writer"), explain);
      if (mode.equals("basic")) assertTrue(explain.contains(": Committer"), explain);
    }
  }
}
