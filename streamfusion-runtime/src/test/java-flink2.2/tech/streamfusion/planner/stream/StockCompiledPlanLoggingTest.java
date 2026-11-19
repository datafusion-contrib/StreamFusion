package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;
import tech.streamfusion.NativePlannerTestEnvironment;

class StockCompiledPlanLoggingTest {
  @RegisterExtension
  final LoggerAuditingExtension logs =
      new LoggerAuditingExtension(StockCompiledPlanLowering.class, Level.INFO);

  @Test
  void defaultInfoExplainsWhyTheWindowJoinRemainsStock() throws Exception {
    var table = stockEnvironment();
    table.getConfig().set("execution.checkpointing.interval", "1 s");
    table.getConfig().set("execution.checkpointing.unaligned.enabled", "true");
    for (String name : List.of("left_input", "right_input")) {
      table.executeSql(
          "CREATE TABLE "
              + name
              + "(k BIGINT,v BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS rt)"
              + " WITH ('connector'='datagen')");
    }
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    String portable =
        table
            .compilePlanSql(
                "INSERT INTO output_table SELECT l.k,l.v,r.v"
                    + " FROM TABLE(TUMBLE(TABLE left_input,DESCRIPTOR(rt),INTERVAL '3' SECOND)) l"
                    + " JOIN TABLE(TUMBLE(TABLE right_input,DESCRIPTOR(rt),INTERVAL '3' SECOND)) r"
                    + " ON l.k=r.k AND l.window_start=r.window_start AND l.window_end=r.window_end")
            .asJsonString();

    table.getConfig().set("streamfusion.native.enabled", "true");
    String plan = table.loadPlan(PlanReference.fromJsonString(portable)).explain();

    assertFalse(plan.contains("NativeWindowJoin"), plan);
    assertTrue(
        logs.getMessages()
            .contains(
                "Stock compiled plan fallback: window join: canonical exchange reassembly is not"
                    + " yet bound"),
        logs.getMessages().toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void defaultInfoReportsFunctionsAdmittedFromThePortablePlan(boolean jvm) throws Exception {
    var table = stockEnvironment();
    table.executeSql("CREATE TABLE input_table(id INT,s STRING) WITH ('connector'='datagen')");
    table.executeSql("CREATE TABLE output_table(v INT) WITH ('connector'='blackhole')");
    table.createTemporarySystemFunction("JVM_ONLY", PlannerLoggingTest.JvmOnlyFunction.class);
    String expression = jvm ? "JVM_ONLY(id) + CHAR_LENGTH(s)" : "CHAR_LENGTH(s)";
    String portable =
        table
            .compilePlanSql("INSERT INTO output_table SELECT " + expression + " FROM input_table")
            .asJsonString();

    table.getConfig().set("streamfusion.native.enabled", "true");
    String plan = table.loadPlan(PlanReference.fromJsonString(portable)).explain();

    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(
        logs.getMessages().stream()
            .anyMatch(
                message ->
                    message.startsWith("Stock compiled plan NativeCalc functions:")
                        && message.contains("native=[CHAR_LENGTH]")
                        && message.contains(jvm ? "jvm=[JVM_ONLY]" : "jvm=[]")),
        logs.getMessages().toString());
  }

  private static TableEnvironment stockEnvironment() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    // Keep the native-capable planner installed while producing an unchanged stock plan.
    table.getConfig().set("streamfusion.native.enabled", "false");
    return table;
  }
}
