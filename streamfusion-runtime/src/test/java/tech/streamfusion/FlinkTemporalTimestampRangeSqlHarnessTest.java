package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkTemporalTimestampRangeSqlHarnessTest {
  @Test
  void expandedIntermediateDatesCanProduceStringsAndNumbers() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalTimestampRangeSqlHarnessTest::environment,
        "SELECT DATE_FORMAT(TO_TIMESTAMP(s), 'yyyy-MM-dd HH:mm:ss.SSS'), "
            + "CAST(TIMESTAMPADD(MONTH, -1, TO_TIMESTAMP(s)) AS STRING), "
            + "TIMESTAMPDIFF(DAY, TO_TIMESTAMP(s), TIMESTAMP '9999-01-01 00:00:00'), "
            + "TO_TIMESTAMP(s) < TIMESTAMP '9999-01-01 00:00:00' FROM dates");
  }

  @Test
  void unboundedTimestampResultsRemainNative() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalTimestampRangeSqlHarnessTest::environment,
        "SELECT TO_TIMESTAMP(s) FROM dates");
  }

  @Test
  void standaloneRoundingRemainsNative() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalFunctionsSqlHarnessTest::environment,
        "SELECT FLOOR(ts TO MONTH) FROM temporal_inputs");
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "dates",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
            Row.of("0001-01-01 00:00:00.123456"),
            Row.of("1582-10-15 23:59:59.999999"),
            Row.of("1969-12-31 23:59:59.999999"),
            Row.of("9999-01-01 00:00:00.123456"),
            Row.of((String) null)));
    return tables;
  }
}
