package tech.streamfusion;

import java.math.BigDecimal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Mixed native INT/string and host-exact casts in a Calc. The remaining numeric families and
 * string-length casts use Flink's {@code CastExecutor} through the columnar JVM upcall, including
 * JDK-dependent double rendering and float/double-to-DECIMAL conversion.
 */
class FlinkStringCastSqlHarnessTest {

  @Test
  void numberToStringMatchesHost() throws Exception {
    // The double values include ones Java renders in scientific notation (1.0E-4, 1.23456789E8) and
    // a negative zero — the formatting corners a native port would get wrong.
    NativeParity.assertParity(
        FlinkStringCastSqlHarnessTest::environment,
        "SELECT CAST(l AS STRING) AS ls, CAST(i AS STRING) AS is_, CAST(sm AS STRING) AS ss,"
            + " CAST(ty AS STRING) AS ts, CAST(d AS STRING) AS ds, CAST(f AS STRING) AS fs,"
            + " CAST(dm AS STRING) AS dms FROM t");
  }

  @Test
  void stringToNumberMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkStringCastSqlHarnessTest::environment,
        "SELECT CAST(sl AS BIGINT) AS l, CAST(si AS INT) AS i, CAST(sd AS DOUBLE) AS d,"
            + " CAST(sd AS FLOAT) AS f, CAST(sdec AS DECIMAL(10, 3)) AS dm FROM t");
  }

  @Test
  void stringLengthCastsMatchHost() throws Exception {
    // Narrowing to VARCHAR(2) truncates; CHAR(6) pads with spaces — both the host's own cast rules.
    NativeParity.assertParity(
        FlinkStringCastSqlHarnessTest::environment,
        "SELECT CAST(sl AS VARCHAR(2)) AS v2, CAST(si AS CHAR(6)) AS c6 FROM t");
  }

  @Test
  void floatToDecimalMatchesHost() throws Exception {
    // Previously gated behind the approximate flag; the host executor's BigDecimal.valueOf(double)
    // conversion is byte-exact to Flink by construction.
    NativeParity.assertParity(
        FlinkStringCastSqlHarnessTest::environment,
        "SELECT CAST(d AS DECIMAL(12, 4)) AS dd, CAST(f AS DECIMAL(12, 4)) AS fd FROM t");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"l", "i", "sm", "ty", "d", "f", "dm", "sl", "si", "sd", "sdec"},
                Types.LONG,
                Types.INT,
                Types.SHORT,
                Types.BYTE,
                Types.DOUBLE,
                Types.FLOAT,
                Types.BIG_DEC,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING),
            Row.of(
                1234567890123L,
                -42,
                (short) 7,
                (byte) -3,
                0.0001,
                1.5f,
                new BigDecimal("10.50"),
                "123",
                "-7",
                "2.5",
                "12.345"),
            Row.of(
                -9L,
                2147483647,
                (short) -30000,
                (byte) 100,
                123456789.0,
                -0.0f,
                new BigDecimal("-0.01"),
                "-9223372036854775808",
                "0",
                "1.0E-4",
                "0.001"),
            Row.of(
                0L,
                0,
                (short) 0,
                (byte) 0,
                -0.0,
                3.4e38f,
                new BigDecimal("99999999.99"),
                "42",
                "13",
                "-123456789.25",
                "-99.999"));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("l", DataTypes.BIGINT())
            .column("i", DataTypes.INT())
            .column("sm", DataTypes.SMALLINT())
            .column("ty", DataTypes.TINYINT())
            .column("d", DataTypes.DOUBLE())
            .column("f", DataTypes.FLOAT())
            .column("dm", DataTypes.DECIMAL(10, 2))
            .column("sl", DataTypes.STRING())
            .column("si", DataTypes.STRING())
            .column("sd", DataTypes.STRING())
            .column("sdec", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
