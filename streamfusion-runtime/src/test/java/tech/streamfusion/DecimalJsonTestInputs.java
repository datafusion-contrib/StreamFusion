package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

final class DecimalJsonTestInputs {
  private DecimalJsonTestInputs() {}

  static TableEnvironment decimals(int precision, int scale) {
    var rows = new ArrayList<Row>();
    var maximum = java.math.BigInteger.TEN.pow(precision).subtract(java.math.BigInteger.ONE);
    for (var unscaled :
        new java.math.BigInteger[] {
          java.math.BigInteger.ZERO,
          java.math.BigInteger.ONE,
          java.math.BigInteger.TEN,
          java.math.BigInteger.valueOf(100),
          java.math.BigInteger.valueOf(-100),
          maximum,
          maximum.negate()
        }) {
      rows.add(Row.of(rows.size(), new BigDecimal(unscaled, scale)));
    }
    rows.add(Row.of(rows.size(), null));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "decimals",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "n"}, Types.INT, Types.BIG_DEC),
            rows.toArray(Row[]::new)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("n", DataTypes.DECIMAL(precision, scale))
            .build());
    return tables;
  }
}
