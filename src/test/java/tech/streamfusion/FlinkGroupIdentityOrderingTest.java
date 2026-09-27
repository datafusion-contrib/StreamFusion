package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import tech.streamfusion.planner.NativePlanner;

class FlinkGroupIdentityOrderingTest {
  @org.junit.jupiter.api.Test
  void collidingBinaryGroupsAllowMultipleHostOverflowResults() throws Exception {
    int keys = 16, bundle = 31, seed = 0;
    long[] collisionKeys = {
      -3729342630220267520L,
      -3973892085165064191L,
      -1640024491654381566L,
      5005413500177088515L,
      3039188583148683268L,
      1967933004047187973L,
      5782700983301701638L,
      -5500902612733526009L,
      -4136263839186419704L,
      -7354755839380422647L,
      1515539859626786826L,
      -9082972510886035445L,
      3989303523039772684L,
      -5456799350212526067L,
      2786763313529225230L,
      -600928429369458673L
    };
    for (long value : collisionKeys) {
      var row = new org.apache.flink.table.data.binary.BinaryRowData(1);
      var writer = new org.apache.flink.table.data.writer.BinaryRowWriter(row);
      writer.writeLong(0, value);
      writer.complete();
      assertEquals(0, row.hashCode());
    }
    Supplier<TableEnvironment> source =
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
          BigDecimal large = BigDecimal.TEN.pow(37).multiply(BigDecimal.valueOf(9));
          List<Row> rows = new ArrayList<>();
          for (int repeat = 0; repeat < 3; repeat++) {
            for (BigDecimal value :
                new BigDecimal[] {
                  large, large.subtract(BigDecimal.valueOf(3)), large.negate(), null
                }) {
              for (int key = 0; key < keys; key++) rows.add(Row.of(collisionKeys[key], value));
            }
          }
          java.util.Collections.shuffle(rows, new java.util.Random(seed));
          table.createTemporaryView(
              "src",
              table.fromDataStream(
                  fromData(
                      env,
                      rows,
                      Types.ROW_NAMED(new String[] {"k", "d"}, Types.LONG, Types.BIG_DEC)),
                  Schema.newBuilder().column("k", BIGINT()).column("d", DECIMAL(38, 0)).build()));
          return table;
        };
    var results = new java.util.HashSet<java.util.Map<java.util.List<Object>, Long>>();
    for (int trial = 0; trial < 9; trial++) {
      var table = source.get();
      String sql = "SELECT k, SUM(DISTINCT d), COUNT(*) FROM src GROUP BY k";
      String plan = NativePlanner.explain(source.get(), sql);
      assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
      assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      var scan = trial == 8 ? NativePlanner.install(table) : null;
      var result = new java.util.HashMap<java.util.List<Object>, Long>();
      try (var rows = table.executeSql(sql).collect()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          var fields = new java.util.ArrayList<Object>();
          for (int i = 0; i < row.getArity(); i++) fields.add(row.getField(i));
          int sign =
              row.getKind() == org.apache.flink.types.RowKind.INSERT
                      || row.getKind() == org.apache.flink.types.RowKind.UPDATE_AFTER
                  ? 1
                  : -1;
          long count = result.getOrDefault(fields, 0L) + sign;
          if (count == 0) result.remove(fields);
          else result.put(fields, count);
        }
      }
      if (scan != null) assertTrue(scan.substitutions() > 0);
      results.add(result);
    }
    BigDecimal large = BigDecimal.TEN.pow(37).multiply(BigDecimal.valueOf(9));
    // JVM identity hashes vary, so the number of observed result sets is not an invariant.
    for (var result : results) {
      assertEquals(keys, result.size());
      result.forEach(
          (fields, multiplicity) -> {
            assertEquals(1L, multiplicity);
            assertEquals(12L, fields.get(2));
            assertTrue(
                fields.get(1).equals(large.negate())
                    || fields.get(1).equals(large.subtract(BigDecimal.valueOf(3))));
          });
    }
  }
}
