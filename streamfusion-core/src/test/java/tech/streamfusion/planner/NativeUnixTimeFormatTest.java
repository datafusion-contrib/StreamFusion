package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneId;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.Test;

class NativeUnixTimeFormatTest {
  @Test
  void supportedCallsHaveNoJvmCallback() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var output = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    for (String zone :
        List.of(
            "UTC",
            "GMT+05:30",
            "GMT-12:00",
            "GMT+18:00",
            "Asia/Shanghai",
            "Asia/Kolkata",
            "Pacific/Apia")) {
      for (var type : List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT)) {
        var input = rex.makeInputRef(types.createSqlType(type), 0);
        for (String pattern : List.of("yyyyMMddHHmm", "yyyy-MM-dd HH:mm:ss", "'o''clock'", "")) {
          for (boolean explicit : new boolean[] {false, true}) {
            var call =
                rex.makeCall(
                    output,
                    FlinkSqlOperatorTable.FROM_UNIXTIME,
                    explicit ? List.of(input, rex.makeLiteral(pattern)) : List.of(input));
            var encoded = encode(call, zone);
            assertEquals(36, encoded.kinds()[0]);
            var binding = encoded.udfBinding();
            long[] longs = encoded.longs();
            try {
              assertSame(longs, binding.bind(longs), "formatting must register no JVM callback");
            } finally {
              binding.unbind();
            }
          }
        }
      }
    }
  }

  @Test
  void finiteOffsetTablesMatchTheRunningJdkAtTransitionsAndAcrossTheLongRange() {
    int admitted = 0;
    for (String zoneId : ZoneId.getAvailableZoneIds()) {
      String encoded = NativeUnixTimeFormat.encode("yyyyMMddHHmm", zoneId);
      if (encoded == null) continue;
      admitted++;
      String[] entries = encoded.substring(0, encoded.indexOf('|')).split(",");
      var zone =
          java.util.TimeZone.getTimeZone(java.util.TimeZone.getTimeZone(ZoneId.of(zoneId)).getID());
      var instants = new java.util.ArrayList<Long>();
      instants.addAll(
          List.of(
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              -2208988800001L,
              -2208988800000L,
              0L,
              2114380800000L,
              2145916800000L,
              4102444800000L));
      for (int i = 1; i < entries.length; i++) {
        long millis = Long.parseLong(entries[i].split(":")[0]);
        instants.add(millis - 1);
        instants.add(millis);
        instants.add(millis + 1);
      }
      for (var transition : ZoneId.of(zoneId).getRules().getTransitions()) {
        long millis = transition.getInstant().toEpochMilli();
        instants.add(millis - 1);
        instants.add(millis);
        instants.add(millis + 1);
      }
      var random = new java.util.Random(123);
      for (int i = 0; i < 1000; i++) instants.add(random.nextLong());
      for (long millis : instants) {
        int offset = Integer.parseInt(entries[0]);
        for (int i = 1; i < entries.length; i++) {
          String[] transition = entries[i].split(":");
          if (Long.parseLong(transition[0]) > millis) break;
          offset = Integer.parseInt(transition[1]);
        }
        assertEquals(zone.getOffset(millis), offset, zoneId + " at " + millis);
      }
    }
    assertTrue(admitted > 200, "finite regional zones should use native formatting");
  }

  @Test
  void dynamicPatternsAndTransitionZonesKeepTheBridge() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var string = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var input = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);
    var dynamic =
        rex.makeCall(
            string,
            FlinkSqlOperatorTable.FROM_UNIXTIME,
            List.of(input, rex.makeInputRef(string, 1)));
    assertEquals(17, encode(dynamic, "UTC").kinds()[0]);
    for (String pattern : List.of("yyyyMMddHHmm", "EEEE MMMM yyyy", "yyyy-'")) {
      var call =
          rex.makeCall(
              string,
              FlinkSqlOperatorTable.FROM_UNIXTIME,
              List.of(input, rex.makeLiteral(pattern)));
      assertEquals(17, encode(call, "America/New_York").kinds()[0]);
    }
    assertNull(NativeUnixTimeFormat.encode("yy", "UTC"));
    assertNull(NativeUnixTimeFormat.encode("yyyy-QQ", "UTC"));
    assertNull(NativeUnixTimeFormat.encode("yyyy-'", "UTC"));
  }

  private static RexExpression encode(RexNode call, String zone) throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var encoded = constructor.newInstance();
    var config = TableConfig.getDefault();
    config.setLocalTimeZone(ZoneId.of(zone));
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    configure.invoke(encoded, config);
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    assertTrue((Boolean) emit.invoke(encoded, call), call.toString());
    return encoded;
  }
}
