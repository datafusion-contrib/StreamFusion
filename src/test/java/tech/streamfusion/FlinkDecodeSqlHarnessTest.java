package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkDecodeSqlHarnessTest {
  @Test
  void decodesBytesAndReplacesMalformedSequencesLikeTheJdk() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::bytes,
        "SELECT id, DECODE(b, 'UTF-8'), DECODE(b, 'ASCII'), DECODE(b, 'latin1') FROM inputs");
  }

  @Test
  void unverifiedFormsFallBackBeforeExecution() throws Exception {
    NativeParity.assertFallback(
        TextTimeFunctionTestInputs::bytes, "SELECT id, DECODE(b, 'UTF-32') FROM inputs");
  }

  @Test
  void utf16MatchesEverySingleCodeUnitAndRandomMalformedByteStrings() throws Exception {
    NativeParity.assertParity(
        FlinkDecodeSqlHarnessTest::utf16Bytes,
        "SELECT id, DECODE(b, 'UTF-16'), DECODE(b, 'UnicodeBigUnmarked'),"
            + " DECODE(b, 'UnicodeLittleUnmarked') FROM inputs");
  }

  @Test
  void utf16RoundTripsAndPredicateComposition() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, DECODE(ENCODE(s, 'UTF-16'), 'UTF-16'),"
            + " DECODE(ENCODE(s, 'UTF-16BE'), 'UTF-16BE'),"
            + " DECODE(ENCODE(s, 'UTF-16LE'), 'UTF-16LE') FROM inputs"
            + " WHERE DECODE(ENCODE(s, 'UTF-16'), 'UTF-16') <> ''");
  }

  private static TableEnvironment utf16Bytes() {
    List<Row> rows = new ArrayList<>();
    rows.add(Row.of(0, null));
    rows.add(Row.of(1, new byte[0]));
    for (int unit = 0; unit <= 0xffff; unit++) {
      rows.add(Row.of(rows.size(), new byte[] {(byte) (unit >>> 8), (byte) unit}));
    }
    int[] units = {0, 0x61, 0xd7ff, 0xd800, 0xdbff, 0xdc00, 0xdfff, 0xe000, 0xfeff, 0xfffe};
    for (int first : units) {
      for (int second : units) {
        rows.add(
            Row.of(
                rows.size(),
                new byte[] {
                  (byte) (first >>> 8), (byte) first, (byte) (second >>> 8), (byte) second
                }));
        rows.add(Row.of(rows.size(), new byte[] {(byte) (first >>> 8), (byte) first, 0}));
      }
    }
    Random random = new Random(163211);
    for (int i = 0; i < 4096; i++) {
      byte[] bytes = new byte[random.nextInt(19)];
      random.nextBytes(bytes);
      rows.add(Row.of(rows.size(), bytes));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "b"}, Types.INT, Types.PRIMITIVE_ARRAY(Types.BYTE)),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("b", DataTypes.BYTES()).build());
    return tables;
  }

  @Test
  void validUtf8OnlyBatchUsesTheBufferReusePath() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::strings,
        "SELECT id, DECODE(ENCODE(s, 'UTF-8'), 'UTF-8') FROM inputs");
  }

  @Test
  void dynamicCharsetMatchesHost() throws Exception {
    BuiltinFunctionParity.assertParity(
        TextTimeFunctionTestInputs::parameters,
        "SELECT id, DECODE(ENCODE(s, 'UTF-8'), CASE WHEN n > 0 THEN 'UTF-8' ELSE 'ASCII' END) FROM"
            + " inputs");
  }
}
