package tech.streamfusion;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

class CanonicalNormalizerRowKeyJobTest extends CanonicalNormalizerJobTest {
  private boolean joinConsumer;

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> joinConsumerCases() {
    return CanonicalTestMatrix.select(
        java.util.stream.Stream.of(
                "immediate", "mini", "no-input", "empty", "checkpoint", "rescale")
            .flatMap(
                mode ->
                    handoffs()
                        .filter(args -> !mode.equals("checkpoint") || (boolean) args.get()[0])
                        .map(
                            args ->
                                org.junit.jupiter.params.provider.Arguments.of(
                                    mode, args.get()[0], args.get()[1]))));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("joinConsumerCases")
  @org.junit.jupiter.api.Timeout(value = 8, unit = java.util.concurrent.TimeUnit.MINUTES)
  void normalizerAndJoinRetainBothOwnersAcrossCanonicalSwitches(
      String mode, boolean nativeFirst, String backend) throws Exception {
    joinConsumer = true;
    check(mode, nativeFirst, backend);
  }

  boolean nested() {
    return false;
  }

  @Override
  String rescaleFamilies() {
    return joinConsumer ? "Normalize,UpdatingJoin" : "Normalize,GroupAgg";
  }

  @Override
  java.util.Map<String, Long> expected(Path root, String mode) throws Exception {
    if (joinConsumer) return super.expected(root, mode);
    var retained = new java.util.HashMap<String, String>();
    for (String line : Files.readAllLines(oracleInput(root))) {
      var fields = line.split(",", -1);
      if (fields[0].equals("DELETE")) retained.remove(fields[1]);
      else retained.put(fields[1], fields[2]);
    }
    var counts = new java.util.HashMap<String, Long>();
    for (var value : retained.values()) counts.merge(value, 1L, Long::sum);
    var result = new java.util.HashMap<String, Long>();
    counts.forEach((value, count) -> result.put(value + "|" + count + "|0", 1L));
    return result;
  }

  @Override
  Path oracleInput(Path root) {
    return root.resolve("oracle-input.csv");
  }

  @Override
  Path prepare(String name, String mode) throws Exception {
    var root = super.prepare(name, mode);
    Files.copy(root.resolve("input.csv"), oracleInput(root));
    var keyType =
        nested()
            ? RowType.of(
                new BigIntType(),
                RowType.of(new VarCharType(100), new TimestampType(9)),
                new DecimalType(38, 2))
            : RowType.of(new BigIntType(), new VarCharType(100));
    var rowType = RowType.of(keyType, new BigIntType());
    var serializer = new RowDataSerializer(rowType);
    var encoded = new ArrayList<String>();
    for (String line : Files.readAllLines(oracleInput(root))) {
      var fields = line.split(",", -1);
      long id = Long.parseLong(fields[1]);
      RowData key =
          nested()
              ? GenericRowData.of(
                  id,
                  id % 2 == 0
                      ? null
                      : GenericRowData.of(
                          id % 3 == 0 ? null : StringData.fromString("雪" + id),
                          TimestampData.fromEpochMillis(-1234L - id, 456789)),
                  id % 2 == 0
                      ? null
                      : DecimalData.fromBigDecimal(
                          new BigDecimal("123456789012345678901234567890123456.78"), 38, 2))
              : GenericRowData.of(id, id % 2 == 0 ? null : StringData.fromString("雪" + id));
      var row =
          GenericRowData.ofKind(
              RowKind.valueOf(fields[0]),
              key,
              fields[2].equals("null") ? null : Long.parseLong(fields[2]));
      var output = new DataOutputSerializer(256);
      serializer.serialize(row, output);
      encoded.add(
          "BINARY," + java.util.Base64.getEncoder().encodeToString(output.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    Files.writeString(
        root.resolve("input_table-columns.sql"),
        "k "
            + (nested()
                ? "ROW<i BIGINT,n ROW<s VARCHAR(100),t TIMESTAMP(9)>,d DECIMAL(38,2)>"
                : "ROW<i BIGINT,s VARCHAR(100)>")
            + " NOT NULL,v BIGINT,PRIMARY KEY(k) NOT ENFORCED");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT v,COUNT(*),CAST(0 AS BIGINT) FROM input_table GROUP BY v");
    if (joinConsumer) {
      Files.writeString(
          root.resolve("query.sql"),
          "INSERT INTO output_table SELECT l.k.i,l.v,r.v FROM input_table l JOIN right_input_table"
              + " r ON l.v=r.k");

    } else Files.delete(root.resolve("right-input.csv"));
    Files.writeString(
        root.resolve("native-operator"),
        "NativeChangelogNormalize,"
            + (joinConsumer ? "NativeColumnarUpdatingJoin" : "NativeColumnarGroupAgg")
            + ",RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), rescaleFamilies());
    Files.writeString(root.resolve("uid-generation"), "ALWAYS");
    return root;
  }
}
