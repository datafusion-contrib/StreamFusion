package tech.streamfusion;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalWindowRankPayloadJobTest extends CanonicalWindowRankJobTest {
  static Stream<Arguments> continuedCases() {
    return CanonicalTestMatrix.select(CanonicalWindowRankJobTest.continuedCases());
  }

  static Stream<Arguments> recoveryCases() {
    return CanonicalTestMatrix.select(CanonicalWindowRankJobTest.recoveryCases());
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("nested-rank-checksum"), "map");
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,p "
            + CanonicalFixtureFactory.NestedJoinChecksum.TYPE
            + ",rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,rank_checksum(p,CAST(1 AS BIGINT)) FROM"
            + " (SELECT k,v,p,ROW_NUMBER() OVER (PARTITION BY window_start,window_end,k ORDER BY "
            + (deduplicate() ? "rt ASC" : "v ASC NULLS LAST")
            + ") AS rn FROM TABLE(TUMBLE(TABLE input_table,DESCRIPTOR(rt),INTERVAL '3' SECOND)))"
            + " WHERE rn <= "
            + (deduplicate() ? "1" : "3"));
    var serializer =
        new RowDataSerializer(
            RowType.of(
                new BigIntType(),
                new BigIntType(),
                CanonicalNestedJoinFixture.PAYLOAD,
                new TimestampType(3)));
    var rows = new ArrayList<String>();
    for (String line : input(mode)) {
      var fields = line.split(",");
      if (fields[0].equals("WATERMARK")) {
        rows.add(line);
        continue;
      }
      var row =
          GenericRowData.of(
              fields[1].equals("null") ? null : Long.valueOf(fields[1]),
              fields[2].equals("null") ? null : Long.valueOf(fields[2]),
              CanonicalNestedJoinFixture.payload(Long.parseLong(fields[3])),
              TimestampData.fromEpochMillis(Long.parseLong(fields[4])));
      var bytes = new DataOutputSerializer(256);
      serializer.serialize(row, bytes);
      rows.add("BINARY," + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), rows);
    return root;
  }

  @Override
  List<String> expected(String mode) {
    var rows = new ArrayList<String>();
    for (String line : super.expected(mode)) {
      var fields = line.split("\\|");
      rows.add(
          fields[0]
              + "|"
              + fields[1]
              + "|"
              + fields[2]
              + "|"
              + (CanonicalNestedJoinFixture.checksum(Long.parseLong(fields[3])) * 31 + 1));
    }
    Collections.sort(rows);
    return rows;
  }
}
