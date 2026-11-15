package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTemporalSortPayloadJobTest extends CanonicalTemporalSortJobTest {
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), CanonicalNestedJoinFixture.PAYLOAD);

  static Stream<Arguments> continuedCases() {
    return CanonicalTestMatrix.select(cases(false));
  }

  static Stream<Arguments> recoveryCases() {
    return CanonicalTestMatrix.select(cases(true));
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("require-retained-map-owner"), "stream-exec-temporal-sort");
    Files.writeString(
        root.resolve("input-columns.sql"),
        "k BIGINT,v BIGINT,p "
            + CanonicalFixtureFactory.NestedJoinChecksum.TYPE
            + ",rt TIMESTAMP(3),WATERMARK FOR rt AS SOURCE_WATERMARK()");
    Files.writeString(
        root.resolve("output-columns.sql"),
        "k BIGINT,n BIGINT,total " + CanonicalFixtureFactory.NestedJoinChecksum.TYPE);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,v,p FROM input_table ORDER BY rt");
    var serializer =
        new RowDataSerializer(
            RowType.of(
                new BigIntType(),
                new BigIntType(),
                CanonicalNestedJoinFixture.PAYLOAD,
                new TimestampType(3)));
    var encoded = new ArrayList<String>();
    for (String line : input(mode)) {
      var cells = line.split(",");
      if (cells[0].equals("WATERMARK")) {
        encoded.add(line);
        continue;
      }
      long time = Long.parseLong(cells[3]);
      var row =
          GenericRowData.of(
              cells[1].equals("null") ? null : Long.valueOf(cells[1]),
              cells[2].equals("null") ? null : Long.valueOf(cells[2]),
              CanonicalNestedJoinFixture.payload(time),
              TimestampData.fromEpochMillis(Long.parseLong(cells[4])));
      var bytes = new DataOutputSerializer(256);
      serializer.serialize(row, bytes);
      encoded.add("BINARY," + Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer()));
    }
    Files.write(root.resolve("input.csv"), encoded);
    return root;
  }

  @Override
  List<String> expected(String mode) {
    var result = new ArrayList<String>();
    for (String row : super.expected(mode)) {
      var cells = row.split("\\|");
      result.add(
          encode(
              GenericRowData.of(
                  cells[1].equals("null") ? null : Long.valueOf(cells[1]),
                  cells[2].equals("null") ? null : Long.valueOf(cells[2]),
                  CanonicalNestedJoinFixture.payload(Long.parseLong(cells[3])))));
    }
    return result;
  }

  @Override
  List<String> committedRows(Path root) throws Exception {
    return CanonicalFixtureFactory.committedRecords(root.resolve("committed")).stream()
        .map(bytes -> Base64.getEncoder().encodeToString(bytes))
        .collect(java.util.stream.Collectors.toList());
  }

  private static String encode(RowData row) {
    try {
      var bytes = new DataOutputSerializer(256);
      new RowDataSerializer(OUTPUT).serialize(row, bytes);
      return Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer());
    } catch (java.io.IOException failure) {
      throw new IllegalStateException(failure);
    }
  }
}
