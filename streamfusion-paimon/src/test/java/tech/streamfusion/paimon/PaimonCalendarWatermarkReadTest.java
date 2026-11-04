package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.WatermarkExpression;

class PaimonCalendarWatermarkReadTest {
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  void decodedBatchesAndResumedOffsetsRetainCalendarCandidates(String format) throws Exception {
    var path = new Path(Files.createTempDirectory("paimon-calendar-watermark").toUri());
    var io = LocalFileIO.create();
    new FileSystemSchemaManager(io, path)
        .createTable(
            Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("ts", DataTypes.TIMESTAMP(3))
                .options(Map.of("bucket", "-1", "file.format", format))
                .build());
    var table = FileStoreTableFactory.create(io, path);
    List<Long> timestamps =
        Arrays.asList(
            millis("2024-03-30T23:00:00Z"),
            millis("2024-03-31T00:00:00Z"),
            null,
            millis("2024-02-01T00:00:00Z"),
            millis("2024-01-20T00:00:00Z"));
    var builder = table.newStreamWriteBuilder().withCommitUser("calendar-watermark");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int i = 0; i < timestamps.size(); i++) {
        Long time = timestamps.get(i);
        writer.write(GenericRow.of(i, time == null ? null : Timestamp.fromEpochMillis(time)));
      }
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    var read = table.newReadBuilder().withProjection(new int[] {1, 0});
    var type = LogicalTypeConversion.toLogicalType(read.readType());
    var splits = read.newStreamScan().plan().splits();
    assertEquals(1, splits.size());
    for (int skip : new int[] {0, 1, 2, timestamps.size()}) {
      List<Long> actual = new ArrayList<>();
      try (var reader =
          new NativePaimonSplitReader(
              table, read, read.newRead(), 2, 0, WatermarkExpression.subtractMonths(0, 1))) {
        reader.handleSplitsChanges(
            new SplitsAddition<>(List.of(new FileStoreSourceSplit("split", splits.get(0), skip))));
        boolean finished = false;
        while (!finished) {
          var fetched = reader.fetch();
          if (fetched.nextSplit() != null) {
            var record = fetched.nextRecordFromSplit();
            if (record != null) {
              long maxRowtime = Long.MIN_VALUE;
              long maxCandidate = Long.MIN_VALUE;
              try (var root = record.batch().root()) {
                for (var row : RowDataArrowConverter.read(root, type)) {
                  Long time = row.isNullAt(0) ? null : row.getTimestamp(0, 3).getMillisecond();
                  actual.add(time);
                  if (time != null) {
                    maxRowtime = Math.max(maxRowtime, time);
                    maxCandidate = Math.max(maxCandidate, DateTimeUtils.addMonths(time, -1));
                  }
                }
              }
              assertEquals(maxRowtime, record.maxRowtimeMillis());
              assertEquals(maxCandidate, record.batch().sourceWatermarkMillis());
              assertEquals(skip + actual.size(), record.nextOffset());
            }
          }
          finished = !fetched.finishedSplits().isEmpty();
          fetched.recycle();
        }
        assertTrue(reader.nativeFilesRead() > 0);
      }
      assertEquals(timestamps.subList(skip, timestamps.size()), actual);
    }
  }

  private static long millis(String timestamp) {
    return Instant.parse(timestamp).toEpochMilli();
  }
}
