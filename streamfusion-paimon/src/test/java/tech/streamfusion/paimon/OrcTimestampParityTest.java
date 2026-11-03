package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** UTC timestamp parity and planning fallback outside verified Arrow timestamp conversions. */
class OrcTimestampParityTest {
  @ParameterizedTest
  @CsvSource({
    "UTC,true",
    "UTC,false",
    "America/New_York,true",
    "America/New_York,false",
    "Europe/Berlin,true",
    "Europe/Berlin,false",
    "Asia/Kolkata,true",
    "Asia/Kolkata,false"
  })
  void timestampsAndStatisticsMatchJava(String zone, boolean legacy) throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(zone));
    try {
      var type =
          new RowType(
              List.of(
                  new DataField(0, "id", DataTypes.INT().notNull()),
                  new DataField(1, "ts", DataTypes.TIMESTAMP(6)),
                  new DataField(2, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))));
      var options =
          Map.of(
              "file.format",
              "orc",
              "write-only",
              "true",
              "changelog-producer",
              "input",
              "orc.timestamp-ltz.legacy.type",
              Boolean.toString(legacy));
      var stock = PaimonMergeEngineTest.table(options, type);
      var nativeTable = PaimonMergeEngineTest.table(options, type);
      var format =
          (NativePaimonOrcFormat)
              org.apache.paimon.format.FileFormat.fromIdentifier(
                  "orc", new org.apache.paimon.options.Options(options));
      if (!zone.equals("UTC")) {
        assertTrue(format.nativeWriterFallbackReason(type).contains("UTC"));
        try (var writer =
            new PaimonMergeEngineTest.Writer(
                stock, false, new PaimonChangelogSinkWriteTest.MemoryState(), 32)) {
          writer.write(
              List.of(
                  GenericRow.of(
                      1,
                      Timestamp.fromEpochMillis(-1, 999000),
                      Timestamp.fromEpochMillis(-1, 999000))));
          writer.commit(1);
        }
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        var read = stock.newReadBuilder();
        PaimonSourceReadTest.assertRead(stock, read, read.newScan().plan().splits(), false);
        return;
      }
      assertNull(format.nativeWriterFallbackReason(type));
      var values =
          List.of(
              "1969-12-31T23:59:59.999999",
              "1969-12-31T23:59:58.999999",
              "1970-01-01T00:00:00.000001",
              "0001-01-01T00:00:00.123456",
              "1582-10-15T23:59:59.999999",
              "2262-04-12T00:00:00.123456",
              "9999-12-31T23:59:59.999999",
              "1900-01-01T00:00:00.123456",
              "1700-02-28T23:59:59.123456",
              "1800-10-04T12:00:00.123456",
              "2200-10-10T12:00:00.123456",
              "2024-03-10T02:30:00.123456",
              "2024-11-03T01:30:00.123456",
              "2024-03-31T02:30:00.123456",
              "2024-10-27T02:30:00.123456");
      List<InternalRow> rows = new java.util.ArrayList<>();
      for (int i = 0; i < values.size(); i++) {
        var instant = LocalDateTime.parse(values.get(i)).toInstant(ZoneOffset.UTC);
        var timestamp =
            Timestamp.fromEpochMillis(instant.toEpochMilli(), instant.getNano() % 1_000_000);
        rows.add(GenericRow.of(i, timestamp, timestamp));
      }
      for (boolean nativeWriter : new boolean[] {false, true}) {
        var table = nativeWriter ? nativeTable : stock;
        assertEquals("orc", table.coreOptions().fileFormatString());
        try (var writer =
            new PaimonMergeEngineTest.Writer(
                table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 32)) {
          writer.write(rows);
          writer.commit(1);
        }
      }
      assertEquals(
          PaimonTestTables.readRows(stock, type), PaimonTestTables.readRows(nativeTable, type));
      var nativeRead = stock.newReadBuilder();
      PaimonSourceReadTest.assertRead(
          stock, nativeRead, nativeRead.newScan().plan().splits(), false);
      var left = PaimonTestTables.dataFiles(stock).values().iterator().next().get(0);
      var right = PaimonTestTables.dataFiles(nativeTable).values().iterator().next().get(0);
      assertEquals(PaimonTestTables.describe(left, type), PaimonTestTables.describe(right, type));
    } finally {
      TimeZone.setDefault(previous);
    }
  }
}
