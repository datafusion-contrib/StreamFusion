package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.Native;

/** Isolates acknowledged append and broker fetch/decode from Flink and query execution. */
@EnabledIfEnvironmentVariable(named = "SF_FLUSS_TRANSPORT_BENCH", matches = "true")
class FlussTransportBenchmark {
  @Test
  void transport() throws Exception {
    assertTrue(
        System.getProperty("java.library.path").contains("release"),
        "Use -Pbench for measurements");
    int batchRows = Integer.getInteger("fluss.transport.batchRows", 8192);
    int batches = Integer.getInteger("fluss.transport.batches", 128);
    int runs = Integer.getInteger("fluss.transport.runs", 3);
    boolean async = Boolean.getBoolean("fluss.transport.async");
    boolean wide = Boolean.getBoolean("fluss.transport.wide");
    long rows = Math.multiplyExact((long) batchRows, batches);
    var fields =
        org.apache.fluss.metadata.Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT());
    if (wide)
      fields
          .column("payload", DataTypes.STRING())
          .column("created", DataTypes.TIMESTAMP(3))
          .column("price", DataTypes.DECIMAL(18, 3));
    var tableSchema = fields.build();
    Schema schema = FlussArrowSchema.wireSchema(tableSchema.getRowType());
    Schema nativeSchema =
        new Schema(
            schema.getFields().stream()
                .map(
                    field ->
                        field.getType()
                                instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
                            ? tech.streamfusion.arrow.TimestampAccessor.field(
                                field.getName(), field.isNullable())
                            : field)
                .toList());
    try (FlussTestCluster cluster = new FlussTestCluster()) {
      cluster.start();
      try (var connection = ConnectionFactory.createConnection(cluster.config());
          RootAllocator allocator = new RootAllocator();
          VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        connection.getAdmin().createDatabase("transport", DatabaseDescriptor.EMPTY, true).get();
        root.allocateNew();
        GenericRow[] stockInput = new GenericRow[batchRows];
        for (int i = 0; i < batchRows; i++) {
          ((BigIntVector) root.getVector(0)).setSafe(i, i);
          ((BigIntVector) root.getVector(1)).setSafe(i, i * 31L);
          if (wide) {
            String payload = i % 7 == 0 ? null : payload(i);
            Long timestamp = i % 11 == 0 ? null : 1609459200000L + i;
            BigDecimal price = i % 13 == 0 ? null : BigDecimal.valueOf(i * 31L, 3);
            if (payload != null)
              ((VarCharVector) root.getVector(2))
                  .setSafe(i, payload.getBytes(StandardCharsets.UTF_8));
            else ((VarCharVector) root.getVector(2)).setNull(i);
            if (timestamp != null) ((TimeStampMilliVector) root.getVector(3)).setSafe(i, timestamp);
            else ((TimeStampMilliVector) root.getVector(3)).setNull(i);
            if (price != null) ((DecimalVector) root.getVector(4)).setSafe(i, price);
            else ((DecimalVector) root.getVector(4)).setNull(i);
            stockInput[i] =
                GenericRow.of(
                    (long) i,
                    i * 31L,
                    payload == null ? null : BinaryString.fromString(payload),
                    timestamp == null ? null : TimestampNtz.fromMillis(timestamp),
                    price == null ? null : Decimal.fromBigDecimal(price, 18, 3));
          } else stockInput[i] = GenericRow.of((long) i, i * 31L);
        }
        root.setRowCount(batchRows);
        try (var nativeInput = FlussArrowSchema.convert(root, nativeSchema, allocator)) {
          long expected = batches * (long) batchRows * (batchRows - 1) / 2;
          for (String compression : List.of("NONE", "LZ4_FRAME", "ZSTD")) {
            for (int run = -1; run < runs; run++) {
              TablePath path =
                  TablePath.of("transport", compression.toLowerCase() + "_" + (run + 1));
              connection
                  .getAdmin()
                  .createTable(
                      path,
                      TableDescriptor.builder()
                          .schema(tableSchema)
                          .distributedBy(1)
                          .property("table.log.format", "ARROW")
                          .property("table.log.arrow.compression.type", compression)
                          .build(),
                      false)
                  .get();
              try (FlussArrowClient writer =
                      new FlussArrowClient(cluster.config(), path, schema, allocator, false);
                  FlussArrowClient reader =
                      new FlussArrowClient(cluster.config(), path, nativeSchema, allocator)) {
                // Warm writer initialization and leader routing before timing acknowledged appends.
                writer.append(nativeInput);
                var writerBefore = writer.transportProfile();
                long start = System.nanoTime();
                for (int i = 0; i < batches; i++) {
                  if (async) writer.appendAsync(nativeInput);
                  else writer.append(nativeInput);
                }
                writer.flush();
                long appendNs = System.nanoTime() - start;
                TableBucket bucket = new TableBucket(reader.tableInfo().getTableId(), 0);
                for (var fetched : reader.fetch(new LogSplit(bucket, null, 0, batchRows))) {
                  try (fetched) {
                    if (run == 0 && fetched.root() != null)
                      reportHandoff(compression, wide, fetched.root(), allocator);
                  }
                }
                var readerBefore = reader.transportProfile();
                long offset = batchRows;
                long sum = 0;
                start = System.nanoTime();
                while (offset < rows + batchRows) {
                  var fetched = reader.fetch(new LogSplit(bucket, null, offset, rows + batchRows));
                  for (var batch : fetched) {
                    try (batch) {
                      if (batch.root() != null) {
                        BigIntVector vector = (BigIntVector) batch.root().getVector(0);
                        for (int i = 0; i < batch.root().getRowCount(); i++) sum += vector.get(i);
                      }
                      offset = batch.nextOffset();
                    }
                  }
                }
                long readNs = System.nanoTime() - start;
                assertEquals(expected, sum);
                long stockSum = 0;
                long stockRows = 0;
                long stockNs;
                try (var scanner = connection.getTable(path).newScan().createLogScanner()) {
                  scanner.subscribe(0, batchRows);
                  start = System.nanoTime();
                  while (stockRows < rows) {
                    for (var record : scanner.poll(Duration.ofSeconds(1))) {
                      stockSum += record.getRow().getLong(0);
                      stockRows++;
                    }
                  }
                  stockNs = System.nanoTime() - start;
                }
                assertEquals(expected, stockSum);
                TablePath stockPath = TablePath.of("transport", path.getTableName() + "_stock");
                connection
                    .getAdmin()
                    .createTable(
                        stockPath,
                        TableDescriptor.builder()
                            .schema(tableSchema)
                            .distributedBy(1)
                            .property("table.log.format", "ARROW")
                            .property("table.log.arrow.compression.type", compression)
                            .build(),
                        false)
                    .get();
                var stockWriter = connection.getTable(stockPath).newAppend().createWriter();
                for (GenericRow row : stockInput) stockWriter.append(row);
                stockWriter.flush();
                start = System.nanoTime();
                for (int batch = 0; batch < batches; batch++)
                  for (GenericRow row : stockInput) stockWriter.append(row);
                stockWriter.flush();
                long stockAppendNs = System.nanoTime() - start;
                long stockBatches = 0;
                try (var stockReader =
                    new FlussArrowClient(cluster.config(), stockPath, schema, allocator)) {
                  long count = 0;
                  long checksum = 0;
                  long stockOffset = batchRows;
                  while (count < rows) {
                    for (var record :
                        stockReader.fetch(
                            new LogSplit(
                                new TableBucket(stockReader.tableInfo().getTableId(), 0),
                                null,
                                stockOffset,
                                rows + batchRows))) {
                      try (record) {
                        if (record.root() != null) {
                          stockBatches++;
                          var ids = (BigIntVector) record.root().getVector(0);
                          for (int i = 0; i < record.root().getRowCount(); i++)
                            checksum += ids.get(i);
                          count += record.root().getRowCount();
                        }
                        stockOffset = record.nextOffset();
                      }
                    }
                  }
                  assertEquals(rows, count);
                  assertEquals(expected, checksum);
                }
                connection.getAdmin().dropTable(stockPath, false).get();
                if (run >= 0)
                  System.out.printf(
                      "FLUSS_TRANSPORT codec=%s run=%d rows=%d batchRows=%d async=%s wide=%s"
                          + " appendMs=%.3f arrowReadMs=%.3f stockReadMs=%.3f stockAppendMs=%.3f"
                          + " stockBatches=%d%n",
                      compression,
                      run,
                      rows,
                      batchRows,
                      async,
                      wide,
                      appendNs / 1e6,
                      readNs / 1e6,
                      stockNs / 1e6,
                      stockAppendNs / 1e6,
                      stockBatches);
                if (run >= 0 && Boolean.getBoolean("streamfusion.fluss.profile")) {
                  var writeProfile = writer.transportProfile();
                  var readProfile = reader.transportProfile();
                  System.out.printf(
                      "FLUSS_STAGES codec=%s run=%d encodeMs=%.3f produceRpcMs=%.3f fetchRpcMs=%.3f"
                          + " decodeMs=%.3f produceRequests=%d receivedRecordBytes=%d"
                          + " borrowedRecordBytes=%d%n",
                      compression,
                      run,
                      (writeProfile.encodeNanos() - writerBefore.encodeNanos()) / 1e6,
                      (writeProfile.produceRpcNanos() - writerBefore.produceRpcNanos()) / 1e6,
                      (readProfile.fetchRpcNanos() - readerBefore.fetchRpcNanos()) / 1e6,
                      (readProfile.decodeNanos() - readerBefore.decodeNanos()) / 1e6,
                      writeProfile.produceRequests() - writerBefore.produceRequests(),
                      readProfile.receivedRecordBytes() - readerBefore.receivedRecordBytes(),
                      readProfile.borrowedRecordBytes() - readerBefore.borrowedRecordBytes());
                }
              }
              connection.getAdmin().dropTable(path, false).get();
            }
          }
        }
      }
    }
  }

  private static String payload(int row) {
    StringBuilder result = new StringBuilder(128);
    long value = row + 1L;
    for (int i = 0; i < 128; i++) {
      value = value * 6364136223846793005L + 1442695040888963407L;
      result.append((char) ('a' + Long.remainderUnsigned(value >>> 32, 26)));
    }
    return result.toString();
  }

  private static void reportHandoff(
      String compression, boolean wide, VectorSchemaRoot root, RootAllocator allocator) {
    int columns = root.getFieldVectors().size();
    int[] indexes = new int[columns];
    String[] names = new String[columns];
    for (int i = 0; i < columns; i++) {
      indexes[i] = i;
      names[i] = root.getVector(i).getName();
    }
    long calc =
        Native.createCalcExpression(
            new int[columns],
            indexes,
            new int[columns],
            new long[0],
            new double[0],
            new String[0],
            indexes,
            -1,
            names);
    long sharedBytes = 0;
    long changedBytes = 0;
    try (var dictionaries = new CDataDictionaryProvider();
        var inputArray = ArrowArray.allocateNew(allocator);
        var inputSchema = ArrowSchema.allocateNew(allocator);
        var outputArray = ArrowArray.allocateNew(allocator);
        var outputSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, inputArray, inputSchema);
      Native.calcExpression(
          calc,
          inputArray.memoryAddress(),
          inputSchema.memoryAddress(),
          outputArray.memoryAddress(),
          outputSchema.memoryAddress());
      try (var result =
          Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries)) {
        assertEquals(root.getRowCount(), result.getRowCount());
        for (int column = 0; column < columns; column++) {
          var source = root.getVector(column);
          var output = result.getVector(column);
          var sourceBuffers = buffers(source);
          var outputBuffers = buffers(output);
          assertEquals(sourceBuffers.size(), outputBuffers.size());
          for (int i = 0; i < sourceBuffers.size(); i++) {
            long bytes = sourceBuffers.get(i).readableBytes();
            if (bytes == 0) continue;
            if (sourceBuffers.get(i).memoryAddress() == outputBuffers.get(i).memoryAddress())
              sharedBytes += bytes;
            else changedBytes += bytes;
          }
          for (int i = 0; i < source.getValueCount(); i++)
            assertEquals(source.getObject(i), output.getObject(i));
        }
      }
    } finally {
      Native.closeCalcExpression(calc);
    }
    System.out.printf(
        "FLUSS_HANDOFF codec=%s wide=%s sharedBufferBytes=%d changedBufferBytes=%d%n",
        compression, wide, sharedBytes, changedBytes);
  }

  private static List<ArrowBuf> buffers(FieldVector vector) {
    List<ArrowBuf> result = new java.util.ArrayList<>(vector.getFieldBuffers());
    for (var child : vector.getChildrenFromFields()) result.addAll(buffers(child));
    return result;
  }
}
