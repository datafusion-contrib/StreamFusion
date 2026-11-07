package tech.streamfusion.operator;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import tech.streamfusion.Native;
import tech.streamfusion.state.FlinkRocksDBOptions;

/**
 * Explicit historical-library fixture generation; ordinary test execution never rewrites fixtures.
 */
public final class OverLegacyFrameFixtureGenerator {
  private OverLegacyFrameFixtureGenerator() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4)
      throw new IllegalArgumentException("library expected-sha256 writer-commit output-json");
    Path library = Path.of(args[0]).toAbsolutePath();
    if (!args[2].matches("[0-9a-f]{40}"))
      throw new IllegalArgumentException("Expected writer commit SHA");
    var digest = MessageDigest.getInstance("SHA-256");
    try (var input = Files.newInputStream(library)) {
      var buffer = new byte[1 << 20];
      int n;
      while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
    }
    var sha = new StringBuilder();
    for (byte value : digest.digest()) sha.append(String.format("%02x", value & 255));
    if (!sha.toString().equals(args[1]))
      throw new IllegalArgumentException("Historical library hash mismatch");
    if (!Path.of(System.getProperty("java.library.path"))
        .toRealPath()
        .equals(library.getParent().toRealPath()))
      throw new IllegalArgumentException(
          "java.library.path must name only the historical library directory");
    Map<String, String> fixtures = new LinkedHashMap<>();
    for (int code : new int[] {0, 2, 4, 5, 6, 1})
      for (int frame : new int[] {1, 2})
        for (boolean proctime : new boolean[] {false, true}) {
          if (proctime && frame == 2) continue;
          for (boolean negativeZero : new boolean[] {false, true}) {
            LogicalType type =
                code == 0
                    ? new BigIntType()
                    : code == 2
                        ? new IntType()
                        : code == 4
                            ? new SmallIntType()
                            : code == 5
                                ? new TinyIntType()
                                : code == 6 ? new FloatType() : new DoubleType();
            Object value = null;
            if (negativeZero) {
              switch (code) {
                case 0:
                  value = 0L;
                  break;
                case 2:
                  value = 0;
                  break;
                case 4:
                  value = (short) 0;
                  break;
                case 5:
                  value = (byte) 0;
                  break;
                case 6:
                  value = -0.0f;
                  break;
                default:
                  value = -0.0d;
              }
            }
            var input = RowType.of(new BigIntType(), new BigIntType(), type);
            long handle =
                Native.createOverAggregator(
                    new int[] {code},
                    new int[] {0},
                    1,
                    new int[] {2},
                    new int[] {0},
                    frame,
                    frame == 1 ? 2 : 10000,
                    proctime,
                    0,
                    1 << 24);
            try {
              push(handle, proctime, input, List.of(GenericRowData.of(1L, 100L, value)), 100);
              fixtures.put(
                  "primitive-" + code + "-" + frame + "-" + proctime + "-" + negativeZero,
                  snapshot(handle));
            } finally {
              Native.closeOverAggregator(handle);
            }
          }
        }
    for (int frame : new int[] {1, 2})
      for (boolean proctime : new boolean[] {false, true}) {
        if (proctime && frame == 2) continue;
        var input =
            RowType.of(
                new BigIntType(),
                new BigIntType(),
                new FloatType(),
                new VarCharType(),
                new DecimalType(12, 2));
        long handle =
            Native.createOverAggregator(
                new int[] {6, 3, 3202},
                new int[] {0, 5, 22},
                1,
                new int[] {2, 3, 4},
                new int[] {0},
                frame,
                2,
                proctime,
                0,
                1 << 24);
        try {
          var decimal = DecimalData.fromBigDecimal(new BigDecimal("0.03"), 12, 2);
          push(
              handle,
              proctime,
              input,
              List.of(
                  GenericRowData.of(1L, 1000L, null, StringData.fromString("kept"), decimal),
                  GenericRowData.of(1L, 1001L, 2.0f, StringData.fromString("kept"), decimal)),
              1001);
          fixtures.put("mixed-" + frame + "-" + proctime, snapshot(handle));
        } finally {
          Native.closeOverAggregator(handle);
        }
      }
    Path directory = Files.createTempDirectory("over-legacy-frame-");
    var input = RowType.of(new BigIntType(), new BigIntType(), new FloatType());
    try (var root = RowDataArrowConverter.write(List.of(), input, NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
      long handle =
          Native.createRocksDBOverAggregator(
              new int[] {6},
              new int[] {0},
              1,
              new int[] {2},
              new int[] {0},
              1,
              2,
              false,
              new int[] {-1},
              0,
              0,
              1 << 24,
              schema.memoryAddress(),
              directory.resolve("db").toString(),
              8,
              FlinkRocksDBOptions.from(new Configuration()).json(),
              0,
              new String[0],
              new String[0],
              0,
              7,
              false,
              new byte[0][]);
      try {
        push(handle, false, input, List.of(GenericRowData.of(1L, 1000L, null)), 1000);
        Path checkpoint = directory.resolve("checkpoint");
        Native.checkpointRocksDBOverAggregator(handle, checkpoint.toString());
        RocksDB.loadLibrary();
        try (var options = new Options();
            var db = RocksDB.openReadOnly(options, checkpoint.toString());
            var rows = db.newIterator()) {
          for (rows.seekToFirst(); rows.isValid(); rows.next())
            if (rows.key().length >= 21 && rows.key()[4] == 2)
              fixtures.put(
                  "rocks-unmarked-null-f32", Base64.getEncoder().encodeToString(rows.value()));
          rows.status();
        }
      } finally {
        Native.closeRocksDBOverAggregator(handle);
      }
    } finally {
      try (var paths = Files.walk(directory)) {
        for (var path :
            paths
                .sorted(java.util.Comparator.reverseOrder())
                .collect(java.util.stream.Collectors.toList())) Files.delete(path);
      }
    }
    var mixedType = RowType.of(new BigIntType(), new BigIntType(), new DoubleType());
    var mixedRows =
        List.<RowData>of(
            GenericRowData.of(1L, 1000L, 9007199254740992.0),
            GenericRowData.of(1L, 1001L, 1.0),
            GenericRowData.of(1L, 1002L, -9007199254740992.0));
    long mixedHandle =
        Native.createOverAggregator(
            new int[] {1, 1, 1, 1},
            new int[] {0, 22, 3, 4},
            1,
            new int[] {2, 2, 2, 2},
            new int[] {0},
            1,
            2,
            false,
            0,
            1 << 24);
    try {
      push(mixedHandle, false, mixedType, mixedRows, 1002);
      fixtures.put("memory-v1-mixed-float", snapshot(mixedHandle));
    } finally {
      Native.closeOverAggregator(mixedHandle);
    }
    Path mixedDirectory = Files.createTempDirectory("over-legacy-mixed-frame-");
    try (var root = RowDataArrowConverter.write(List.of(), mixedType, NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, root.getSchema(), NativeAllocator.DICTIONARIES, schema);
      long handle =
          Native.createRocksDBOverAggregator(
              new int[] {1, 1, 1, 1},
              new int[] {0, 22, 3, 4},
              1,
              new int[] {2, 2, 2, 2},
              new int[] {0},
              1,
              2,
              false,
              new int[] {-1},
              0,
              0,
              1 << 24,
              schema.memoryAddress(),
              mixedDirectory.resolve("db").toString(),
              8,
              FlinkRocksDBOptions.from(new Configuration()).json(),
              0,
              new String[0],
              new String[0],
              0,
              7,
              false,
              new byte[0][]);
      try {
        push(handle, false, mixedType, mixedRows, 1002);
        Path checkpoint = mixedDirectory.resolve("checkpoint");
        Native.checkpointRocksDBOverAggregator(handle, checkpoint.toString());
        try (var options = new Options();
            var db = RocksDB.openReadOnly(options, checkpoint.toString());
            var rows = db.newIterator()) {
          int index = 0;
          for (rows.seekToFirst(); rows.isValid(); rows.next())
            if (rows.key().length >= 21 && rows.key()[4] == 2) {
              byte[] value = rows.value();
              if (value.length < 5
                  || !Arrays.equals(Arrays.copyOf(value, 5), new byte[] {83, 70, 79, 86, 1}))
                throw new IllegalStateException("Expected historical V1 frame");
              fixtures.put(
                  "rocks-v1-mixed-float-" + index++, Base64.getEncoder().encodeToString(value));
            }
          rows.status();
          if (index != 3) throw new IllegalStateException("Expected three historical frame rows");
        }
      } finally {
        Native.closeRocksDBOverAggregator(handle);
      }
    } finally {
      try (var paths = Files.walk(mixedDirectory)) {
        for (var path :
            paths
                .sorted(java.util.Comparator.reverseOrder())
                .collect(java.util.stream.Collectors.toList())) Files.delete(path);
      }
    }
    if (!fixtures.containsKey("rocks-unmarked-null-f32"))
      throw new IllegalStateException("Historical frame not found");
    var json =
        new StringBuilder(
            "{\n  \"writer_commit\": \""
                + args[2]
                + "\",\n  \"writer_library_sha256\": \""
                + sha
                + "\",\n  \"fixtures\": {\n");
    int i = 0;
    for (var entry : fixtures.entrySet()) {
      if (i++ > 0) json.append(",\n");
      json.append("    \"")
          .append(entry.getKey())
          .append("\": \"")
          .append(entry.getValue())
          .append("\"");
    }
    json.append("\n  }\n}\n");
    Files.writeString(Path.of(args[3]), json);
    System.out.println("Generated " + fixtures.size() + " historical fixtures");
  }

  private static String snapshot(long handle) {
    byte[][] partitions = Native.snapshotOverAggregatorPartitions(handle, 8, new int[] {-1});
    if (partitions.length != 1) throw new IllegalStateException("One fixture partition required");
    return Base64.getEncoder()
        .encodeToString(Arrays.copyOfRange(partitions[0], 4, partitions[0].length));
  }

  private static void push(
      long handle, boolean proctime, RowType type, List<RowData> rows, long watermark) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var input = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, input, schema);
      if (proctime)
        Native.pushProctimeOverAggregator(
            handle,
            input.memoryAddress(),
            schema.memoryAddress(),
            0,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      else {
        Native.pushOverAggregator(handle, input.memoryAddress(), schema.memoryAddress(), 0);
        Native.flushOverAggregator(
            handle, watermark, 0, output.memoryAddress(), outputSchema.memoryAddress());
      }
      try (var ignored =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {}
    }
  }
}
