package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcReaderFactory;
import org.apache.paimon.format.orc.OrcTypeUtil;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.shade.org.apache.orc.*;
import org.apache.paimon.shade.org.apache.orc.impl.PhysicalFsWriter;
import org.apache.paimon.shade.org.apache.orc.impl.WriterImpl;
import org.apache.paimon.shade.org.apache.orc.impl.writer.WriterEncryptionVariant;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.orc.NativeOrc;

/**
 * Native-owned Arrow batches through a closed ORC file, including all vector conversion and I/O.
 */
@EnabledIfEnvironmentVariable(named = "SF_ORC_WRITER_COMPARISON", matches = "true")
class OrcWriterComparisonBenchmark {
  private static final int BATCH_ROWS = integer("SF_ORC_WRITER_BATCH_ROWS", 4096);
  private static final String[] BACKENDS = {"java_arrow", "java_vectors", "stock_oracle"};
  private static final String STRATEGY =
      OrcFile.CompressionStrategy.valueOf(
              System.getenv().getOrDefault("SF_ORC_WRITER_STRATEGY", "SPEED"))
          .name();
  private static final Map<String, String> SETTINGS =
      Map.of(
          "stripe.size", "67108864",
          "compress.size", "65536",
          "row.index.stride", "10000",
          "dictionary.key.threshold", "0.8",
          "compression.strategy", STRATEGY,
          "write.format", "0.12");
  @TempDir java.nio.file.Path directory;

  private static int integer(String key, int fallback) {
    int value = Integer.parseInt(System.getenv().getOrDefault(key, Integer.toString(fallback)));
    if (value <= 0) throw new IllegalArgumentException(key + " must be positive");
    return value;
  }

  private static final class Fixture implements AutoCloseable {
    final List<InternalRow> rows;
    final RowType type;
    final Schema arrow;
    final TypeDescription orc;
    final List<Long> batches = new ArrayList<>();

    Fixture(RowType type, List<InternalRow> rows) {
      this.rows = rows;
      this.type = type;
      var flink =
          (org.apache.flink.table.types.logical.RowType) LogicalTypeConversion.toLogicalType(type);
      arrow = PaimonArrowFields.annotate(ArrowConversion.toArrowSchema(flink), type);
      orc = OrcTypeUtil.convertToOrcSchema((RowType) OrcFileFormat.refineDataType(type));
      // Closing this allocator before timing also proves Rust retained no Java-owned data buffers.
      try (var allocator =
          NativeAllocator.SHARED.newChildAllocator("writer-fixture", 0, Long.MAX_VALUE)) {
        for (int at = 0; at < rows.size(); at += BATCH_ROWS) {
          List<RowData> input =
              rows.subList(at, Math.min(rows.size(), at + BATCH_ROWS)).stream()
                  .map(r -> (RowData) new FlinkRowData(r))
                  .collect(ListCollectors.toList());
          try (var root = RowDataArrowConverter.write(input, flink, allocator, false);
              var array = ArrowArray.allocateNew(allocator);
              var schema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array);
            Data.exportSchema(allocator, arrow, NativeAllocator.DICTIONARIES, schema);
            batches.add(
                NativeOrc.importWriterComparisonBatch(
                    array.memoryAddress(), schema.memoryAddress()));
          }
        }
      } catch (Throwable failure) {
        close();
        throw failure;
      }
    }

    @Override
    public void close() {
      batches.forEach(NativeOrc::closeWriterComparisonBatch);
      batches.clear();
    }
  }

  private Path output(int backend) {
    return new Path(directory.resolve(BACKENDS[backend] + ".orc").toUri());
  }

  private long write(Fixture fixture, String compression, int backend) throws Exception {
    long start = System.nanoTime();
    long handoff = 0, conversion = 0, encoding = 0;
    boolean profile = Boolean.parseBoolean(System.getenv("SF_ORC_WRITER_PROFILE"));
    var io = LocalFileIO.create();
    try (var out = io.newOutputStream(output(backend), true)) {
      if (backend == 0) {
        Map<String, String> options = new LinkedHashMap<>(SETTINGS);
        options.put("compression", compression);
        var codec =
            new tech.streamfusion.orc.OrcCodec(
                fixture.orc.toString(), false, new PaimonOrcWriter());
        try (var encoder =
                codec.createEncoder(
                    fixture.arrow,
                    new int[0],
                    options.keySet().toArray(String[]::new),
                    options.values().toArray(String[]::new),
                    false,
                    out);
            var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
          for (long batch : fixture.batches) {
            NativeOrc.exportWriterComparisonBatch(batch, array.memoryAddress());
            try {
              encoder.write(array.memoryAddress(), new int[0], 0, -1);
            } finally {
              if (array.snapshot().release != 0) array.release();
            }
          }
          encoder.finish();
        }
      } else {
        var properties = new Properties();
        SETTINGS.forEach((k, v) -> properties.setProperty("orc." + k, v));
        properties.setProperty("orc.compress", compression);
        var options =
            OrcFile.writerOptions(properties, new Configuration(false))
                .setSchema(fixture.orc)
                .useUTCTimestamp(true)
                .enforceBufferSize();
        options.physicalWriter(
            new PhysicalFsWriter(
                new FSDataOutputStream(out, null) {
                  @Override
                  public void close() {} // FileIO owns the output, as in Paimon's factory.
                },
                options,
                new WriterEncryptionVariant[0]));
        try (var writer =
            new WriterImpl(null, new org.apache.hadoop.fs.Path(output(backend).toUri()), options)) {
          var batch =
              fixture.orc.createRowBatch(
                  backend == 1
                      ? TypeDescription.RowBatchVersion.USE_DECIMAL64
                      : TypeDescription.RowBatchVersion.ORIGINAL,
                  BATCH_ROWS);
          if (backend == 1) {
            var converter = new ArrowOrcVectors(fixture.arrow.getFields());
            try (var root = VectorSchemaRoot.create(fixture.arrow, NativeAllocator.SHARED);
                var array = ArrowArray.allocateNew(NativeAllocator.SHARED)) {
              for (long nativeBatch : fixture.batches) {
                long stage = profile ? System.nanoTime() : 0;
                NativeOrc.exportWriterComparisonBatch(nativeBatch, array.memoryAddress());
                try {
                  Data.importIntoVectorSchemaRoot(
                      NativeAllocator.SHARED,
                      ArrowArray.wrap(array.memoryAddress()),
                      root,
                      NativeAllocator.DICTIONARIES);
                  if (profile) {
                    handoff += System.nanoTime() - stage;
                    stage = System.nanoTime();
                  }
                  converter.copy(root, batch);
                  if (profile) {
                    conversion += System.nanoTime() - stage;
                    stage = System.nanoTime();
                  }
                  writer.addRowBatch(batch);
                  if (profile) encoding += System.nanoTime() - stage;
                } finally {
                  if (array.snapshot().release != 0) array.release();
                }
              }
            }
          } else {
            var vectorizer = new RowDataVectorizer(fixture.orc, fixture.type.getFields(), true);
            for (var row : fixture.rows) {
              vectorizer.vectorize(row, batch);
              if (batch.size == BATCH_ROWS) {
                writer.addRowBatch(batch);
                batch.reset();
              }
            }
            if (batch.size > 0) writer.addRowBatch(batch);
          }
        }
      }
    }
    long elapsed = System.nanoTime() - start;
    if (profile && backend == 1)
      System.out.printf(
          "JAVA_STAGES,%d,%s,%.3f,%.3f,%.3f,%.3f,%.3f%n",
          fixture.type.getFieldCount(),
          compression,
          handoff / 1e6,
          conversion / 1e6,
          encoding / 1e6,
          (elapsed - handoff - conversion - encoding) / 1e6,
          elapsed / 1e6);
    return elapsed;
  }

  private byte[] fingerprint(Fixture fixture, String compression, int backend) throws Exception {
    var digest = MessageDigest.getInstance("SHA-256");
    try (var reader =
            OrcReaderFactory.createReader(
                new Configuration(false), LocalFileIO.create(), output(backend), null);
        var rows = reader.rows(new Reader.Options())) {
      assertEquals(fixture.rows.size(), reader.getNumberOfRows());
      assertEquals(fixture.orc.toString(), reader.getSchema().toString());
      assertEquals(CompressionKind.valueOf(compression), reader.getCompressionKind());
      assertEquals(10000, reader.getRowIndexStride());
      assertEquals(OrcFile.Version.V_0_12, reader.getFileVersion());
      for (int id = 1; id <= fixture.orc.getMaximumId(); id++)
        assertEquals(
            fixture.orc.findSubtype(id).getAttributeValue("paimon.id"),
            reader.getSchema().findSubtype(id).getAttributeValue("paimon.id"));
      var batch = fixture.orc.createRowBatch(BATCH_ROWS);
      long count = 0;
      while (rows.nextBatch(batch)) {
        count += batch.size;
        for (int r = 0; r < batch.size; r++) {
          for (var column : batch.cols) {
            var value = new StringBuilder();
            column.stringifyValue(value, r);
            byte[] encoded = value.toString().getBytes(StandardCharsets.UTF_8);
            digest.update(java.nio.ByteBuffer.allocate(4).putInt(encoded.length).array());
            digest.update(encoded);
          }
        }
      }
      assertEquals(fixture.rows.size(), count);
    }
    return digest.digest();
  }

  private byte[] verify(Fixture fixture, String compression) throws Exception {
    write(fixture, compression, 2);
    byte[] expected = fingerprint(fixture, compression, 2);
    for (int backend = 0; backend < 2; backend++) {
      write(fixture, compression, backend);
      assertArrayEquals(expected, fingerprint(fixture, compression, backend), BACKENDS[backend]);
    }
    return expected;
  }

  static Stream<Arguments> scalarTypes() {
    return OrcReaderComparisonBenchmark.scalarTypes();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("scalarTypes")
  void scalarCompatibility(
      org.apache.paimon.types.DataType valueType, List<?> values, String format) throws Exception {
    var type =
        RowType.of(
            new org.apache.paimon.types.DataField(
                0, "id", org.apache.paimon.types.DataTypes.INT().notNull()),
            new org.apache.paimon.types.DataField(1, "value", valueType));
    List<InternalRow> rows = new ArrayList<>();
    rows.add(GenericRow.of(0, null));
    for (int i = 0; i < values.size(); i++) rows.add(GenericRow.of(i + 1, values.get(i)));
    try (var fixture = new Fixture(type, rows)) {
      verify(fixture, "NONE");
      verify(fixture, "ZSTD");
    }
    assertEquals("", NativeOrc.liveNativeHandles());
  }

  @Test
  void nestedEmptyAndNullValues() throws Exception {
    var type = new RowType(PaimonTestTables.paimonSchema(Map.of()).fields());
    var nulls = new GenericRow(type.getFieldCount());
    nulls.setField(0, 1L);
    var empty = new GenericRow(type.getFieldCount());
    empty.setField(0, 2L);
    empty.setField(1, org.apache.paimon.data.BinaryString.fromString(""));
    empty.setField(7, new org.apache.paimon.data.GenericArray(new Integer[0]));
    empty.setField(8, new org.apache.paimon.data.GenericMap(Map.of()));
    empty.setField(9, GenericRow.of(null, null));
    empty.setField(12, new byte[0]);
    try (var fixture = new Fixture(type, List.of(nulls, empty))) {
      verify(fixture, "NONE");
      verify(fixture, "ZSTD");
    }
    assertEquals("", NativeOrc.liveNativeHandles());
  }

  @Test
  void benchmark() throws Exception {
    int rows = integer("SF_ORC_WRITER_ROWS", 262144);
    int trials = integer("SF_ORC_WRITER_TRIALS", 5);
    int warmups = integer("SF_ORC_WRITER_WARMUPS", 2);
    String[] codecs =
        System.getenv()
            .getOrDefault("SF_ORC_WRITER_CODECS", "NONE,ZLIB,SNAPPY,LZ4,ZSTD")
            .split(",");
    var fullType = new RowType(PaimonTestTables.paimonSchema(Map.of()).fields());
    int[][] projections = {
      {0, 6, 10, 11}, {0, 1, 12, 13}, java.util.stream.IntStream.range(0, 14).toArray()
    };
    String[] shapes = {"numeric4", "strings4", "full14"};
    System.out.println(
        "ORC_WRITE_HEADER,shape,codec,strategy,backend,rows,batch_rows,trials,median_ms,min_ms,max_ms,file_bytes");
    for (int shape = 0; shape < projections.length; shape++) {
      int[] projection = projections[shape];
      var type = fullType.project(projection);
      var getters =
          Arrays.stream(projection)
              .mapToObj(c -> InternalRow.createFieldGetter(fullType.getTypeAt(c), c))
              .collect(ListCollectors.toList());
      List<InternalRow> input = new ArrayList<>();
      for (var values : PaimonTestTables.values(rows)) {
        var full = PaimonTestTables.paimonRow(values);
        var row = new GenericRow(projection.length);
        for (int c = 0; c < projection.length; c++)
          row.setField(c, getters.get(c).getFieldOrNull(full));
        input.add(row);
      }
      try (var fixture = new Fixture(type, input)) {
        for (String codec : codecs) {
          byte[] expected = verify(fixture, codec);
          long[][] times = new long[2][trials];
          for (int trial = -warmups; trial < trials; trial++) {
            for (int candidate = 0; candidate < 2; candidate++) {
              int backend = Math.floorMod(trial + candidate, 2);
              long elapsed = write(fixture, codec, backend);
              if (trial >= 0) times[backend][trial] = elapsed;
            }
          }
          for (int backend = 0; backend < 2; backend++) {
            assertArrayEquals(
                expected, fingerprint(fixture, codec, backend), BACKENDS[backend] + " " + codec);
            Arrays.sort(times[backend]);
            System.out.printf(
                Locale.ROOT,
                "ORC_WRITE,%s,%s,%s,%s,%d,%d,%d,%.3f,%.3f,%.3f,%d%n",
                shapes[shape],
                codec,
                STRATEGY,
                BACKENDS[backend],
                rows,
                BATCH_ROWS,
                trials,
                times[backend][trials / 2] / 1e6,
                times[backend][0] / 1e6,
                times[backend][trials - 1] / 1e6,
                LocalFileIO.create().getFileSize(output(backend)));
          }
        }
      }
    }
    assertEquals("", NativeOrc.liveNativeHandles());
  }
}
