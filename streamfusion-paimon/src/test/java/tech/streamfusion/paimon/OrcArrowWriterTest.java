package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcReaderFactory;
import org.apache.paimon.format.orc.OrcTypeUtil;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.shade.org.apache.orc.Reader;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.orc.OrcCodec;

class OrcArrowWriterTest {
  @TempDir java.nio.file.Path directory;

  @Test
  void paimonReaderCodecLoadsWithoutFlinksOrcClasses() throws Exception {
    var location = OrcCodec.class.getProtectionDomain().getCodeSource().getLocation();
    try (var loader =
        new java.net.URLClassLoader(new java.net.URL[] {location}, getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.orc.")
                || name.startsWith("org.apache.flink.orc.")
                || name.startsWith("org.apache.hadoop.hive."))
              throw new ClassNotFoundException(name);
            if (name.startsWith("tech.streamfusion.orc.")) {
              Class<?> loaded = findLoadedClass(name);
              if (loaded == null) loaded = findClass(name);
              if (resolve) resolveClass(loaded);
              return loaded;
            }
            return super.loadClass(name, resolve);
          }
        }) {
      Object codec =
          loader
              .loadClass(OrcCodec.class.getName())
              .getConstructor(String.class, boolean.class)
              .newInstance("struct<id:int>", false);
      assertInstanceOf(tech.streamfusion.format.ColumnarFileCodec.class, codec);
    }
  }

  @Test
  void slicedAndSelectedNestedBuffersMatchStockVectors() throws Exception {
    var type = new RowType(PaimonTestTables.paimonSchema(Map.of()).fields());
    var description = OrcTypeUtil.convertToOrcSchema((RowType) OrcFileFormat.refineDataType(type));
    var values = PaimonTestTables.values(24);
    var path = new Path(directory.resolve("sliced.orc").toUri());
    var io = LocalFileIO.create();
    var codec = new OrcCodec(description.toString(), false, new PaimonOrcWriter());
    try (var allocator = NativeAllocator.SHARED.newChildAllocator("orc-slices", 0, Long.MAX_VALUE);
        var root =
            RowDataArrowConverter.write(
                values.stream().map(PaimonTestTables::flinkRow).collect(ListCollectors.toList()),
                PaimonTestTables.FLINK_TYPE,
                allocator);
        var output = io.newOutputStream(path, false);
        var writer =
            codec.createEncoder(
                PaimonArrowFields.annotate(root.getSchema(), type),
                new int[0],
                new String[] {"compression"},
                new String[] {"ZSTD"},
                false,
                output)) {
      writer.write(root, new int[0], 5, 7);
      try (var array = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array);
        writer.write(array.memoryAddress(), new int[] {19, 1, 19}, 0, 3);
        assertEquals(0, array.snapshot().release);
      }
      writer.write(root, new int[0], root.getRowCount(), 0);
      writer.finish();
      writer.finish();
      assertThrows(IllegalStateException.class, () -> writer.write(root, new int[0], 0, 1));
      // Finishing owns the ORC footer, while Paimon still owns its stream.
      output.flush();
    }
    var expected = description.createRowBatch(32);
    var vectorizer = new RowDataVectorizer(description, type.getFields(), true);
    for (int row : new int[] {5, 6, 7, 8, 9, 10, 11, 19, 1, 19})
      vectorizer.vectorize(PaimonTestTables.paimonRow(values.get(row)), expected);
    List<String> actual = new ArrayList<>();
    try (var reader = OrcReaderFactory.createReader(new Configuration(false), io, path, null);
        var rows = reader.rows(new Reader.Options())) {
      assertEquals(10, reader.getNumberOfRows());
      for (int id = 1; id <= description.getMaximumId(); id++)
        assertEquals(
            description.findSubtype(id).getAttributeValue("paimon.id"),
            reader.getSchema().findSubtype(id).getAttributeValue("paimon.id"));
      var batch = description.createRowBatch(3);
      while (rows.nextBatch(batch)) actual.addAll(render(batch));
    }
    assertEquals(render(expected), actual);
  }

  @Test
  void abandonedWriterDoesNotFinalizeTheHostStream() throws Exception {
    var type = new RowType(PaimonTestTables.paimonSchema(Map.of()).fields());
    var description = OrcTypeUtil.convertToOrcSchema((RowType) OrcFileFormat.refineDataType(type));
    var output = new java.io.ByteArrayOutputStream();
    var values = PaimonTestTables.values(12);
    var codec = new OrcCodec(description.toString(), false, new PaimonOrcWriter());
    try (var root =
        RowDataArrowConverter.write(
            values.stream().map(PaimonTestTables::flinkRow).collect(ListCollectors.toList()),
            PaimonTestTables.FLINK_TYPE,
            NativeAllocator.SHARED)) {
      var writer =
          codec.createEncoder(
              PaimonArrowFields.annotate(root.getSchema(), type),
              new int[0],
              new String[0],
              new String[0],
              false,
              output);
      writer.write(root, new int[0], 0, -1);
      byte[] before = output.toByteArray();
      writer.close();
      writer.close();
      assertArrayEquals(before, output.toByteArray());
      assertThrows(IllegalStateException.class, writer::finish);
    }
  }

  private static List<String> render(
      org.apache.paimon.shade.org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch batch) {
    List<String> rows = new ArrayList<>();
    for (int r = 0; r < batch.size; r++) {
      var value = new StringBuilder();
      for (var column : batch.cols) {
        column.stringifyValue(value, r);
        value.append('|');
      }
      rows.add(value.toString());
    }
    return rows;
  }
}
