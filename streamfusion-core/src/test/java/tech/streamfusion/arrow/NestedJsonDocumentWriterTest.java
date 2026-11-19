package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class NestedJsonDocumentWriterTest {
  @Test
  void redundantCachesAreOmittedWithoutChangingJsonCharacters() {
    Field original = NestedRepresentation.stringField("s", true);
    Field field = new Field("s", new FieldType(true, original.getType(), null,
        Map.of(NestedRepresentation.JSON_DOCUMENT, "1")), original.getChildren());
    byte[] wrong = "{\"a\":2}".getBytes(StandardCharsets.UTF_8);
    var inconsistent = new BinaryStringData(
        new MemorySegment[] {MemorySegmentFactory.wrap(wrong)}, 0, wrong.length, "{\"a\":1}");
    var lazy = BinaryStringData.fromString("{\"a\":1}");
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, RowType.of(new VarCharType()));
      for (var value : java.util.Arrays.asList(lazy,
          BinaryStringData.fromString("\u4e2d"), BinaryStringData.fromString("\ud800"),
          inconsistent, BinaryStringData.fromBytes(wrong), null,
          BinaryStringData.fromString("\ud83d\ude00"),
          BinaryStringData.fromString("\ud83d\ude00\ud800"), BinaryStringData.fromString("\udc00")))
        writer.write(GenericRowData.of(value));
      writer.finish();
      assertNull(lazy.getBinarySection(), "JSON input must retain the lazy UTF-8 encoding path");
      var parent = (StructVector) root.getVector(0);
      var cache = (ListVector) parent.getChild("java");
      assertTrue(cache.isNull(0));
      assertTrue(cache.isNull(1));
      assertEquals("\ud800", Utf16Cache.read(cache, 2));
      assertEquals("{\"a\":1}", Utf16Cache.read(cache, 3));
      assertTrue(cache.isNull(4));
      assertTrue(parent.isNull(5));
      assertTrue(cache.isNull(6));
      assertEquals("\ud83d\ude00\ud800", Utf16Cache.read(cache, 7));
      assertEquals("\udc00", Utf16Cache.read(cache, 8));
    }
  }
}
