package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class ArrowWriterResetTest {
  private static final MapType MAP_TYPE =
      new MapType(new IntType(false), new ArrayType(new VarCharType()));

  @Test
  void resetsMapKeysAndNestedValueWritersBetweenBatches() {
    RowType type = RowType.of(MAP_TYPE);
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (int batch = 0; batch < 3; batch++) {
        writer.write(GenericRowData.of(map(batch)));
        writer.write(GenericRowData.of((Object) null));
        writer.write(GenericRowData.of(new GenericMapData(Map.of())));
        writer.finish();
        var reader = ArrowConversion.createArrowReader(root, type);
        assertEquals(3, root.getRowCount());
        assertMap(reader.read(0).getMap(0), batch);
        assertTrue(reader.read(1).isNullAt(0));
        assertEquals(0, reader.read(2).getMap(0).size());
        writer.reset();
        assertEquals(0, root.getRowCount());
      }
    }
  }

  @Test
  void resetsMapsNestedInsideArrays() {
    RowType type = RowType.of(new ArrayType(MAP_TYPE));
    try (var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (int batch = 0; batch < 3; batch++) {
        writer.write(GenericRowData.of(new GenericArrayData(new Object[] {map(batch), null})));
        writer.finish();
        var array = ArrowConversion.createArrowReader(root, type).read(0).getArray(0);
        assertEquals(2, array.size());
        assertMap(array.getMap(0), batch);
        assertTrue(array.isNullAt(1));
        writer.reset();
      }
    }
  }

  private static GenericMapData map(int batch) {
    Map<Integer, Object> entries = new LinkedHashMap<>();
    entries.put(batch + 10, new GenericArrayData(new Object[] {
        StringData.fromString("batch-" + batch), null, StringData.fromString("value")
    }));
    entries.put(batch + 20, new GenericArrayData(new Object[0]));
    return new GenericMapData(entries);
  }

  private static void assertMap(MapData map, int batch) {
    assertEquals(2, map.size());
    assertEquals(batch + 10, map.keyArray().getInt(0));
    assertEquals(batch + 20, map.keyArray().getInt(1));
    var values = map.valueArray().getArray(0);
    assertEquals(3, values.size());
    assertEquals("batch-" + batch, values.getString(0).toString());
    assertTrue(values.isNullAt(1));
    assertEquals("value", values.getString(2).toString());
    assertEquals(0, map.valueArray().getArray(1).size());
  }
}
