package tech.streamfusion.planner;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.arrow.ArrowConversion;

class NestedRepresentationTest {
  @Test
  void binaryAndGenericCopiesKeepTheirGetterCacheLifetimeAndActualNulls() {
    var element = STRING().getLogicalType();
    var nested = ROW(FIELD("s", STRING())).getLogicalType();
    var type =
        (RowType)
            ROW(
                    FIELD("a", ARRAY(STRING())),
                    FIELD("r", ROW(FIELD("s", STRING()))),
                    FIELD("n", ARRAY(INT().notNull())))
                .getLogicalType();
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var scope = NativeConfig.usePlannerConfig(config);
        var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowInputSchema(type, true), allocator)) {
      var arrays = new org.apache.flink.table.runtime.typeutils.ArrayDataSerializer(element);
      var rows =
          new org.apache.flink.table.runtime.typeutils.RowDataSerializer(
              nested
                  .getChildren()
                  .toArray(org.apache.flink.table.types.logical.LogicalType[]::new));
      var array =
          new GenericArrayData(
              new Object[] {
                null,
                BinaryStringData.fromBytes("😀".getBytes(java.nio.charset.StandardCharsets.UTF_8))
              });
      var row =
          GenericRowData.of(
              BinaryStringData.fromBytes("😀".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      row.setRowKind(RowKind.DELETE);
      var numbers = new GenericArrayData(new Integer[] {null, -1});
      var numberSerializer =
          new org.apache.flink.table.runtime.typeutils.ArrayDataSerializer(
              INT().notNull().getLogicalType());
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      writer.write(GenericRowData.of(array, row, numbers));
      writer.write(
          GenericRowData.of(
              arrays.toBinaryArray(array).copy(),
              rows.toBinaryRow(row).copy(),
              numberSerializer.toBinaryArray(numbers).copy()));
      writer.finish();
      var reader = ArrowConversion.createArrowReader(root, type);
      for (int index = 0; index < 2; index++) {
        var actual = reader.read(index);
        var value = actual.getArray(0);
        assertEquals(
            index == 1, value instanceof org.apache.flink.table.data.binary.BinaryArrayData);
        value.getString(1).toString();
        assertEquals(index == 1, ((BinaryStringData) value.getString(1)).getJavaObject() == null);
        var record = actual.getRow(1, 1);
        assertEquals(
            index == 1, record instanceof org.apache.flink.table.data.binary.BinaryRowData);
        assertEquals(RowKind.DELETE, record.getRowKind());
        assertTrue(actual.getArray(2).isNullAt(0));
        assertEquals(
            index == 1,
            actual.getArray(2) instanceof org.apache.flink.table.data.binary.BinaryArrayData);
      }
    }
  }

  @Test
  void cachedAndColdStringsKeepTheirDistinctComparisonAndRenderingSemantics() {
    var type = (RowType) ROW(FIELD("a", ARRAY(STRING())), FIELD("s", STRING())).getLogicalType();
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var scope = NativeConfig.usePlannerConfig(config);
        var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowInputSchema(type, true), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      var cached = BinaryStringData.fromString("\ud800");
      var cold = BinaryStringData.fromBytes(new byte[] {'?'});
      writer.write(
          GenericRowData.of(new GenericArrayData(new Object[] {cached, cold, null}), cached));
      writer.finish();
      var row = ArrowConversion.createArrowReader(root, type).read(0);
      var array = row.getArray(0);
      assertEquals("\ud800", array.getString(0).toString());
      assertNull(((BinaryStringData) array.getString(1)).getJavaObject());
      assertEquals("?", array.getString(1).toString());
      assertTrue(array.isNullAt(2));
      assertEquals("\ud800", row.getString(1).toString());
    }
  }

  @Test
  void rowKindsSurviveNestedArraysAndMapsWithoutOccupyingLogicalFields() {
    var nested = ROW(FIELD("__sf_nested_row_kind", INT()));
    var type =
        (RowType) ROW(FIELD("a", ARRAY(nested)), FIELD("m", MAP(INT(), nested))).getLogicalType();
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var scope = NativeConfig.usePlannerConfig(config);
        var allocator = new RootAllocator();
        var root = VectorSchemaRoot.create(ArrowConversion.toArrowInputSchema(type, true), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (var kind : RowKind.values()) {
        var nestedRow = GenericRowData.of(42);
        nestedRow.setRowKind(kind);
        writer.write(
            GenericRowData.of(
                new GenericArrayData(new Object[] {null, nestedRow}),
                new GenericMapData(Map.of(1, nestedRow))));
      }
      writer.finish();
      var reader = ArrowConversion.createArrowReader(root, type);
      for (int i = 0; i < RowKind.values().length; i++) {
        var row = reader.read(i);
        var arrayRow = row.getArray(0).getRow(1, 1);
        var mapRow = row.getMap(1).valueArray().getRow(0, 1);
        assertEquals(RowKind.values()[i], arrayRow.getRowKind());
        assertEquals(RowKind.values()[i], mapRow.getRowKind());
        assertEquals(1, arrayRow.getArity());
        assertEquals(42, arrayRow.getInt(0));
      }
    }
  }
}
