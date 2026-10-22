package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;

class CollectionExitOwnershipTest {
  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void ownsArraysMapsAndFreshRowsAfterIpcAndBatchClosure(boolean reuse, boolean boxed) throws Exception {
    var inner = RowType.of(new IntType(), new BigIntType());
    var schema = RowType.of(new ArrayType(new BigIntType()),
        new MapType(new BigIntType(), new BigIntType()), inner);
    var fields = new ArrayList<>(ArrowConversion.toArrowSchema(schema).getFields());
    // Native ROW constructors have ordinary Struct children and an implicit INSERT kind.
    fields.set(2, new Field(fields.get(2).getName(), FieldType.nullable(ArrowType.Struct.INSTANCE),
        ArrowConversion.toArrowSchema(inner).getFields()));
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, RowData>(
        new ArrowToRowDataOperator(schema), new ArrowBatchSerializer())) {
      if (reuse) harness.getExecutionConfig().enableObjectReuse();
      harness.setup(new RowDataSerializer(schema));
      harness.open();
      var serializer = new ArrowBatchSerializer();
      for (int batch = 0; batch < 3; batch++) {
        var root = VectorSchemaRoot.create(new Schema(fields), NativeAllocator.SHARED);
        var writer = ArrowConversion.createRowDataArrowWriter(root, schema, 1);
        for (int row = 0; row < 69; row++) {
          int id = batch * 69 + row;
          var values = new Long[row % 3 == 0 ? 0 : 67];
          var primitive = new long[values.length];
          for (int i = 0; i < values.length; i++) values[i] = i % 5 == 0 ? null : id * 100L + i;
          for (int i = 0; i < primitive.length; i++) primitive[i] = id * 100L + i;
          writer.write(GenericRowData.of(
              row % 7 == 0 ? null : boxed ? new GenericArrayData(values) : new GenericArrayData(primitive),
              row % 11 == 0 ? null : map(id),
              row % 13 == 0 ? null : GenericRowData.of(id, -(long) id)));
          java.util.Arrays.fill(values, -1L);
          java.util.Arrays.fill(primitive, -1L);
        }
        writer.finish();
        var kinds = new org.apache.arrow.vector.TinyIntVector(
            RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
        kinds.allocateNew(69);
        for (int row = 0; row < 69; row++) kinds.set(row, (byte) (row % 4));
        kinds.setValueCount(69);
        var vectors = new ArrayList<>(root.getFieldVectors());
        vectors.add(kinds);
        var tagged = new VectorSchemaRoot(vectors);
        tagged.setRowCount(69);
        var output = new org.apache.flink.core.memory.DataOutputSerializer(1024);
        serializer.serialize(new ArrowBatch(tagged), output);
        var restored = serializer.deserialize(
            new org.apache.flink.core.memory.DataInputDeserializer(output.getCopyOfBuffer()));
        harness.processElement(new StreamRecord<>(restored));
      }
      List<RowData> output = new ArrayList<>();
      for (Object record : harness.getOutput())
        if (record instanceof StreamRecord) output.add((RowData) ((StreamRecord<?>) record).getValue());
      assertEquals(207, output.size());
      for (int id = 0; id < output.size(); id++) {
        int row = id % 69;
        var actual = output.get(id);
        assertEquals(RowKind.fromByteValue((byte) (row % 4)), actual.getRowKind());
        assertEquals(row % 7 == 0, actual.isNullAt(0));
        if (!actual.isNullAt(0)) {
          var array = actual.getArray(0);
          assertEquals(row % 3 == 0 ? 0 : 67, array.size());
          for (int i = 0; i < array.size(); i++) {
            assertEquals(boxed && i % 5 == 0, array.isNullAt(i));
            if (!array.isNullAt(i)) assertEquals(id * 100L + i, array.getLong(i));
          }
        }
        assertEquals(row % 11 == 0, actual.isNullAt(1));
        if (!actual.isNullAt(1)) {
          var map = actual.getMap(1);
          assertEquals(3, map.size());
          assertTrue(map.keyArray().isNullAt(0));
          assertEquals(7L, map.keyArray().getLong(1));
          assertEquals(7L, map.keyArray().getLong(2));
          assertEquals((long) id, map.valueArray().getLong(0));
          assertTrue(map.valueArray().isNullAt(1));
          assertEquals(-(long) id, map.valueArray().getLong(2));
        }
        assertEquals(row % 13 == 0, actual.isNullAt(2));
        if (!actual.isNullAt(2)) {
          var nested = actual.getRow(2, 2);
          assertEquals(RowKind.INSERT, nested.getRowKind());
          assertEquals(id, nested.getInt(0));
          assertEquals(-(long) id, nested.getLong(1));
        }
      }
    }
  }

  private static MapData map(int id) {
    return new MapData() {
      public int size() { return 3; }
      public org.apache.flink.table.data.ArrayData keyArray() {
        return new GenericArrayData(new Long[] {null, 7L, 7L});
      }
      public org.apache.flink.table.data.ArrayData valueArray() {
        return new GenericArrayData(new Long[] {(long) id, null, -(long) id});
      }
    };
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void keepsFloatingArrayPayloadBitsAndRepresentedRowKinds(boolean reuse) throws Exception {
    var nested = RowType.of(new IntType());
    var schema = RowType.of(
        new ArrayType(new org.apache.flink.table.types.logical.DoubleType()), nested);
    double nan = Double.longBitsToDouble(0x7ff8000000000001L);
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, RowData>(
        new ArrowToRowDataOperator(schema), new ArrowBatchSerializer())) {
      if (reuse) harness.getExecutionConfig().enableObjectReuse();
      harness.setup(new RowDataSerializer(schema));
      harness.open();
      for (int i = 0; i < 4; i++) {
        var row = GenericRowData.of(i);
        row.setRowKind(RowKind.values()[i]);
        var input = GenericRowData.of(new GenericArrayData(new double[] {nan, -0.0d}), row);
        var root = RowDataArrowConverter.write(List.of(input), schema, NativeAllocator.SHARED,
            true, true);
        harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      }
      int i = 0;
      for (Object record : harness.getOutput()) {
        if (!(record instanceof StreamRecord)) continue;
        var row = (RowData) ((StreamRecord<?>) record).getValue();
        assertEquals(Double.doubleToRawLongBits(nan), Double.doubleToRawLongBits(row.getArray(0).getDouble(0)));
        assertEquals(Double.doubleToRawLongBits(-0.0d), Double.doubleToRawLongBits(row.getArray(0).getDouble(1)));
        assertEquals(RowKind.values()[i], row.getRow(1, 1).getRowKind());
        assertEquals(i++, row.getRow(1, 1).getInt(0));
      }
      assertEquals(4, i);
    }
  }
}
