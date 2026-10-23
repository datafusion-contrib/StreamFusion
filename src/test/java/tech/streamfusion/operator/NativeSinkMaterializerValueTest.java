package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.FlinkKeyGroupUtils;

class NativeSinkMaterializerValueTest {
  static java.util.stream.Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    cases.add(Arguments.of("zero", new DoubleType(), 0.0, 2.0, -0.0));
    cases.add(
        Arguments.of(
            "nan",
            new DoubleType(),
            Double.longBitsToDouble(0x7ff8000000000001L),
            2.0,
            Double.longBitsToDouble(0x7ff8000000000002L)));
    cases.add(Arguments.of("null", new BigIntType(), null, 2L, null));
    cases.add(
        Arguments.of(
            "decimal", new DecimalType(18, 2), decimal("1.20"), decimal("9.99"), decimal("1.20")));
    cases.add(
        Arguments.of(
            "timestamp",
            new TimestampType(9),
            TimestampData.fromEpochMillis(-1, 999999),
            TimestampData.fromEpochMillis(1, 1),
            TimestampData.fromEpochMillis(-1, 999999)));
    cases.add(
        Arguments.of(
            "array-null",
            new ArrayType(new IntType()),
            new GenericArrayData(new Object[] {1, null}),
            new GenericArrayData(new Object[] {2}),
            new GenericArrayData(new Object[] {1, null})));
    cases.add(
        Arguments.of(
            "array-float",
            new ArrayType(new DoubleType()),
            new GenericArrayData(new Object[] {0.0, Double.NaN}),
            new GenericArrayData(new Object[] {2.0}),
            new GenericArrayData(new Object[] {-0.0, Double.NaN})));
    cases.add(
        Arguments.of(
            "array-nan-generic",
            new ArrayType(new DoubleType()),
            new GenericArrayData(new Object[] {Double.longBitsToDouble(0x7ff8000000000001L)}),
            new GenericArrayData(new Object[] {2.0}),
            new GenericArrayData(new Object[] {Double.longBitsToDouble(0x7ff8000000000002L)})));
    cases.add(
        Arguments.of(
            "array-float-nan-generic",
            new ArrayType(new FloatType()),
            new GenericArrayData(new Object[] {Float.intBitsToFloat(0x7fc00001)}),
            new GenericArrayData(new Object[] {2.0f}),
            new GenericArrayData(new Object[] {Float.intBitsToFloat(0x7fc00002)})));
    cases.add(
        Arguments.of(
            "map-nan-generic",
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()),
            map("a", Double.longBitsToDouble(0x7ff8000000000001L)),
            map("b", 2.0),
            map("a", Double.longBitsToDouble(0x7ff8000000000002L))));
    var map = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
    cases.add(
        Arguments.of("map-order", map, map("a", 1, "b", 2), map("z", 9), map("b", 2, "a", 1)));
    cases.add(Arguments.of("map-null", map, map("a", null), map("z", 9), map("a", null)));
    for (String name : List.of("binary-map", "binary-map-fixed")) {
      var firstMap = new LinkedHashMap<Object, Object>();
      firstMap.put(new byte[] {1, 2}, 1);
      var secondMap = new LinkedHashMap<Object, Object>();
      secondMap.put(new byte[] {3, 4}, 2);
      var replacementMap = new LinkedHashMap<Object, Object>();
      replacementMap.put(new byte[] {1, 2}, 1);
      cases.add(
          Arguments.of(
              name,
              new MapType(new BinaryType(2), new IntType()),
              new GenericMapData(firstMap),
              new GenericMapData(secondMap),
              new GenericMapData(replacementMap)));
    }
    var row = RowType.of(new BigIntType(), new DoubleType());
    cases.add(
        Arguments.of(
            "row-zero",
            row,
            GenericRowData.of(1L, 0.0),
            GenericRowData.of(2L, 9.0),
            GenericRowData.of(1L, -0.0)));
    cases.add(
        Arguments.of(
            "row-kind",
            row,
            GenericRowData.of(1L, 0.0),
            GenericRowData.of(2L, 9.0),
            GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1L, 0.0)));
    var nestedSerializer = new RowDataSerializer(row);
    cases.add(
        Arguments.of(
            "row-binary",
            row,
            nestedSerializer.toBinaryRow(GenericRowData.of(1L, 0.0)).copy(),
            nestedSerializer.toBinaryRow(GenericRowData.of(2L, 9.0)).copy(),
            nestedSerializer.toBinaryRow(GenericRowData.of(1L, -0.0)).copy()));
    return cases.stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void generatedEqualityMatchesAfterTheRequiredFlinkKeyedExchange(
      String name, LogicalType valueType, Object first, Object second, Object replacement)
      throws Exception {
    String binary = System.getProperty("streamfusion.experimental.binaryElt");
    String nested = System.getProperty("streamfusion.experimental.nestedScalarUdfs");
    try {
      if (name.endsWith("-fixed")) {
        System.setProperty("streamfusion.experimental.binaryElt", "false");
        System.setProperty("streamfusion.experimental.nestedScalarUdfs", "false");
      }
      var type =
          RowType.of(
              new LogicalType[] {new BigIntType(), valueType, new BigIntType()},
              new String[] {"id", "value", "tag"});
      for (boolean key : new boolean[] {false, true}) {
        var rows = new ArrayList<RowData>();
        var nativeInput = new ArrayList<RowData>();
        var serializer = new RowDataSerializer(type);
        for (GenericRowData row :
            List.of(
                GenericRowData.ofKind(RowKind.INSERT, 1L, first, 10L),
                GenericRowData.ofKind(RowKind.INSERT, 1L, second, 20L),
                GenericRowData.ofKind(
                    key ? RowKind.UPDATE_AFTER : RowKind.DELETE, 1L, replacement, key ? 11L : 10L),
                GenericRowData.ofKind(RowKind.DELETE, 1L, second, 20L),
                GenericRowData.ofKind(RowKind.DELETE, 1L, replacement, key ? 99L : 10L))) {
          var serialized = serializer.toBinaryRow(row).copy();
          rows.add(serialized);
          nativeInput.add(
              name.equals("row-binary") || name.endsWith("-generic") ? row : serialized);
        }
        assertEquals(
            stock(type, rows, key), nativeRows(type, nativeInput, key), name + " inputKey=" + key);
      }
    } finally {
      restore("streamfusion.experimental.binaryElt", binary);
      restore("streamfusion.experimental.nestedScalarUdfs", nested);
    }
  }

  private static void restore(String key, String previous) {
    if (previous == null) System.clearProperty(key);
    else System.setProperty(key, previous);
  }

  private static List<String> stock(RowType type, List<RowData> rows, boolean key)
      throws Exception {
    var loader = NativeSinkMaterializerValueTest.class.getClassLoader();
    var operator =
        new SinkUpsertMaterializer(
            StateTtlConfig.DISABLED,
            new RowDataSerializer(type),
            new EqualiserCodeGenerator(type, loader).generateRecordEqualiser("ValueOracle"),
            key
                ? new EqualiserCodeGenerator(RowType.of(type.getTypeAt(1)), loader)
                    .generateRecordEqualiser("ValueKeyOracle")
                : null,
            key ? new int[] {1} : null);
    try (var h =
        new KeyedOneInputStreamOperatorTestHarness<Integer, RowData, RowData>(
            operator, row -> 0, Types.INT, 128, 1, 0)) {
      h.setup(new RowDataSerializer(type));
      h.open();
      for (var row : rows)
        h.processElement(new StreamRecord<>(new RowDataSerializer(type).copy(row)));
      var result = new ArrayList<String>();
      for (var record : h.extractOutputStreamRecords()) result.add(tag(record.getValue()));
      return result;
    }
  }

  private static List<String> nativeRows(RowType type, List<RowData> rows, boolean key)
      throws Exception {
    int stateKey = FlinkKeyGroupUtils.stateKeysForSubtasks(128, 1)[0];
    try (var allocator = new RootAllocator();
        var h =
            new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
                new NativeSinkMaterializer(
                    new int[] {0}, new int[] {-1}, key ? new int[] {1} : null, 0, 128),
                b -> stateKey,
                Types.INT,
                128,
                1,
                0)) {
      h.setup(new ArrowBatchSerializer());
      h.open();
      var result = new ArrayList<String>();
      for (var row : rows) {
        h.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(row), type, allocator, true))));
        while (!h.getOutput().isEmpty()) {
          Object event = h.getOutput().poll();
          if (event instanceof StreamRecord) {
            try (VectorSchemaRoot root =
                ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
              for (RowData emitted : RowDataArrowConverter.read(root, type))
                result.add(tag(emitted));
            }
          }
        }
      }
      return result;
    }
  }

  private static String tag(RowData row) {
    return row.getRowKind().shortString() + ":" + row.getLong(0) + ":" + row.getLong(2);
  }

  private static DecimalData decimal(String value) {
    return DecimalData.fromBigDecimal(new java.math.BigDecimal(value), 18, 2);
  }

  private static GenericMapData map(Object... pairs) {
    var values = new LinkedHashMap<Object, Object>();
    for (int i = 0; i < pairs.length; i += 2)
      values.put(StringData.fromString((String) pairs[i]), pairs[i + 1]);
    return new GenericMapData(values);
  }
}
