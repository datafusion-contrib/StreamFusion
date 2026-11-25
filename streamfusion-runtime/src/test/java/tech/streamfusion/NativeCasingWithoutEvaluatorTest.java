package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.NestedStringColumnVector;
import tech.streamfusion.arrow.StringCacheColumns;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.planner.common.ExpressionKind;

class NativeCasingWithoutEvaluatorTest {
  @ParameterizedTest
  @ValueSource(strings = {"en-US", "tr-TR", "az-AZ", "lt-LT", "th-TH"})
  void mixedRepresentationsMatchFlinkWithoutAnEvaluator(String language) throws Exception {
    Locale previous = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag(language));
    try {
      var allocator = NativeAllocator.SHARED;
      try (var source = new VarCharVector("s", allocator);
          var caches = new StringCacheColumns.Writer(RowType.of(new VarCharType()), allocator)) {
        List<String> values = samples();
        var lower = new ArrayList<BinaryStringData>();
        var upper = new ArrayList<BinaryStringData>();
        source.allocateNew();
        for (int i = 0; i < values.size(); i++) {
          String text = values.get(i);
          boolean cached = i % 2 == 0;
          if (text == null) {
            source.setNull(i);
            caches.write(GenericRowData.of((Object) null), i);
            lower.add(null);
            upper.add(null);
            continue;
          }
          var value = flinkString(text, cached);
          caches.write(GenericRowData.of(value), i);
          source.setSafe(i, value.toBytes());
          lower.add(flinkString(text, cached).toLowerCase());
          upper.add(flinkString(text, cached).toUpperCase());
        }
        source.setValueCount(values.size());
        List<FieldVector> vectors = new ArrayList<>();
        vectors.add(source);
        vectors.addAll(caches.finish(values.size()));
        var root = new VectorSchemaRoot(vectors);
        root.setRowCount(values.size());
        // There are no evaluator descriptors in this expression. Setup supplies only JDK data.
        long calc = Native.createCalcExpression(
            new int[] {ExpressionKind.REPRESENTED_STRING_CASE.code(), ExpressionKind.INPUT_REF.code(),
                ExpressionKind.REPRESENTED_STRING_CASE.code(), ExpressionKind.INPUT_REF.code()},
            new int[] {-1, 0, -2, 0},
            new int[] {1, 0, 1, 0},
            new long[0],
            new double[0], new String[0], new int[] {0, 2}, -1, new String[] {"up", "lo"});
        try (var inArray = ArrowArray.allocateNew(allocator);
            var inSchema = ArrowSchema.allocateNew(allocator);
            var outArray = ArrowArray.allocateNew(allocator);
            var outSchema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, root, null, inArray, inSchema);
          Native.calcExpression(calc, inArray.memoryAddress(), inSchema.memoryAddress(),
              outArray.memoryAddress(), outSchema.memoryAddress());
          try (var output = Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
            var nativeUpper = new NestedStringColumnVector((StructVector) output.getVector(0));
            var nativeLower = new NestedStringColumnVector((StructVector) output.getVector(1));
            for (int i = 0; i < values.size(); i++) {
              String detail = language + " row " + i;
              assertResult(upper.get(i), nativeUpper, i, detail);
              assertResult(lower.get(i), nativeLower, i, detail);
            }
          }
        } finally {
          Native.closeCalcExpression(calc);
        }
      }
    } finally {
      Locale.setDefault(previous);
    }
  }

  private static void assertResult(BinaryStringData stock, NestedStringColumnVector nativeResult,
      int row, String detail) {
    assertEquals(stock == null, nativeResult.isNullAt(row), detail);
    if (stock == null) return;
    var value = (BinaryStringData) nativeResult.getString(row);
    byte[] bytes = stock.toBytes();
    assertArrayEquals(bytes, value.toBytes(), detail);
    if (bytes.length == 0) return;
    assertEquals(stock.getJavaObject(), value.getJavaObject(), detail);
  }

  private static BinaryStringData flinkString(String text, boolean cached) {
    return cached ? BinaryStringData.fromString(text)
        : BinaryStringData.fromBytes(text.getBytes(StandardCharsets.UTF_8));
  }

  private static List<String> samples() {
    var rows = new ArrayList<String>();
    rows.add(null);
    for (String text : List.of("", "iI", "Straße", "İ", "\u1d62\u0307",
        "\u1d62\u0307a", "a\u1d62\u0307", "I\u0327\u0307", "J\u0301",
        "I\u0301\u0307", "AΣ𐐀ʰ", "AΣ\uffffΣ", "ΣΑ ΑΣ Σ", "\ud800AΣ",
        "\u1f80\u0345", "\u2071\u0307🙂", "𐐀Σ", "AΣภาษาไทยΣA")) {
      rows.add(text);
      rows.add(text);
    }
    var alphabet = ("IiJİĄjΣσc-' .\u0327\u0301\u0307\u1d62\u2071\u2161"
        + "\uffff中🙂𐐀").codePoints().toArray();
    var random = new Random(3937);
    for (int row = 0; row < 1024; row++) {
      var text = new StringBuilder();
      for (int count = 1 + random.nextInt(15); count > 0; count--) {
        text.appendCodePoint(alphabet[random.nextInt(alphabet.length)]);
      }
      rows.add(text.toString());
    }
    return rows;
  }
}
