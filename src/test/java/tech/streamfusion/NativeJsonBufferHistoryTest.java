package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Compares actual JNI evaluation with Flink under controlled Jackson recycler histories. */
class NativeJsonBufferHistoryTest {
  private static final JsonFactory FACTORY = new JsonFactory();

  @org.junit.jupiter.api.Test
  void numericSemanticsMatchEachHostsProviderAcrossBatches() {
    var documents = new ArrayList<String>();
    for (String number :
        List.of(
            "-0.0",
            "1.2300",
            "1e23",
            "4.9e-324",
            "1e309",
            "1e2147483648",
            "1e-2147483648",
            "0e9999999999",
            "1".repeat(1001))) {
      documents.add("{\"a\":" + number + "}");
    }
    var random = new java.util.Random(1844);
    documents.add("{\"a\":1,\"nested\":" + "[".repeat(4096) + "0" + "]".repeat(4096) + "}");
    for (int i = 0; i < 2053; i++) {
      double value = Double.longBitsToDouble(random.nextLong());
      if (Double.isFinite(value)) documents.add("{\"a\":" + Double.toString(value) + "}");
    }
    for (int batchSize : new int[] {1, 513, documents.size()}) {
      assertEquals(
          withBuffer(4000, () -> hostRows(documents, false)),
          withBuffer(4000, () -> nativeRows(documents, false, batchSize)));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {144, 145, 146, 147})
  void documentPredicatesUseTheActualRecycledBuffer(int op) {
    List<String> documents =
        Arrays.asList(
            null,
            "null",
            "[]",
            "true",
            boundary(3990, "1." + "2".repeat(1000)),
            " ".repeat(8000) + "invalid",
            boundary(7990, "1." + "2".repeat(1000)));
    for (int size : new int[] {4000, 8000, 16000, 32768}) {
      List<String> expected =
          withBuffer(
              size,
              () ->
                  documents.stream()
                      .map(
                          document ->
                              Boolean.toString(
                                  switch (op) {
                                    case 144 -> SqlJsonUtils.isJsonValue(document);
                                    case 145 -> SqlJsonUtils.isJsonObject(document);
                                    case 146 -> SqlJsonUtils.isJsonArray(document);
                                    case 147 -> SqlJsonUtils.isJsonScalar(document);
                                    default -> throw new AssertionError(op);
                                  }))
                      .toList());
      for (int batchSize : new int[] {1, documents.size()}) {
        assertEquals(
            expected,
            withBuffer(
                size, () -> nativeRows(documents, op, new String[] {unicodeVersion()}, batchSize)),
            "op " + op + ", buffer " + size + ", batch " + batchSize);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void numericBoundaryUsesTheActualRecycledBuffer(boolean exists) {
    List<String> documents = new ArrayList<>();
    for (int start : new int[] {2999, 3000, 3990, 7990, 15990, 32760}) {
      for (int digits : new int[] {999, 1000, 1001}) {
        documents.add(boundary(start, "1." + "2".repeat(digits)));
        documents.add(boundary(start, "-1e" + "0".repeat(digits)));
      }
      documents.add(boundary(start, "1." + "2".repeat(1000)).replace("x", "\u4e2d"));
      documents.add(boundary(start, "1." + "2".repeat(1000)).replace("x", "\ud83d\ude00"));
    }
    List<String> fresh = withBuffer(4000, () -> hostRows(documents, exists));
    List<String> grown = withBuffer(32768, () -> hostRows(documents, exists));
    if (!tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON)
      assertNotEquals(fresh, grown, "The fixture must exercise Jackson's history dependence");
    else assertEquals(fresh, grown, "Jackson 2.14 has no token-length constraint");
    for (int size : new int[] {4000, 8000, 16000, 32768}) {
      assertEquals(
          withBuffer(size, () -> hostRows(documents, exists)),
          withBuffer(size, () -> nativeRows(documents, exists, documents.size())),
          "initial buffer " + size);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void growthIncludesInvalidUnicodeAndSimdInputsAcrossBatches(boolean exists) {
    String number = "1." + "2".repeat(1000);
    StringBuilder wide = new StringBuilder("{\"a\":\"ok\"");
    for (int i = 0; i < 64; i++) {
      wide.append(",\"f")
          .append(i)
          .append("\":\"")
          .append("x".repeat(i < 16 ? 1 : 150))
          .append('"');
    }
    wide.append('}');
    List<String> documents =
        Arrays.asList(
            boundary(3990, number),
            null,
            "{\"a\":\"" + "\u4e2d\ud83d\ude00".repeat(1700) + "\"}",
            boundary(3990, number),
            wide.toString(),
            boundary(wide.length() - 10, number),
            " ".repeat(8000) + "invalid",
            boundary(7990, number),
            "{\"padding\":\"" + "x".repeat(32700) + "\"}",
            boundary(15990, number),
            " ".repeat(32767) + "0",
            " ".repeat(32768) + "0",
            "\"" + "x".repeat(33000) + "\"",
            boundary(32760, number));
    List<String> expected = withBuffer(4000, () -> hostRows(documents, exists));
    int expectedSize =
        withBuffer(
            4000,
            () -> {
              hostRows(documents, exists);
              return bufferSize();
            });
    for (int batch : new int[] {1, 3, documents.size()}) {
      withBuffer(
          4000,
          () -> {
            assertEquals(expected, nativeRows(documents, exists, batch), "batch size " + batch);
            assertEquals(expectedSize, bufferSize(), "publish growth for subsequent Flink calls");
            assertEquals(
                hostRows(List.of(boundary(15990, number)), exists),
                nativeRows(List.of(boundary(15990, number)), exists, 1));
            return null;
          });
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void errorPoliciesReturnTheGrownBuffer(boolean exists) {
    String document = " ".repeat(8192) + "invalid";
    withBuffer(
        4000,
        () -> {
          assertThrows(NativeException.class, () -> nativeRows(List.of(document), exists, 1, true));
          assertEquals(document.length(), bufferSize());
          return null;
        });
  }

  private static String boundary(int start, String number) {
    String prefix = "{\"a\":\"ok\",\"padding\":\"";
    String before = prefix + "x".repeat(start - prefix.length() - 6) + "\",\"n\":";
    String tail = ",\"tail\":\"";
    return before
        + number
        + tail
        + "x".repeat(37502 - before.length() - number.length() - tail.length() - 2)
        + "\"}";
  }

  private static List<String> hostRows(List<String> documents, boolean exists) {
    List<String> result = new ArrayList<>();
    for (String document : documents) {
      Object value =
          document == null
              ? null
              : exists
                  ? SqlJsonUtils.jsonExists(document, "$.a", JsonExistsOnError.UNKNOWN)
                  : SqlJsonUtils.jsonValue(
                      document,
                      "$.a",
                      JsonValueOnEmptyOrError.DEFAULT,
                      "EMPTY",
                      JsonValueOnEmptyOrError.DEFAULT,
                      "ERROR");
      result.add(value == null ? null : value.toString());
    }
    return result;
  }

  private static List<String> nativeRows(List<String> documents, boolean exists, int batchSize) {
    return nativeRows(documents, exists, batchSize, false);
  }

  private static List<String> nativeRows(
      List<String> documents, boolean exists, int batchSize, boolean throwOnError) {
    String unicode = unicodeVersion();
    String[] literals =
        exists
            ? new String[] {"strict $.a", throwOnError ? "ERROR" : "UNKNOWN", unicode}
            : new String[] {
              "strict $.a", "DEFAULT", "EMPTY", throwOnError ? "ERROR" : "DEFAULT", "ERROR", unicode
            };
    return nativeRows(documents, exists ? 142 : 141, literals, batchSize);
  }

  static String unicodeVersion() {
    return switch (Runtime.version().feature()) {
      case 17 -> "13.0";
      case 21 -> "15.0";
      case 24, 25 -> "16.0";
      default -> throw new IllegalStateException("Unverified JDK");
    };
  }

  static List<String> nativeRows(List<String> documents, int op, String[] literals, int batchSize) {
    int[] kinds = new int[literals.length + 2];
    int[] payload = new int[kinds.length];
    int[] children = new int[kinds.length];
    kinds[0] = 6;
    payload[0] = op;
    children[0] = literals.length + 1;
    for (int i = 0; i < literals.length; i++) {
      kinds[i + 2] = 3;
      payload[i + 2] = i;
    }
    long calc =
        Native.createCalcExpression(
            kinds,
            payload,
            children,
            new long[0],
            new double[0],
            literals,
            new int[] {0},
            -1,
            new String[] {"result"});
    List<String> result = new ArrayList<>();
    try (RootAllocator allocator = new RootAllocator()) {
      for (int offset = 0; offset < documents.size(); offset += batchSize) {
        int rows = Math.min(batchSize, documents.size() - offset);
        VarCharVector values = new VarCharVector("s", allocator);
        try (VectorSchemaRoot input = VectorSchemaRoot.of(values);
            ArrowArray inArray = ArrowArray.allocateNew(allocator);
            ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          values.allocateNew();
          for (int row = 0; row < rows; row++) {
            String document = documents.get(offset + row);
            if (document == null) {
              values.setNull(row);
            } else {
              values.setSafe(row, document.getBytes(StandardCharsets.UTF_8));
            }
          }
          input.setRowCount(rows);
          Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
          Native.calcExpression(
              calc,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress());
          try (VectorSchemaRoot output =
              Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
            for (int row = 0; row < rows; row++) {
              Object value = output.getVector(0).getObject(row);
              result.add(value == null ? null : value.toString());
            }
          }
        }
      }
    } finally {
      Native.closeCalcExpression(calc);
    }
    return result;
  }

  private static int bufferSize() {
    BufferRecycler recycler = FACTORY._getBufferRecycler();
    char[] buffer = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
    recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, buffer);
    tech.streamfusion.compat.JsonRuntimeCompat.releaseToPool(recycler);
    return buffer.length;
  }

  private static <T> T withBuffer(int size, Supplier<T> action) {
    BufferRecycler recycler = FACTORY._getBufferRecycler();
    char[] original = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
    recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, new char[size]);
    try {
      return action.get();
    } finally {
      recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
      recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, original);
      tech.streamfusion.compat.JsonRuntimeCompat.releaseToPool(recycler);
    }
  }
}
