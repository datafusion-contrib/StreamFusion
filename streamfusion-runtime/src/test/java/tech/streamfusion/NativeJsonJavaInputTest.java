package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.BufferRecycler;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.NestedRepresentation;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeJsonJavaInputTest {
  private static final RowType TYPE = RowType.of(new VarCharType());
  private static final JsonFactory FACTORY = new JsonFactory();

  @Test
  void originalJavaDocumentWinsOverDifferentByteRepresentation() throws Exception {
    byte[] bytes = "{\"a\":2}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    var document = new BinaryStringData(
        new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length, "{\"a\":1}");
    assertEquals(List.of("1"), nativeValues(List.of(document), 1, true, "strict $.a"));
  }

  @Test
  void rawMemberNamesRemainDistinctBeforeOutputEncoding() throws Exception {
    List<String> documents = List.of(
        "{\"?\":\"literal\",\"\ud800\":\"high\"}",
        "{\"?\":\"literal\",\"\udc00\":\"low\"}",
        "{\"?\":\"literal\",\"\\uD800\":\"escaped\"}");
    String path = "strict $['?']";
    assertEquals(List.of("literal", "literal", "literal"), host(documents, path));
    assertEquals(host(documents, path), nativeRows(documents, 2, true, path));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 127})
  void javaDocumentsSurviveInputReleaseAndMatchUnionSerialization(int batchSize) throws Exception {
    List<String> documents = Arrays.asList(
        "{\"a\":\"\\uD800\"}", "{\"a\":\"\\uDC00\"}", "{\"a\":\"?\"}",
        "{\"a\":\"\\uD83D\\uDE00\"}", "{\"a\":\"\ud800\"}",
        "{\"a\":\"\udc00\"}", "{\"a\":\"\ud83d\ude00\"}", "{\"a\":\"\ue000\"}",
        "{\"a\":\"\"}", null, "{\"a\":null}", "{\"a\":\"\\\ud800\"}",
        "{\"a\":true}", "{\"a\":123}", "{}", "invalid");
    assertEquals(units(host(documents)), units(nativeRows(documents, batchSize, true)));
    assertNotEquals(units(List.of("\ud800")), units(List.of("?")));
  }

  @ParameterizedTest
  @ValueSource(ints = {4000, 8000, 32768})
  void rawSurrogatesDoNotChangeJacksonBufferHistory(int bufferSize) throws Exception {
    List<String> documents = new ArrayList<>();
    documents.add("{\"a\":\"\ud800\",\"padding\":\"" + "x".repeat(3000) + "\"}");
    for (int start : new int[] {2994, 2999, 3990, 3997, 7997, 15990, 32760}) {
      String prefix = "{\"a\":\"\ud800\",\"padding\":\"";
      String before = prefix + "x".repeat(start - prefix.length() - 6) + "\",\"n\":";
      documents.add(before + "1." + "2".repeat(1000) + ",\"tail\":\""
          + "x".repeat(38000 - before.length()) + "\"}");
    }
    List<List<Integer>> expected = withBuffer(bufferSize, () -> units(host(documents)));
    for (int batch : new int[] {1, documents.size()}) {
      List<List<Integer>> actual = withBuffer(bufferSize, () -> {
        try { return units(nativeRows(documents, batch, false)); }
        catch (Exception failure) { throw new RuntimeException(failure); }
      });
      assertEquals(expected, actual, "buffer=" + bufferSize + ",batch=" + batch);
    }
  }

  private static List<String> host(List<String> documents) {
    return host(documents, "strict $.a");
  }

  private static List<String> host(List<String> documents, String path) {
    List<String> result = new ArrayList<>();
    for (String document : documents) {
      Object value = document == null ? null : SqlJsonUtils.jsonValue(document, path,
          JsonValueOnEmptyOrError.DEFAULT, "EMPTY", JsonValueOnEmptyOrError.DEFAULT, "ERROR");
      result.add(value == null ? null : value.toString());
    }
    return result.stream().map(value -> value == null ? null
        : new String(value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            java.nio.charset.StandardCharsets.UTF_8)).collect(java.util.stream.Collectors.toList());
  }

  private static List<List<Integer>> units(List<String> values) {
    List<List<Integer>> result = new ArrayList<>();
    for (String value : values) result.add(value == null ? null
        : value.chars().boxed().collect(java.util.stream.Collectors.toList()));
    return result;
  }

  private static List<String> nativeRows(List<String> documents, int batchSize, boolean ipc)
      throws Exception {
    return nativeRows(documents, batchSize, ipc, "strict $.a");
  }

  private static List<String> nativeRows(List<String> documents, int batchSize, boolean ipc,
      String path) throws Exception {
    List<StringData> values = documents.stream().map(StringData::fromString)
        .collect(java.util.stream.Collectors.toList());
    return nativeValues(values, batchSize, ipc, path);
  }

  private static List<String> nativeValues(List<StringData> documents, int batchSize, boolean ipc,
      String path) throws Exception {
    String[] strings = {path, "DEFAULT", "EMPTY", "DEFAULT", "ERROR",
        NativeJsonBufferHistoryTest.unicodeVersion()};
    int[] kinds = {6, 0, 3, 3, 3, 3, 3, 3};
    int[] payload = {202, 0, 0, 1, 2, 3, 4, 5};
    int[] children = {7, 0, 0, 0, 0, 0, 0, 0};
    long calc = Native.createCalcExpression(kinds, payload, children, new long[0],
        new double[0], strings, new int[] {0}, -1, new String[] {"result"});
    List<String> result = new ArrayList<>();
    try (var allocator = new RootAllocator()) {
      for (int offset = 0; offset < documents.size(); offset += batchSize) {
        int rows = Math.min(batchSize, documents.size() - offset);
        VectorSchemaRoot output;
        var original = NestedRepresentation.stringField("s", true);
        var field = new org.apache.arrow.vector.types.pojo.Field("s",
            new org.apache.arrow.vector.types.pojo.FieldType(true, original.getType(), null,
                java.util.Map.of(NestedRepresentation.JSON_DOCUMENT, "1")), original.getChildren());
        var schema = new org.apache.arrow.vector.types.pojo.Schema(
            List.of(field,
                new org.apache.arrow.vector.types.pojo.Field(RowDataArrowConverter.ROW_KIND_COLUMN,
                    org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.Int(8, true)), null)));
        try (var input = VectorSchemaRoot.create(schema, allocator);
            var inArray = ArrowArray.allocateNew(allocator);
            var inSchema = ArrowSchema.allocateNew(allocator);
            var outArray = ArrowArray.allocateNew(allocator);
            var outSchema = ArrowSchema.allocateNew(allocator)) {
          var writer = ArrowConversion.createRowDataArrowWriter(
              new VectorSchemaRoot(List.of(input.getVector(0))), TYPE);
          for (int row = 0; row < rows; row++)
            writer.write(GenericRowData.of(documents.get(offset + row)));
          writer.finish();
          var kindsVector = (TinyIntVector) input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          kindsVector.allocateNew(rows);
          for (int row = 0; row < rows; row++) kindsVector.set(row,
              RowKind.values()[(offset + row) % 4].toByteValue());
          input.setRowCount(rows);
          Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
          Native.calcExpression(calc, inArray.memoryAddress(), inSchema.memoryAddress(),
              outArray.memoryAddress(), outSchema.memoryAddress());
          output = Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
        }
        // The input and its JNI exports are gone before either output consumer runs.
        var batch = new ArrowBatch(output);
        if (ipc) {
          batch.shareAcross(2);
          try (var first = batch.root()) { assertEquals(rows, first.getRowCount()); }
          var serializer = new ArrowBatchSerializer();
          var encoded = new DataOutputSerializer(1024);
          serializer.serialize(batch, encoded);
          batch = serializer.deserialize(new DataInputDeserializer(encoded.getCopyOfBuffer()));
        }
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, TYPE);
          for (int row = 0; row < rows; row++) {
            assertEquals(RowKind.values()[(offset + row) % 4].toByteValue(),
                ((TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN)).get(row));
            var value = reader.read(row);
            if (value.isNullAt(0)) result.add(null);
            else {
              var text = (BinaryStringData) value.getString(0);
              assertNull(text.getJavaObject(), "UNION transport exposes byte-backed strings");
              result.add(text.toString());
            }
          }
        }
      }
    } finally {
      Native.closeCalcExpression(calc);
    }
    return result;
  }

  private static <T> T withBuffer(int size, Supplier<T> action) {
    BufferRecycler recycler = FACTORY._getBufferRecycler();
    char[] original = recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
    recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, new char[size]);
    try { return action.get(); }
    finally {
      recycler.allocCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER);
      recycler.releaseCharBuffer(BufferRecycler.CHAR_TOKEN_BUFFER, original);
      tech.streamfusion.compat.JsonRuntimeCompat.releaseToPool(recycler);
    }
  }
}
