package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.NativeJsonBufferHistoryTest.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NativeJsonSharedBufferHistoryTest {
  @ParameterizedTest
  @CsvSource({"141,141", "141,142", "142,141", "144,141", "145,142", "146,147",
      "254,254", "141,254"})
  void readersPreserveRowMajorValuesAndFinalCapacity(int leftOp, int rightOp) {
    var left = Arrays.asList("{}", "\"" + "x".repeat(16000) + "\"", null,
        boundary(7990, "1." + "2".repeat(1000)), "null", "invalid", "{\"a\":[0,1]}");
    var right = Arrays.asList(boundary(3990, "1." + "2".repeat(1000)), "{}",
        boundary(15990, "1." + "2".repeat(1000)), null, "{\"a\":true}", "{\"a\":{}}", "null");
    for (boolean sameInput : new boolean[] {false, true}) {
      var second = sameInput ? left : right;
      for (int capacity : new int[] {4000, 8000, 16000, 32768}) {
        var expected = withBuffer(capacity, () -> {
          var rows = new ArrayList<List<String>>();
          for (int row = 0; row < left.size(); row++)
            rows.add(Arrays.asList(host(leftOp, left.get(row)), host(rightOp, second.get(row))));
          return new Result(rows, bufferSize());
        });
        for (int batch : new int[] {1, 3, left.size()}) {
          var actual = withBuffer(capacity,
              () -> nativeRows(left, second, leftOp, rightOp, batch));
          assertEquals(expected.values, actual.values,
              "capacity=" + capacity + ", batch=" + batch + ", same=" + sameInput);
          assertEquals(expected.capacity, actual.capacity);
        }
      }
    }
  }

  @Test
  void literalReadersAdvanceTheSharedHistoryForEverySurvivingRow() {
    var varying = Arrays.asList("{}", boundary(3990, "1." + "2".repeat(1000)),
        null, "{\"a\":\"short\"}");
    var literal = java.util.Collections.nCopies(varying.size(),
        "{\"a\":\"" + "x".repeat(16000) + "\"}");
    for (int literalColumn : new int[] {0, 1}) {
      List<String> left = literalColumn == 0 ? literal : varying;
      List<String> right = literalColumn == 1 ? literal : varying;
      var expected = withBuffer(4000, () -> {
        var values = new ArrayList<List<String>>();
        for (int row = 0; row < left.size(); row++)
          values.add(Arrays.asList(host(141, left.get(row)), host(141, right.get(row))));
        return new Result(values, bufferSize());
      });
      var actual = withBuffer(4000,
          () -> nativeRows(left, right, 141, 141, varying.size(), literalColumn));
      assertEquals(expected.values, actual.values, "literal column=" + literalColumn);
      assertEquals(expected.capacity, actual.capacity);
    }
  }

  private static String host(int op, String document) {
    if (document == null && (op == 141 || op == 142)) return null;
    switch (op) {
      case 141:
        Object value = SqlJsonUtils.jsonValue(document, "strict $.a",
            JsonValueOnEmptyOrError.DEFAULT, "EMPTY", JsonValueOnEmptyOrError.DEFAULT, "ERROR");
        return value == null ? null : value.toString();
      case 142:
        Boolean exists = SqlJsonUtils.jsonExists(document, "strict $.a", JsonExistsOnError.UNKNOWN);
        return exists == null ? null : exists.toString();
      case 144: return Boolean.toString(SqlJsonUtils.isJsonValue(document));
      case 145: return Boolean.toString(SqlJsonUtils.isJsonObject(document));
      case 146: return Boolean.toString(SqlJsonUtils.isJsonArray(document));
      case 147: return Boolean.toString(SqlJsonUtils.isJsonScalar(document));
      case 254:
        return document == null ? null : NativeJsonQueryBufferHistoryTest.query(
            document, "strict", JsonQueryWrapper.CONDITIONAL_ARRAY);
      default: throw new AssertionError(op);
    }
  }

  private static String[] literals(int op) {
    switch (op) {
      case 141: return new String[] {"strict $.a", "DEFAULT", "EMPTY", "DEFAULT", "ERROR", unicodeVersion()};
      case 142: return new String[] {"strict $.a", "UNKNOWN", unicodeVersion()};
      case 254: return new String[] {"strict $.a", "CONDITIONAL_ARRAY", "NULL", "EMPTY_OBJECT", unicodeVersion()};
      default: return new String[] {unicodeVersion()};
    }
  }

  private static Result nativeRows(List<String> left, List<String> right,
      int leftOp, int rightOp, int batchSize) {
    return nativeRows(left, right, leftOp, rightOp, batchSize, -1);
  }

  private static Result nativeRows(List<String> left, List<String> right,
      int leftOp, int rightOp, int batchSize, int literalColumn) {
    var literalPool = new ArrayList<String>();
    var kinds = new ArrayList<Integer>();
    var payload = new ArrayList<Integer>();
    var children = new ArrayList<Integer>();
    int[] roots = new int[2];
    int[] ops = {leftOp, rightOp};
    for (int column = 0; column < 2; column++) {
      roots[column] = kinds.size();
      String[] literals = literals(ops[column]);
      kinds.add(6); payload.add(ops[column]); children.add(1 + literals.length);
      if (column == literalColumn) {
        kinds.add(3); payload.add(literalPool.size()); children.add(0);
        literalPool.add((column == 0 ? left : right).get(0));
      } else {
        kinds.add(0); payload.add(column); children.add(0);
      }
      for (String literal : literals) {
        kinds.add(3); payload.add(literalPool.size()); children.add(0); literalPool.add(literal);
      }
    }
    long calc = Native.createCalcExpression(kinds.stream().mapToInt(i -> i).toArray(),
        payload.stream().mapToInt(i -> i).toArray(), children.stream().mapToInt(i -> i).toArray(),
        new long[0], new double[0], literalPool.toArray(new String[0]), roots, -1,
        new String[] {"left", "right"});
    var result = new ArrayList<List<String>>();
    try (var allocator = new RootAllocator()) {
      for (int offset = 0; offset < left.size(); offset += batchSize) {
        int rows = Math.min(batchSize, left.size() - offset);
        try (var a = new VarCharVector("a", allocator);
            var b = new VarCharVector("b", allocator);
            var input = VectorSchemaRoot.of(a, b);
            var inArray = ArrowArray.allocateNew(allocator);
            var inSchema = ArrowSchema.allocateNew(allocator);
            var outArray = ArrowArray.allocateNew(allocator);
            var outSchema = ArrowSchema.allocateNew(allocator)) {
          a.allocateNew(); b.allocateNew();
          for (int row = 0; row < rows; row++) {
            for (int column = 0; column < 2; column++) {
              String text = (column == 0 ? left : right).get(offset + row);
              VarCharVector vector = column == 0 ? a : b;
              if (text == null) vector.setNull(row);
              else vector.setSafe(row, text.getBytes(StandardCharsets.UTF_8));
            }
          }
          input.setRowCount(rows);
          Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
          Native.calcExpression(calc, inArray.memoryAddress(), inSchema.memoryAddress(),
              outArray.memoryAddress(), outSchema.memoryAddress());
          try (var output = Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
            assertEquals(rows, output.getRowCount());
            for (int row = 0; row < rows; row++) {
              var fields = new ArrayList<String>();
              for (int column = 0; column < 2; column++) {
                Object value = output.getVector(column).getObject(row);
                fields.add(value == null ? null : value.toString());
              }
              result.add(fields);
            }
          }
        }
      }
    } finally {
      Native.closeCalcExpression(calc);
    }
    return new Result(result, bufferSize());
  }

  private static final class Result {
    final List<List<String>> values;
    final int capacity;

    Result(List<List<String>> values, int capacity) {
      this.values = values;
      this.capacity = capacity;
    }
  }
}
