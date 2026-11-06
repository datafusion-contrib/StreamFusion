package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.NativeJsonBufferHistoryTest.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NativeJsonQueryBufferHistoryTest {
  private static final Method QUERY = Arrays.stream(SqlJsonUtils.class.getMethods())
      .filter(method -> method.getName().equals("jsonQuery")).findFirst().orElseThrow();

  @ParameterizedTest
  @EnumSource(JsonQueryWrapper.class)
  void selectedContainersRetainTokenLimitsAcrossBatches(JsonQueryWrapper wrapper) {
    var documents = new ArrayList<String>();
    documents.add(null);
    documents.add("{\"a\":{\"n\":1.2300}}");
    for (int start : new int[] {2999, 3990, 7990, 15990, 32760}) {
      documents.add(boundary(start, "1." + "2".repeat(1000)));
      documents.add("\"" + "x".repeat(Math.min(32768, start * 2)) + "\"");
    }
    documents.add("{\"a\":[],\"invalid\":}");
    for (String mode : List.of("strict", "lax")) {
      for (int size : new int[] {4000, 8000, 16000, 32768}) {
        var expected = withBuffer(size, () -> hostRows(documents, mode, wrapper));
        for (int batch : new int[] {1, 3, documents.size()}) {
          assertEquals(expected, withBuffer(size, () -> nativeRows(documents, 254,
              new String[] {mode + " $.a", wrapper.name(), "NULL", "EMPTY_OBJECT", unicodeVersion()},
              batch)), "mode=" + mode + ", buffer=" + size + ", batch=" + batch);
        }
      }
    }
  }

  @Test
  void readerSchedulingCanChangeSuccessfulResults() {
    var left = List.of("{}", "\"" + "x".repeat(16000) + "\"");
    var right = List.of(boundary(3990, "1." + "2".repeat(1000)), "{}");
    var rowMajor = withBuffer(4000, () -> {
      var values = new ArrayList<String>();
      for (int row = 0; row < left.size(); row++) {
        values.add(query(left.get(row), "strict", JsonQueryWrapper.CONDITIONAL_ARRAY));
        values.add(query(right.get(row), "strict", JsonQueryWrapper.CONDITIONAL_ARRAY));
      }
      return values;
    });
    var columnMajor = withBuffer(4000, () -> {
      var first = hostRows(left, "strict", JsonQueryWrapper.CONDITIONAL_ARRAY);
      var second = hostRows(right, "strict", JsonQueryWrapper.CONDITIONAL_ARRAY);
      return Arrays.asList(first.get(0), second.get(0), first.get(1), second.get(1));
    });
    if (tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON) {
      assertEquals(rowMajor, columnMajor);
    } else {
      assertNotEquals(rowMajor, columnMajor);
    }
  }

  private static List<String> hostRows(List<String> documents, String mode, JsonQueryWrapper wrapper) {
    var result = new ArrayList<String>();
    for (String document : documents)
      result.add(document == null ? null : query(document, mode, wrapper));
    return result;
  }

  static String query(String document, String mode, JsonQueryWrapper wrapper) {
    try {
      // The released 2.2 API adds a return-type parameter; 1.18 always returns STRING.
      if (QUERY.getParameterCount() == 5)
        return (String) QUERY.invoke(null, document, mode + " $.a", wrapper,
            JsonQueryOnEmptyOrError.NULL, JsonQueryOnEmptyOrError.EMPTY_OBJECT);
      Object stringType = Arrays.stream(QUERY.getParameterTypes()[2].getEnumConstants())
          .filter(value -> ((Enum<?>) value).name().equals("STRING")).findFirst().orElseThrow();
      return (String) QUERY.invoke(null, document, mode + " $.a", stringType, wrapper,
          JsonQueryOnEmptyOrError.NULL, JsonQueryOnEmptyOrError.EMPTY_OBJECT);
    } catch (ReflectiveOperationException failure) {
      throw new AssertionError(failure);
    }
  }
}
