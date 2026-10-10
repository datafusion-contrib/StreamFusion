package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.junit.jupiter.api.Test;

/** Exercises Jackson's JDK-dependent token boundary through actual native Calc evaluation. */
class JdkJsonTokenParityTest {
  @Test
  void everyBmpSuffixMatchesTheRunningFlinkJdk() {
    List<String> documents = new ArrayList<>(65536);
    List<String> expected = new ArrayList<>(65536);
    for (int suffix = 0; suffix <= Character.MAX_VALUE; suffix++) {
      String document = "true" + (char) suffix;
      documents.add(document);
      expected.add(Boolean.toString(SqlJsonUtils.isJsonValue(document)));
    }
    List<String> actual =
        NativeJsonBufferHistoryTest.nativeRows(
            documents, 144, new String[] {NativeJsonBufferHistoryTest.unicodeVersion()}, 1024);
    assertEquals(expected.size(), actual.size());
    for (int suffix = 0; suffix < expected.size(); suffix++) {
      assertEquals(
          expected.get(suffix), actual.get(suffix), "Suffix U+" + Integer.toHexString(suffix));
    }
  }
}
