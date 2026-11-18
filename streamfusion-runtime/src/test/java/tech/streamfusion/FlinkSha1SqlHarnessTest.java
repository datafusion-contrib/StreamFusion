package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkSha1SqlHarnessTest {
  @Test
  void hashesUtf8BytesIncludingNullEmptyNulAndBlockBoundaries() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null,
                "",
                "abc",
                "a\u0000b",
                "\u00e9\u4e2d\ud83d\ude00",
                "a".repeat(55),
                "a".repeat(56),
                "a".repeat(63),
                "a".repeat(64),
                "\u4e2d\ud83d\ude00".repeat(4097)),
        "SELECT id, SHA1(s) FROM inputs");
  }

  @Test
  void composesInProjectionAndPredicate() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows(null, "", "abc", "abcdef"),
        "SELECT id, SHA1(CONCAT(s, '!')) FROM inputs WHERE SHA1(s) <> SHA1('abc')");
  }
}
