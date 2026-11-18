package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.junit.jupiter.api.Test;

class NativeStringCaseTest {
  @Test
  void cachedAndBinaryAsciiKeepTheirDifferentFlinkLocaleRules() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertEquals("\u0131", NativeBuiltinFunctions.lower(BinaryStringData.fromString("I")).toString());
      assertEquals("\u0130", NativeBuiltinFunctions.upper(BinaryStringData.fromString("i")).toString());
      assertEquals("i", NativeBuiltinFunctions.lower(binary("I")).toString());
      assertEquals("I", NativeBuiltinFunctions.upper(binary("i")).toString());
      assertEquals("\ud800", NativeBuiltinFunctions.lower(BinaryStringData.fromString("\ud800")).toString());
    } finally {
      Locale.setDefault(previous);
    }
  }

  private static BinaryStringData binary(String value) {
    return BinaryStringData.fromBytes(value.getBytes(StandardCharsets.UTF_8));
  }
}
