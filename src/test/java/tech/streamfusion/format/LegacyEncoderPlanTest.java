package tech.streamfusion.format;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ObjectInputStream;
import org.junit.jupiter.api.Test;

class LegacyEncoderPlanTest {
  @Test
  void preFactoryPlansRestoreTheirFormatOwnedEncoderAndChangelogContract() throws Exception {
    try (var input =
        new ObjectInputStream(
            getClass().getResourceAsStream("/runtime-compatibility/encode-format-v1.bin"))) {
      EncodeFormat json = (EncodeFormat) input.readObject();
      EncodeFormat cdc = (EncodeFormat) input.readObject();
      assertEquals(FormatCodes.JSON, json.format);
      assertEquals("timestamp-format=ISO-8601\n", json.openOptions());
      assertFalse(json.acceptsChangelog);
      assertEquals(FormatCodes.DEBEZIUM_JSON, cdc.format);
      assertEquals("encode.ignore-null-fields=true\n", cdc.openOptions());
      assertTrue(cdc.acceptsChangelog);
      EncodeFormat completed = (EncodeFormat) input.readObject();
      assertEquals("timestamp-format=ISO-8601\nencode.ignore-null-fields=true\n", completed.openOptions());
      for (var restored : new EncodeFormat[] {json, cdc, completed}) {
        try (var encoder =
            restored.createEncoder(new String[] {"INTEGER"}, new String[] {"id"}, new int[] {0})) {
          assertNotNull(encoder);
        }
      }
    }
  }
}
