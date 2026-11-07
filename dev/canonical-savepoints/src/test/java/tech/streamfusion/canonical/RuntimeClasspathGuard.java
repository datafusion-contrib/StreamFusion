package tech.streamfusion.canonical;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Verifies the stock control cannot accidentally restore through StreamFusion classes. */
public final class RuntimeClasspathGuard implements BeforeAllCallback {
  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    boolean nativeExpected = "native".equals(System.getProperty("canonical.engine"));
    boolean nativePresent;
    try {
      Class.forName(
          "tech.streamfusion.Native", nativeExpected, Thread.currentThread().getContextClassLoader());
      nativePresent = true;
    } catch (ClassNotFoundException absent) {
      nativePresent = false;
    }
    if (nativePresent != nativeExpected) {
      throw new AssertionError(
          "Unexpected StreamFusion runtime visibility for canonical engine "
              + System.getProperty("canonical.engine"));
    }
    System.err.println(
        "Canonical savepoint "
            + (nativeExpected ? "native" : "stock")
            + " runtime classpath verified");
  }
}
