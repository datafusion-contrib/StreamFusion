package tech.streamfusion.operator;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Disables the post-exchange coalescer for operator-level tests that assert per-push emission —
 * they exercise the operator's ingest contract directly, and buffering would defer every output to
 * a flush boundary the test never reaches. The coalesced path has its own coverage
 * ({@link BatchCoalescerTest} and the parallelism-2 SQL parity tests).
 */
public final class CoalescingOff implements BeforeAllCallback, AfterAllCallback {

  @Override
  public void beforeAll(ExtensionContext context) {
    System.setProperty("streamfusion.exchange.coalesceRows", "0");
  }

  @Override
  public void afterAll(ExtensionContext context) {
    System.clearProperty("streamfusion.exchange.coalesceRows");
  }
}
