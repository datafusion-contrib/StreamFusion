package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PaimonTestWatchTest {
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  @BeforeEach
  void captureDiagnostics() {
    PaimonTestWatch.initialize(new PrintStream(output, true, StandardCharsets.UTF_8));
  }

  @AfterEach
  void restoreDiagnostics() {
    PaimonTestWatch.initialize(System.err);
  }

  @Test
  void reportsFixturesWithoutRandomizedDefaults() {
    PaimonTestWatch.start("ContinuousFileStoreITCase.testScanFromOldSchema", new Object())
        .finish(null);
    String log = output.toString(StandardCharsets.UTF_8);
    assertTrue(log.contains("test started ContinuousFileStoreITCase.testScanFromOldSchema at "));
    assertTrue(log.contains("randomized table defaults none"));
    assertTrue(
        log.contains("test finished ContinuousFileStoreITCase.testScanFromOldSchema after "));
    assertTrue(log.contains("seconds: passed"));
    assertFalse(log.contains("unavailable"));
  }

  @Test
  void retainsInheritedRandomizedDefaults() {
    PaimonTestWatch.start("DerivedFixture.testWrite", new DerivedFixture()).finish(null);
    assertTrue(
        output.toString(StandardCharsets.UTF_8).contains("{sink.writer-coordinator.enabled=true}"));
  }

  @Test
  void printsTheFailureBeforeTheReportIsWritten() {
    PaimonTestWatch.start("ReadWriteTableITCase.testSinkParallelism", new Object())
        .finish(new IllegalStateException("fixture linkage failure"));
    String log = output.toString(StandardCharsets.UTF_8);
    assertTrue(log.contains("seconds: java.lang.IllegalStateException"));
    assertTrue(log.contains("java.lang.IllegalStateException: fixture linkage failure"));
    assertTrue(log.contains("at tech.streamfusion.suite.PaimonTestWatchTest."));
  }

  private static class BaseFixture {
    private final Map<String, String> tableDefaultProperties =
        Map.of("sink.writer-coordinator.enabled", "true");
  }

  private static final class DerivedFixture extends BaseFixture {}
}
