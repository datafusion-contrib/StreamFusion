package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.state.StateTableCompactor;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/**
 * Guards the ServiceLoader registration itself: the parent build replaces the conventional
 * resource directory (for the native library tree), which once silently dropped this module's
 * META-INF/services file — leaving the backend nothing to discover, so it would fail closed at
 * creation. Also pins the capability validations the backend requires of the bundled Paimon.
 */
class CompactorProbeTest {
  @Test
  void discoveryAndSupportProbe() {
    JavaPaimonStateCompactor compactor = new JavaPaimonStateCompactor();
    assertTrue(compactor.available(), "available");
    assertTrue(compactor.supports("parquet"), "supports parquet");
    assertTrue(
        compactor.supportsDeletionVectors(),
        "the default bundle must carry the binary-key lookup comparator fix"
            + " (apache/paimon#8873); the backend refuses to start otherwise");
    boolean found = false;
    for (StateTableCompactor c :
        ServiceLoader.load(StateTableCompactor.class, StateTableCompactor.class.getClassLoader())) {
      found = true;
    }
    assertTrue(found, "service loader finds the compactor");
  }
}
