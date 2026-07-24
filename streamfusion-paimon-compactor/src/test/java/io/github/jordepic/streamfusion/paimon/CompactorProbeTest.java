package io.github.jordepic.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.state.StateTableCompactor;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/**
 * Guards the ServiceLoader registration itself: the parent build replaces the conventional
 * resource directory (for the native library tree), which once silently dropped this module's
 * META-INF/services file — making the backend run every table unmaintained.
 */
class CompactorProbeTest {
  @Test
  void discoveryAndSupportProbe() {
    JavaPaimonStateCompactor compactor = new JavaPaimonStateCompactor();
    assertTrue(compactor.available(), "available");
    assertTrue(compactor.supports("parquet"), "supports parquet");
    boolean found = false;
    for (StateTableCompactor c :
        ServiceLoader.load(StateTableCompactor.class, StateTableCompactor.class.getClassLoader())) {
      found = true;
    }
    assertTrue(found, "service loader finds the compactor");
  }
}
