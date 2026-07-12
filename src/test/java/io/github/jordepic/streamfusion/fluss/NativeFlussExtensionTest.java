package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

@Tag("streamfusion-fluss")
@EnabledIf("nativeFlussFeatureBuilt")
class NativeFlussExtensionTest {

  static boolean nativeFlussFeatureBuilt() {
    try {
      return NativeFluss.featureBuilt();
    } catch (LinkageError ignored) {
      return false;
    }
  }

  @Test
  void loadsTheFlussJniFacadeFromTheDevelopmentLibrary() {
    assertTrue(NativeFluss.featureBuilt());
  }
}
