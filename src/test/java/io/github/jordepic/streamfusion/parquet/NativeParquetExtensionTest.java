package io.github.jordepic.streamfusion.parquet;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-parquet")
class NativeParquetExtensionTest {

  @Test
  void loadsTheParquetJniFacadeFromTheDevelopmentLibrary() {
    assertTrue(NativeParquet.isLoaded());
  }
}
