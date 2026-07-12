package io.github.jordepic.streamfusion.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.format.avro.AvroFormatProvider;
import io.github.jordepic.streamfusion.format.avro.NativeAvroFormat;
import io.github.jordepic.streamfusion.format.avroconfluent.AvroConfluentFormatProvider;
import io.github.jordepic.streamfusion.format.csv.CsvFormatProvider;
import io.github.jordepic.streamfusion.format.csv.NativeCsvFormat;
import io.github.jordepic.streamfusion.format.json.JsonFormatProvider;
import io.github.jordepic.streamfusion.format.json.NativeJsonFormat;
import io.github.jordepic.streamfusion.format.protobuf.NativeProtobufFormat;
import io.github.jordepic.streamfusion.format.protobuf.ProtobufFormatProvider;
import io.github.jordepic.streamfusion.format.raw.NativeRawFormat;
import io.github.jordepic.streamfusion.format.raw.RawFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Direct JNI and provider smoke tests for each independently deployed format artifact. */
class OptionalFormatFacadeTest {

  @Test
  @Tag("streamfusion-json")
  void jsonFacadeLoads() {
    assertTrue(NativeJsonFormat.isLoaded());
    assertNotEquals(0, NativeJsonFormat.driverInitAddress());
    assertEquals("json", new JsonFormatProvider().formatIdentifier());
  }

  @Test
  @Tag("streamfusion-csv")
  void csvFacadeLoads() {
    assertTrue(NativeCsvFormat.isLoaded());
    assertNotEquals(0, NativeCsvFormat.driverInitAddress());
    assertEquals("csv", new CsvFormatProvider().formatIdentifier());
  }

  @Test
  @Tag("streamfusion-raw")
  void rawFacadeLoads() {
    assertTrue(NativeRawFormat.isLoaded());
    assertNotEquals(0, NativeRawFormat.driverInitAddress());
    assertEquals("raw", new RawFormatProvider().formatIdentifier());
  }

  @Test
  @Tag("streamfusion-avro")
  void avroFacadeLoads() {
    assertTrue(NativeAvroFormat.isLoaded());
    assertNotEquals(0, NativeAvroFormat.driverInitAddress());
    assertEquals("avro", new AvroFormatProvider().formatIdentifier());
  }

  @Test
  @Tag("streamfusion-avro-confluent")
  void avroConfluentFacadeLoads() {
    assertTrue(NativeAvroFormat.isLoaded());
    assertEquals("avro-confluent", new AvroConfluentFormatProvider().formatIdentifier());
  }

  @Test
  @Tag("streamfusion-protobuf")
  void protobufFacadeLoads() {
    assertTrue(NativeProtobufFormat.isLoaded());
    assertNotEquals(0, NativeProtobufFormat.driverInitAddress());
    assertEquals("protobuf", new ProtobufFormatProvider().formatIdentifier());
  }
}
