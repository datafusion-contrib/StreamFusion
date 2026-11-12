package tech.streamfusion.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NativeModuleOwnershipTest {
  @Test
  void optionalImplementationsComeFromTheirDeploymentModules() throws Exception {
    Map<String, String> owners =
        Map.of(
            "tech.streamfusion.kafka.NativeKafka", "kafka",
            "tech.streamfusion.kafka.ConfluentSchemaRegistry", "avro-confluent-registry",
            "tech.streamfusion.format.json.NativeJsonFormat", "json",
            "tech.streamfusion.format.csv.NativeCsvFormat", "csv",
            "tech.streamfusion.format.raw.NativeRawFormat", "raw",
            "tech.streamfusion.format.avro.NativeAvroFormat", "avro",
            "tech.streamfusion.format.protobuf.NativeProtobufFormat", "protobuf",
            "tech.streamfusion.parquet.NativeParquet", "parquet",
            "tech.streamfusion.orc.NativeOrc", "orc");
    ClassLoader loader = getClass().getClassLoader();
    for (Map.Entry<String, String> entry : owners.entrySet()) {
      String name = entry.getKey();
      String artifact = "streamfusion-" + entry.getValue();
      Class<?> implementation = Class.forName(name, false, loader);
      Path origin =
          Path.of(implementation.getProtectionDomain().getCodeSource().getLocation().toURI());
      boolean moduleOutput = origin.endsWith(Path.of(artifact, "target", "classes"));
      boolean moduleJar =
          origin.getFileName().toString().matches(artifact + "-(?:flink1\\.18-)?[0-9].*\\.jar");
      assertTrue(moduleOutput || moduleJar, name + " loaded from " + origin);
      String resource = name.replace('.', '/') + ".class";
      List<URL> definitions = Collections.list(loader.getResources(resource));
      assertEquals(1, definitions.size(), name + " has duplicate definitions: " + definitions);
    }
  }
}
