/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.loader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

/**
 * StreamFusion's planner loader shim for its declared Flink line.
 *
 * <p>The class intentionally has Flink's loader name so it is selected before Flink's bundled
 * loader when installed first in {@code $FLINK_HOME/lib}. It adds a StreamFusion planner payload to
 * the isolated component classloader and directly instantiates that payload's factory. All other
 * planner behavior continues through Flink's normal implementation.
 */
@Internal
public class PlannerModule {

  static final String FLINK_TABLE_PLANNER_FAT_JAR = "flink-table-planner.jar";
  private static final String STREAMFUSION_PLANNER_JAR = "streamfusion-planner.jar";
  private static final Set<String> STREAMFUSION_PLANNER_EXTENSIONS =
      Set.of(
          "streamfusion-kafka",
          "streamfusion-json",
          "streamfusion-csv",
          "streamfusion-raw",
          "streamfusion-avro",
          "streamfusion-avro-confluent-registry",
          "streamfusion-protobuf",
          "streamfusion-parquet");
  private static final String STREAMFUSION_PLANNER_FACTORY =
      "tech.streamfusion.planner.StreamFusionPlannerFactory";

  private static final String[] OWNER_CLASSPATH =
      Stream.concat(
              Arrays.stream(CoreOptions.PARENT_FIRST_LOGGING_PATTERNS),
              Stream.of(
                  "org.codehaus.janino",
                  "org.codehaus.commons",
                  "org.apache.commons.lang3",
                  "org.apache.commons.math3",
                  "org.apache.commons.text",
                  "org.rocksdb",
                  "org.apache.hadoop"))
          .toArray(String[]::new);

  private static final String[] COMPONENT_CLASSPATH = {"org.apache.flink", "tech.streamfusion"};

  private static final Map<String, String> KNOWN_MODULE_ASSOCIATIONS = new HashMap<>();

  static {
    KNOWN_MODULE_ASSOCIATIONS.put("org.apache.flink.table.runtime", "flink-table-runtime");
    KNOWN_MODULE_ASSOCIATIONS.put("org.apache.flink.formats.raw", "flink-table-runtime");
    KNOWN_MODULE_ASSOCIATIONS.put("org.codehaus.janino", "flink-table-runtime");
    KNOWN_MODULE_ASSOCIATIONS.put("org.codehaus.commons", "flink-table-runtime");
    KNOWN_MODULE_ASSOCIATIONS.put(
        "org.apache.flink.table.shaded.com.jayway", "flink-table-runtime");
  }

  private final PlannerComponentClassLoader submoduleClassLoader;

  private PlannerModule() {
    try {
      verifyFlinkVersion();
      ClassLoader flinkClassLoader = PlannerModule.class.getClassLoader();
      Path temporaryDirectory =
          Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
      Files.createDirectories(FileUtils.getTargetPathIfContainsSymbolicPath(temporaryDirectory));

      URL streamFusionPlanner =
          extractResource(flinkClassLoader, temporaryDirectory, STREAMFUSION_PLANNER_JAR);
      String flinkLine = FlinkPayloadIdentity.loaderLine();
      FlinkPayloadIdentity.verify(
          streamFusionPlanner, FlinkPayloadIdentity.attributes(streamFusionPlanner), flinkLine);
      URL flinkPlanner =
          extractResource(flinkClassLoader, temporaryDirectory, FLINK_TABLE_PLANNER_FAT_JAR);
      List<URL> plannerClasspath = new ArrayList<>();
      plannerClasspath.add(streamFusionPlanner);
      plannerClasspath.addAll(extensionJars(flinkLine));
      plannerClasspath.add(flinkPlanner);
      this.submoduleClassLoader =
          new PlannerComponentClassLoader(
              plannerClasspath.toArray(new URL[0]),
              flinkClassLoader,
              OWNER_CLASSPATH,
              COMPONENT_CLASSPATH,
              KNOWN_MODULE_ASSOCIATIONS);
    } catch (IOException e) {
      throw new TableException("Could not initialize the StreamFusion table planner loader.", e);
    }
  }

  public URLClassLoader getSubmoduleClassLoader() {
    return submoduleClassLoader;
  }

  public void addUrlToClassLoader(URL url) {
    submoduleClassLoader.addURL(url);
  }

  public ExecutorFactory loadExecutorFactory() {
    return FactoryUtil.discoverFactory(
        submoduleClassLoader, ExecutorFactory.class, ExecutorFactory.DEFAULT_IDENTIFIER);
  }

  public PlannerFactory loadPlannerFactory() {
    try {
      Class<?> factoryClass =
          Class.forName(STREAMFUSION_PLANNER_FACTORY, true, submoduleClassLoader);
      if (!PlannerFactory.class.isAssignableFrom(factoryClass)) {
        throw new TableException(
            String.format(
                "StreamFusion planner factory '%s' does not implement '%s'.",
                STREAMFUSION_PLANNER_FACTORY, PlannerFactory.class.getName()));
      }
      return (PlannerFactory) factoryClass.getConstructor().newInstance();
    } catch (ReflectiveOperationException | LinkageError e) {
      throw new TableException("Could not load the StreamFusion planner factory.", e);
    }
  }

  public static PlannerModule getInstance() {
    return PlannerComponentsHolder.INSTANCE;
  }

  /** Validates an installed image before its entrypoint starts any Flink daemon. */
  public static void main(String[] args) throws IOException {
    verifyFlinkVersion();
    String line = FlinkPayloadIdentity.loaderLine();
    URL core = PlannerModule.class.getClassLoader().getResource(STREAMFUSION_PLANNER_JAR);
    if (core == null) {
      throw new TableException(
          "Could not find planner resource '" + STREAMFUSION_PLANNER_JAR + "'.");
    }
    FlinkPayloadIdentity.verify(core, FlinkPayloadIdentity.attributes(core), line);
    extensionJars(line);
    System.out.println("StreamFusion payloads verified for Flink " + line);
  }

  private static void verifyFlinkVersion() throws IOException {
    String line = FlinkPayloadIdentity.loaderLine();
    Set<String> supportedVersions =
        switch (line) {
          case "2.2" -> Set.of("2.2.0", "2.2.1");
          case "1.18" -> Set.of("1.18.1");
          default -> throw new TableException("Unverified StreamFusion target Flink line: " + line);
        };
    Package flinkApiPackage = PlannerFactory.class.getPackage();
    String version = flinkApiPackage == null ? null : flinkApiPackage.getImplementationVersion();
    if (version == null || !supportedVersions.contains(version)) {
      throw new TableException(
          String.format(
              "StreamFusion loader targets Flink %s (supported versions %s), but found %s."
                  + " Refusing to cross an unverified planner ABI boundary.",
              line,
              supportedVersions,
              version == null ? "an unversioned Flink API" : "Flink " + version));
    }
  }

  private static URL extractResource(
      ClassLoader classLoader, Path temporaryDirectory, String resource) throws IOException {
    InputStream input = classLoader.getResourceAsStream(resource);
    if (input == null) {
      throw new TableException("Could not find planner resource '" + resource + "'.");
    }

    Path output =
        Files.createFile(temporaryDirectory.resolve(resource + "_" + UUID.randomUUID() + ".jar"));
    try (InputStream resourceStream = input) {
      IOUtils.copyBytes(resourceStream, Files.newOutputStream(output));
    }
    output.toFile().deleteOnExit();
    return output.toUri().toURL();
  }

  /** Returns explicitly installed connector extensions, never arbitrary user or connector JARs. */
  private static List<URL> extensionJars(String flinkLine) throws IOException {
    LinkedHashSet<Path> installed = new LinkedHashSet<>();
    String flinkHome = System.getenv("FLINK_HOME");
    if (flinkHome != null && !flinkHome.isBlank()) {
      collectExtensions(Paths.get(flinkHome, "lib"), installed);
    }
    // Embedded/local clients commonly start without FLINK_HOME. Respect extension artifacts that
    // are actual classpath entries as well; directory trees and arbitrary connector JARs remain
    // excluded.
    for (String entry :
        System.getProperty("java.class.path", "").split(java.io.File.pathSeparator)) {
      if (!entry.isBlank()) {
        Path path = Paths.get(entry).toAbsolutePath().normalize();
        if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(".jar")) {
          installed.add(path);
        }
      }
    }
    List<URL> extensions = new ArrayList<>();
    for (Path path : installed.stream().sorted().toList()) {
      URL url = toUrl(path);
      Attributes attributes = FlinkPayloadIdentity.attributes(url);
      String module = attributes.getValue(FlinkPayloadIdentity.MODULE_ATTRIBUTE);
      if (module != null && module.startsWith("streamfusion-")
          || attributes.getValue(FlinkPayloadIdentity.LINE_ATTRIBUTE) != null
          || path.getFileName().toString().matches("(?:[0-9]+-)?streamfusion-.*\\.jar")) {
        FlinkPayloadIdentity.verify(url, attributes, flinkLine);
        String suffix = "-flink" + flinkLine;
        String logicalModule =
            module != null && module.endsWith(suffix)
                ? module.substring(0, module.length() - suffix.length())
                : module;
        if (logicalModule != null && STREAMFUSION_PLANNER_EXTENSIONS.contains(logicalModule))
          extensions.add(url);
      }
    }
    return extensions;
  }

  private static void collectExtensions(Path directory, Set<Path> installed) throws IOException {
    if (!Files.isDirectory(directory)) {
      return;
    }
    try (Stream<Path> jars = Files.list(directory)) {
      jars.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".jar"))
          .map(path -> path.toAbsolutePath().normalize())
          .forEach(installed::add);
    }
  }

  private static URL toUrl(Path path) {
    try {
      return path.toUri().toURL();
    } catch (IOException e) {
      throw new TableException("Could not load StreamFusion extension '" + path + "'.", e);
    }
  }

  private static class PlannerComponentsHolder {
    private static final PlannerModule INSTANCE = new PlannerModule();
  }

  private static class PlannerComponentClassLoader extends ComponentClassLoader {

    private PlannerComponentClassLoader(
        URL[] classpath,
        ClassLoader ownerClassLoader,
        String[] ownerFirstPackages,
        String[] componentFirstPackages,
        Map<String, String> knownPackagePrefixesModuleAssociation) {
      super(
          classpath,
          ownerClassLoader,
          ownerFirstPackages,
          componentFirstPackages,
          knownPackagePrefixesModuleAssociation);
    }

    @Override
    public void addURL(URL url) {
      super.addURL(url);
    }
  }
}
