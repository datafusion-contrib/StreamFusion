package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import org.apache.fluss.flink.sink.FlinkTableSink;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlussTablesTest {
  private static final RowType TYPE = RowType.of(new BigIntType());

  @Test
  void sourceSharingDistinguishesPredicateLiteralsWithTheSameDisplayText() throws Exception {
    var type = org.apache.fluss.types.RowType.of(org.apache.fluss.types.DataTypes.STRING());
    var builder = new org.apache.fluss.predicate.PredicateBuilder(type);
    List<Object> oneLiterals = new java.util.ArrayList<>();
    List<Object> twoLiterals = new java.util.ArrayList<>();
    oneLiterals.add(org.apache.fluss.row.BinaryString.fromString("a, b"));
    twoLiterals.add(org.apache.fluss.row.BinaryString.fromString("a"));
    twoLiterals.add(org.apache.fluss.row.BinaryString.fromString("b"));
    for (int i = 0; i < 21; i++) {
      var literal = org.apache.fluss.row.BinaryString.fromString("item" + i);
      oneLiterals.add(literal);
      twoLiterals.add(literal);
    }
    var one = builder.in(0, oneLiterals);
    var two = builder.in(0, twoLiterals);
    org.junit.jupiter.api.Assertions.assertEquals(one.toString(), two.toString());
    org.junit.jupiter.api.Assertions.assertNotEquals(
        FlussTables.predicateKey(one), FlussTables.predicateKey(two));
  }

  @Test
  void admitsAppendAndPrimaryKeyLogsButKeepsInitialSnapshotsOnFlink() throws Exception {
    assertNull(FlussTables.sourceFallback(source(false, ScanStartupMode.FULL, config())));
    for (ScanStartupMode startup :
        List.of(ScanStartupMode.EARLIEST, ScanStartupMode.LATEST, ScanStartupMode.TIMESTAMP)) {
      assertNull(FlussTables.sourceFallback(source(false, startup, config())));
      assertNull(FlussTables.sourceFallback(source(true, startup, config())));
    }
    assertNotNull(FlussTables.sourceFallback(source(true, ScanStartupMode.FULL, config())));
  }

  @Test
  void declinesLimitsRowLogsAndCustomClientSettings() throws Exception {
    FlinkTableSource limited = source(false, ScanStartupMode.EARLIEST, config());
    limited.applyLimit(10);
    assertNotNull(FlussTables.sourceFallback(limited));
    Configuration indexed = config();
    indexed.set(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.INDEXED);
    assertNotNull(FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, indexed)));
    Configuration custom = config();
    custom.setString("client.scanner.log.fetch.max.bytes", "4mb");
    assertNotNull(FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, custom)));
  }

  @Test
  void keepsPrimaryKeyWritesAndKeyedBucketRoutingOnFlink() throws Exception {
    assertNull(FlussTables.sinkFallback(sink(false, List.of(), config())));
    assertNotNull(FlussTables.sinkFallback(sink(true, List.of(), config())));
    assertNotNull(FlussTables.sinkFallback(sink(false, List.of("f0"), config())));
    Configuration custom = config();
    custom.setString("client.writer.acks", "0");
    assertNotNull(FlussTables.sinkFallback(sink(false, List.of(), custom)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("clientSettings")
  void unverifiedClientSettingsFallBackWithoutLeakingTheirValues(String key) throws Exception {
    Configuration settings = config();
    settings.setString(key, "unverified-sensitive-value");
    for (String reason :
        List.of(
            FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, settings)),
            FlussTables.sourceFallback(source(true, ScanStartupMode.EARLIEST, settings)),
            FlussTables.sinkFallback(sink(false, List.of(), settings)))) {
      assertTrue(reason.contains(key), reason);
      assertTrue(!reason.contains("unverified-sensitive-value"), reason);
    }
  }

  static java.util.stream.Stream<String> clientSettings() throws Exception {
    java.util.Set<String> keys = new java.util.TreeSet<>();
    for (var field : ConfigOptions.class.getFields()) {
      if (field.getType() == org.apache.fluss.config.ConfigOption.class) {
        String key = ((org.apache.fluss.config.ConfigOption<?>) field.get(null)).key();
        if (key.startsWith("client.") || key.startsWith("netty.client.")) keys.add(key);
      }
    }
    keys.add("client.future.option");
    keys.add("client.fs.s3.endpoint");
    keys.add("client.scanner.remote-log.fetch.max-retries");
    keys.add("netty.client.future.option");
    keys.add("unknown.transport.option");
    return keys.stream().filter(key -> !tech.streamfusion.fluss.FlussClientOptions.supported(key));
  }

  @Test
  void configuredDefaultsStillFallBackAndBootstrapListsRemainAdmitted() throws Exception {
    Configuration explicitDefault = config();
    explicitDefault.set(ConfigOptions.CLIENT_WRITER_ACKS, "all");
    assertNotNull(FlussTables.sinkFallback(sink(false, List.of(), explicitDefault)));
    Configuration bootstrap = config();
    bootstrap.setString("bootstrap.servers", "first:9123,second:9123");
    assertNull(FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, bootstrap)));
    assertNull(FlussTables.sinkFallback(sink(false, List.of(), bootstrap)));
  }

  @Test
  void tableSettingsDroppedByTheFactoryCannotBypassAdmission() throws Exception {
    for (String key : List.of("netty.client.future.option", "client.future.option")) {
      Map<String, String> options = Map.of(key, "unverified-sensitive-value");
      assertTrue(
          FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, config(), options))
              .contains(key));
      assertTrue(FlussTables.sinkOptionsFallback(options).contains(key));
    }
  }

  @Test
  void admitsSettingsOwnedByTheJavaConnection() throws Exception {
    Map<String, String> options =
        Map.of(
            "client.id", "configured-client",
            "client.connect-timeout", "20s",
            "client.request-timeout", "30s",
            "netty.client.num-network-threads", "2",
            "netty.client.allocator.heap-buffer-first", "true",
            "client.security.protocol", "PLAINTEXT",
            "client.security.enable-plugin-discovery", "false",
            "client.writer.dynamic-create-partition.enabled", "false");
    Configuration configured = config();
    options.forEach(configured::setString);
    assertNull(FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, configured)));
    assertNull(FlussTables.sinkFallback(sink(false, List.of(), configured)));
    assertNull(
        FlussTables.sourceFallback(source(false, ScanStartupMode.EARLIEST, config(), options)));
    assertNull(FlussTables.sinkOptionsFallback(options));
  }

  @Test
  void statisticsEnabledTablesRequireTheStockAppendWriter() {
    assertNull(FlussTables.sinkOptionsFallback(Map.of("table.log.format", "ARROW")));
    for (String columns : List.of("*", "f0"))
      assertTrue(
          FlussTables.sinkOptionsFallback(Map.of("table.statistics.columns", columns))
              .contains("table.statistics.columns"));
  }

  private static Configuration config() {
    Configuration config = new Configuration();
    config.setString("bootstrap.servers", "localhost:9123");
    return config;
  }

  private static FlinkTableSource source(
      boolean primaryKey, ScanStartupMode mode, Configuration config) {
    return source(primaryKey, mode, config, Map.of());
  }

  private static FlinkTableSource source(
      boolean primaryKey, ScanStartupMode mode, Configuration config, Map<String, String> options) {
    var startup = new FlinkConnectorOptionsUtils.StartupOptions();
    startup.startupMode = mode;
    Configuration tableConfig = new Configuration();
    tableConfig.set(ConfigOptions.TABLE_LOG_FORMAT, config.get(ConfigOptions.TABLE_LOG_FORMAT));
    Configuration clientConfig = new Configuration(config);
    clientConfig.removeConfig(ConfigOptions.TABLE_LOG_FORMAT);
    return new FlinkTableSource(
        TablePath.of("db", "events"),
        clientConfig,
        new TableConfig(tableConfig),
        TYPE,
        primaryKey ? new int[] {0} : new int[0],
        new int[0],
        new int[0],
        true,
        startup,
        FlinkConnectorOptionsUtils.BoundedOptions.latestOffset(),
        false,
        false,
        null,
        60000,
        10,
        false,
        null,
        options,
        null);
  }

  private static FlinkTableSink sink(
      boolean primaryKey, List<String> bucketKeys, Configuration config) {
    return new FlinkTableSink(
        TablePath.of("db", "events"),
        config,
        TYPE,
        primaryKey ? new int[] {0} : new int[0],
        List.of(),
        true,
        null,
        null,
        false,
        DeleteBehavior.ALLOW,
        4,
        bucketKeys,
        DistributionMode.AUTO,
        null);
  }
}
