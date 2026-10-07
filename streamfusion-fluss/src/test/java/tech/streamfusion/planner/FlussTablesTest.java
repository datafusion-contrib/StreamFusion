package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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

class FlussTablesTest {
  private static final RowType TYPE = RowType.of(new BigIntType());

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

  private static Configuration config() {
    Configuration config = new Configuration();
    config.setString("bootstrap.servers", "localhost:9123");
    return config;
  }

  private static FlinkTableSource source(
      boolean primaryKey, ScanStartupMode mode, Configuration config) {
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
        Map.of(),
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
