package tech.streamfusion;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import tech.streamfusion.planner.Utf8StringSource;

/** Registers fixture data whose generator only emits valid UTF-8. */
final class VerifiedUtf8Fixture {
  private VerifiedUtf8Fixture() {}

  static void register(
      StreamTableEnvironment table,
      String name,
      Function<StreamExecutionEnvironment, DataStream<RowData>> data,
      DataType type) {
    register(table, name, data, Schema.newBuilder().fromRowDataType(type).build());
  }

  static void register(
      StreamTableEnvironment table,
      String name,
      Function<StreamExecutionEnvironment, DataStream<RowData>> data,
      Schema schema) {
    var factory = new FixtureFactory(data);
    String catalog = "verified_utf8_" + name;
    table.registerCatalog(
        catalog,
        new GenericInMemoryCatalog(catalog, "fixtures") {
          @Override
          public Optional<Factory> getFactory() {
            return Optional.of(factory);
          }
        });
    String qualified = catalog + ".fixtures." + name;
    table.createTable(
        qualified,
        TableDescriptor.forConnector(factory.factoryIdentifier())
            .schema(schema)
            .build());
    table.createTemporaryView(name, table.from(qualified));
  }

  private static final class FixtureFactory implements DynamicTableSourceFactory {
    private final Function<StreamExecutionEnvironment, DataStream<RowData>> data;

    FixtureFactory(Function<StreamExecutionEnvironment, DataStream<RowData>> data) {
      this.data = data;
    }

    @Override
    public String factoryIdentifier() {
      return "verified-utf8-fixture";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
      return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
      return Set.of();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
      return new FixtureSource(data);
    }
  }

  private static final class FixtureSource
      implements ScanTableSource, DataStreamScanProvider, Utf8StringSource {
    private final Function<StreamExecutionEnvironment, DataStream<RowData>> data;

    FixtureSource(Function<StreamExecutionEnvironment, DataStream<RowData>> data) {
      this.data = data;
    }

    @Override
    public boolean guaranteesUtf8Strings() {
      return true;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
      return this;
    }

    @Override
    public boolean isBounded() {
      return true;
    }

    @Override
    public DataStream<RowData> produceDataStream(
        ProviderContext provider, StreamExecutionEnvironment environment) {
      return data.apply(environment);
    }

    @Override
    public DynamicTableSource copy() {
      return new FixtureSource(data);
    }

    @Override
    public String asSummaryString() {
      return "VerifiedUtf8Fixture";
    }
  }
}
