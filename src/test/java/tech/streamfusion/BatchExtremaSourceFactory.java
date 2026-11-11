package tech.streamfusion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Deterministic rowwise fixture; both engines retain this same bounded Flink source and map. */
public final class BatchExtremaSourceFactory implements DynamicTableSourceFactory {
  private static final ConfigOption<Long> ROWS =
      ConfigOptions.key("rows").longType().defaultValue(1037L);
  private static final ConfigOption<Long> CARDINALITY =
      ConfigOptions.key("cardinality").longType().defaultValue(2147483647L);
  private static final BigInteger MAXIMUM = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);

  @Override
  public String factoryIdentifier() {
    return "batch-extrema-values";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(ROWS, CARDINALITY);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    var helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    long rows = helper.getOptions().get(ROWS), cardinality = helper.getOptions().get(CARDINALITY);
    if (rows <= 0 || cardinality <= 0)
      throw new IllegalArgumentException("Positive rows/cardinality required");
    RowType type =
        (RowType)
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    return new Source(type, rows, cardinality);
  }

  private static final class Source implements ScanTableSource {
    private final RowType type;
    private final long rows, cardinality;

    Source(RowType type, long rows, long cardinality) {
      this.type = type;
      this.rows = rows;
      this.cardinality = cardinality;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
      return new DataStreamScanProvider() {
        @Override
        public boolean isBounded() {
          return true;
        }

        @Override
        public DataStream<RowData> produceDataStream(
            ProviderContext provider, StreamExecutionEnvironment env) {
          long valueCardinality = cardinality;
          return env.fromSequence(0, rows - 1)
              .map((MapFunction<Long, RowData>) id -> row(id, valueCardinality))
              .returns(InternalTypeInfo.of(type))
              .name("ExtremaRowValues");
        }
      };
    }

    @Override
    public DynamicTableSource copy() {
      return new Source(type, rows, cardinality);
    }

    @Override
    public String asSummaryString() {
      return "BoundedExtremaRows";
    }
  }

  static RowData row(long id, long cardinality) {
    if (id % 17 == 0) return GenericRowData.of(id, null, null, null, null);
    long value = Math.floorMod(id * 48271, cardinality);
    BigInteger unscaled = MAXIMUM.subtract(BigInteger.valueOf(value));
    if ((id & 1) == 0) unscaled = unscaled.negate();
    return GenericRowData.of(
        id,
        (id & 1) == 0,
        DecimalData.fromBigDecimal(new BigDecimal(unscaled, 18), 38, 18),
        (int) (value % 3652059) - 719162,
        (int) (value % 86400000));
  }
}
