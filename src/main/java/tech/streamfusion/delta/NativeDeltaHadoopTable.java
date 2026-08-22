package tech.streamfusion.delta;

import io.delta.flink.table.HadoopTable;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Published Delta Hadoop table with only its data-file Parquet service replaced. */
public final class NativeDeltaHadoopTable extends HadoopTable {
  private final Map<String, String> hadoopOptions;

  public NativeDeltaHadoopTable(
      URI tablePath,
      Map<String, String> configurations,
      StructType schema,
      List<String> partitionColumns) {
    super(tablePath, configurations, schema, partitionColumns);
    hadoopOptions = Map.copyOf(configurations);
  }

  @Override
  public CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues) {
    return withRetry(
        () -> {
          Engine engine = getEngine();
          Row writeState = getWriteState();
          CloseableIterator<FilteredColumnarBatch> physicalData =
              Transaction.transformLogicalData(engine, writeState, data, partitionValues);
          DataWriteContext writeContext =
              Transaction.getWriteContext(engine, writeState, partitionValues);
          Configuration configuration = new Configuration();
          hadoopOptions.forEach(configuration::set);
          CloseableIterator<DataFileStatus> dataFiles =
              new NativeDeltaParquetHandler(engine.getParquetHandler(), configuration)
                  .writeParquetFiles(
                      getTablePath().resolve(pathSuffix).toString(),
                      physicalData,
                      writeContext.getStatisticsColumns());
          return Transaction.generateAppendActions(engine, writeState, dataFiles, writeContext);
        });
  }
}
