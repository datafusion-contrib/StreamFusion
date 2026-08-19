package tech.streamfusion.delta;

import io.delta.flink.kernel.DelegatingEngine;
import io.delta.kernel.engine.*;
import java.util.List;

/** Delta Kernel engine wrapper replacing only data-file Parquet writes. */
final class NativeDeltaEngine implements DelegatingEngine {
  private final Engine delegate;
  private final ParquetHandler parquet;

  NativeDeltaEngine(Engine delegate, org.apache.hadoop.conf.Configuration configuration) {
    this.delegate = delegate;
    this.parquet = new NativeDeltaParquetHandler(delegate.getParquetHandler(), configuration);
  }

  @Override public Engine getDelegateEngine() { return delegate; }

  @Override public ExpressionHandler getExpressionHandler() { return delegate.getExpressionHandler(); }
  @Override public JsonHandler getJsonHandler() { return delegate.getJsonHandler(); }
  @Override public FileSystemClient getFileSystemClient() { return delegate.getFileSystemClient(); }
  @Override public ParquetHandler getParquetHandler() { return parquet; }
  @Override public List<MetricsReporter> getMetricsReporters() { return delegate.getMetricsReporters(); }
}
