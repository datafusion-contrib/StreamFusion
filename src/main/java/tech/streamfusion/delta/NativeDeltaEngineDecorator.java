package tech.streamfusion.delta;

import io.delta.flink.kernel.EngineDecorator;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;

/** Installs StreamFusion's Arrow-to-Parquet service without changing Delta table semantics. */
public final class NativeDeltaEngineDecorator implements EngineDecorator {
  @Override
  public Engine decorate(Engine engine, Configuration configuration) {
    return new NativeDeltaEngine(engine, configuration);
  }
}
