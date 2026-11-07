package tech.streamfusion.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.slf4j.LoggerFactory;

/** Reconstitutes the original backend with the job's stock-schema import contracts. */
public final class CanonicalStateBackendFactory implements StateBackendFactory<StateBackend> {
  @Override
  public StateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
      throws java.io.IOException {
    Configuration delegated = Configuration.fromMap(config.toMap());
    delegated.setString(
        "state.backend.type", delegated.getString(CanonicalStateBackend.DELEGATE, "hashmap"));
    try {
      StateBackend backend =
          StateBackendLoader.loadStateBackendFromConfig(
              delegated, classLoader, LoggerFactory.getLogger(CanonicalStateBackendFactory.class));
      if (backend == null) backend = new HashMapStateBackend();
      if (!Boolean.parseBoolean(
          config.toMap().getOrDefault("streamfusion.native.enabled", "true"))) {
        return backend;
      }
      return new CanonicalStateBackend(
          backend, CanonicalStateBackend.readContracts(delegated, classLoader));
    } catch (Exception failure) {
      throw new java.io.IOException("Cannot construct canonical state backend", failure);
    }
  }
}
