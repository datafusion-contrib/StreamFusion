package tech.streamfusion.state;

import java.util.Base64;
import java.util.Map;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageFactory;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;

/** Keeps job identity projection separate from the configured storage provider. */
public final class CanonicalCheckpointStorageFactory
    implements CheckpointStorageFactory<CheckpointStorage> {
  static final String IDENTITIES = "streamfusion.canonical.operator-identities";
  static final String DELEGATE = "streamfusion.canonical.delegate-storage";
  static final String ACTIVE = "streamfusion.canonical.metadata-projection-active";

  public static void reset(Configuration config) {
    config.setString(ACTIVE, "false");
  }

  public static void install(
      StreamExecutionEnvironment environment,
      Configuration config,
      Map<String, String> identities) {
    install(environment, config, new OperatorIdentityProjection(identities));
  }

  public static void install(
      StreamExecutionEnvironment environment,
      Configuration config,
      OperatorIdentityProjection projection) {
    Configuration pipeline = Configuration.fromMap(environment.getConfiguration().toMap());
    pipeline.addAll(config);
    String configured = pipeline.getOptional(CheckpointingOptions.CHECKPOINT_STORAGE).orElse("");
    if (!configured.equals(CanonicalCheckpointStorageFactory.class.getName())) {
      pipeline.setString(DELEGATE, configured);
    }
    try {
      pipeline.setString(
          IDENTITIES,
          Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(projection)));
    } catch (java.io.IOException failure) {
      throw new IllegalStateException("Cannot configure canonical operator identities", failure);
    }
    pipeline.set(
        CheckpointingOptions.CHECKPOINT_STORAGE, CanonicalCheckpointStorageFactory.class.getName());
    config.addAll(pipeline);
    config.setString(ACTIVE, "true");
  }

  @Override
  public CheckpointStorage createFromConfig(ReadableConfig config, ClassLoader loader) {
    Configuration delegated = Configuration.fromMap(config.toMap());
    String storage = delegated.getString(DELEGATE, "");
    if (storage.isEmpty()) delegated.removeConfig(CheckpointingOptions.CHECKPOINT_STORAGE);
    else delegated.set(CheckpointingOptions.CHECKPOINT_STORAGE, storage);
    try {
      CheckpointStorage provider =
          CheckpointStorageLoader.load(
              null, new HashMapStateBackend(), delegated, new Configuration(), loader, null);
      if (!Boolean.parseBoolean(config.toMap().getOrDefault("streamfusion.native.enabled", "true"))
          || !Boolean.parseBoolean(config.toMap().getOrDefault(ACTIVE, "false"))) {
        return provider;
      }
      OperatorIdentityProjection projection =
          InstantiationUtil.deserializeObject(
              Base64.getDecoder().decode(delegated.getString(IDENTITIES, "")), loader);
      return new CanonicalCheckpointStorage(provider, projection);
    } catch (Exception failure) {
      throw new IllegalConfigurationException(
          "Cannot configure canonical checkpoint storage", failure);
    }
  }
}
