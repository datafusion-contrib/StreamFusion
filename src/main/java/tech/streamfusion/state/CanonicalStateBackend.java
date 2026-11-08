package tech.streamfusion.state;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;
import tech.streamfusion.compat.FlinkStateBackendCompat;
import tech.streamfusion.compat.KeyedBackendContext;

/** Imports stock savepoint handles without changing the native runtime key serializer. */
public final class CanonicalStateBackend extends FlinkStateBackendCompat {
  private static final long serialVersionUID = 1L;
  static final String CONTRACTS = "streamfusion.canonical.state-contracts";
  static final String DELEGATE = "streamfusion.canonical.delegate-backend";
  private final Map<String, BoundContract> contracts;

  CanonicalStateBackend(StateBackend delegate, Map<String, BoundContract> contracts) {
    super(delegate);
    this.contracts = Map.copyOf(contracts);
  }

  public static StateBackend unwrap(StateBackend backend) {
    return backend instanceof CanonicalStateBackend
        ? ((CanonicalStateBackend) backend).delegate
        : backend;
  }

  public static void reset(StreamExecutionEnvironment environment, Configuration config) {
    if (FlinkStateBackendCompat.explicitStateBackend(environment) instanceof CanonicalStateBackend)
      FlinkStateBackendCompat.explicitStateBackend(
          environment, unwrap(FlinkStateBackendCompat.explicitStateBackend(environment)));
    Configuration pipeline = FlinkStateBackendCompat.configuration(environment.getConfiguration());
    pipeline.addAll(config);
    if (pipeline
        .getString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(), "hashmap")
        .equals(CanonicalStateBackendFactory.class.getName())) {
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          pipeline.getString(DELEGATE, "hashmap"));
    }
    config.setString(CONTRACTS, "");
  }

  public static void install(
      StreamExecutionEnvironment environment,
      Configuration config,
      String operatorId,
      String operatorClass,
      StockStateContract contract,
      ClassLoader classLoader) {
    Configuration pipeline = FlinkStateBackendCompat.configuration(environment.getConfiguration());
    pipeline.addAll(config);
    String configured =
        pipeline.getString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(), "hashmap");
    String delegate =
        configured.equals(CanonicalStateBackendFactory.class.getName())
            ? pipeline.getString(DELEGATE, "hashmap")
            : configured;
    try {
      Map<String, BoundContract> contracts = readContracts(pipeline, classLoader);
      contracts.put(operatorId, new BoundContract(operatorClass, contract));
      pipeline.setString(DELEGATE, delegate);
      pipeline.setString(
          CONTRACTS,
          Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(contracts)));
      pipeline.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          CanonicalStateBackendFactory.class.getName());
      config.addAll(pipeline);
      if (FlinkStateBackendCompat.explicitStateBackend(environment) != null)
        FlinkStateBackendCompat.explicitStateBackend(
            environment,
            new CanonicalStateBackend(
                unwrap(FlinkStateBackendCompat.explicitStateBackend(environment)), contracts));
    } catch (Exception failure) {
      throw new IllegalStateException("Cannot configure canonical savepoint import", failure);
    }
  }

  @SuppressWarnings("unchecked")
  static Map<String, BoundContract> readContracts(Configuration config, ClassLoader loader)
      throws Exception {
    String encoded = config.getString(CONTRACTS, "");
    return encoded.isEmpty()
        ? new HashMap<>()
        : (Map<String, BoundContract>)
            InstantiationUtil.deserializeObject(Base64.getDecoder().decode(encoded), loader);
  }

  @Override
  public boolean useManagedMemory() {
    // Canonical imports and complex exports use the released disk-backed stock view.
    return true;
  }

  @Override
  public boolean supportsNoClaimRestoreMode() {
    return delegate.supportsNoClaimRestoreMode();
  }

  @Override
  public boolean supportsSavepointFormat(
      org.apache.flink.core.execution.SavepointFormatType format) {
    return delegate.supportsSavepointFormat(format);
  }

  @Override
  protected <K> CheckpointableKeyedStateBackend<K> createNativeKeyedBackend(
      KeyedBackendContext<K> context) throws Exception {
    StockStateContract contract = null;
    for (Map.Entry<String, BoundContract> entry : contracts.entrySet()) {
      if (context
          .operatorIdentifier()
          .startsWith(entry.getValue().operatorClass + "_" + entry.getKey() + "_(")) {
        contract = entry.getValue().contract;
        break;
      }
    }
    if (contract == null) return context.delegateFactory().apply(context.stateHandles());
    List<KeyedStateHandle> stockHandles = new ArrayList<>();
    for (KeyedStateHandle handle : context.stateHandles()) {
      if (handle instanceof SavepointKeyedStateHandle) stockHandles.add(handle);
    }
    if (!stockHandles.isEmpty() && stockHandles.size() != context.stateHandles().size()) {
      throw new IllegalStateException(
          "restore mixes stock canonical and native checkpoint handles");
    }
    var serializer = contract.keySerializer();
    CanonicalProjection projection =
        new CanonicalProjection(
            () -> context.stockBackendFactory().create(serializer, stockHandles),
            !stockHandles.isEmpty());
    try {
      if (projection.isRestored()) {
        Map<String, StockStateInventory.State> states = new HashMap<>();
        for (var entry : contract.stateDescriptors().entrySet())
          states.put(
              entry.getKey(),
              new StockStateInventory.State(
                  entry.getValue(), contract.namespaceSerializer(entry.getKey())));
        StockStateInventory.validate(
            projection.backend(), states, contract.timerSchema(), contract.keySerializer());
      }
      CheckpointableKeyedStateBackend<K> backend =
          context
              .delegateFactory()
              .apply(stockHandles.isEmpty() ? context.stateHandles() : List.of());
      return new CanonicalKeyedStateBackend<>(backend, projection);
    } catch (Exception | Error failure) {
      try {
        projection.close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  static final class BoundContract implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    final String operatorClass;
    final StockStateContract contract;

    BoundContract(String operatorClass, StockStateContract contract) {
      this.operatorClass = operatorClass;
      this.contract = contract;
    }
  }
}
