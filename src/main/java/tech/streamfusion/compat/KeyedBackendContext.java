package tech.streamfusion.compat;

import java.util.Collection;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.util.function.FunctionWithException;

/** Host construction parameters plus a factory that preserves all remaining delegate arguments. */
public final class KeyedBackendContext<K> {
  private final Environment environment;
  private final JobID jobId;
  private final String operatorIdentifier;
  private final TypeSerializer<K> keySerializer;
  private final int numberOfKeyGroups;
  private final KeyGroupRange keyGroupRange;
  private final Collection<KeyedStateHandle> stateHandles;
  private final double managedMemoryFraction;
  private final FunctionWithException<
          Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
      delegateFactory;
  private final FunctionWithException<
          Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
      canonicalProjectionFactory;

  public KeyedBackendContext(
      Environment environment,
      JobID jobId,
      String operatorIdentifier,
      TypeSerializer<K> keySerializer,
      int numberOfKeyGroups,
      KeyGroupRange keyGroupRange,
      Collection<KeyedStateHandle> stateHandles,
      double managedMemoryFraction,
      FunctionWithException<
              Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
          delegateFactory,
      FunctionWithException<
              Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
          canonicalProjectionFactory) {
    this.environment = environment;
    this.jobId = jobId;
    this.operatorIdentifier = operatorIdentifier;
    this.keySerializer = keySerializer;
    this.numberOfKeyGroups = numberOfKeyGroups;
    this.keyGroupRange = keyGroupRange;
    this.stateHandles = stateHandles;
    this.managedMemoryFraction = managedMemoryFraction;
    this.delegateFactory = delegateFactory;
    this.canonicalProjectionFactory = canonicalProjectionFactory;
  }

  public Environment environment() {
    return environment;
  }

  public JobID jobId() {
    return jobId;
  }

  public String operatorIdentifier() {
    return operatorIdentifier;
  }

  public TypeSerializer<K> keySerializer() {
    return keySerializer;
  }

  public int numberOfKeyGroups() {
    return numberOfKeyGroups;
  }

  public KeyGroupRange keyGroupRange() {
    return keyGroupRange;
  }

  public Collection<KeyedStateHandle> stateHandles() {
    return stateHandles;
  }

  public double managedMemoryFraction() {
    return managedMemoryFraction;
  }

  public FunctionWithException<
          Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
      delegateFactory() {
    return delegateFactory;
  }

  public FunctionWithException<
          Collection<KeyedStateHandle>, CheckpointableKeyedStateBackend<K>, Exception>
      canonicalProjectionFactory() {
    return canonicalProjectionFactory;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    KeyedBackendContext<?> that = (KeyedBackendContext<?>) other;
    return java.util.Objects.equals(environment, that.environment)
        && java.util.Objects.equals(jobId, that.jobId)
        && java.util.Objects.equals(operatorIdentifier, that.operatorIdentifier)
        && java.util.Objects.equals(keySerializer, that.keySerializer)
        && numberOfKeyGroups == that.numberOfKeyGroups
        && java.util.Objects.equals(keyGroupRange, that.keyGroupRange)
        && java.util.Objects.equals(stateHandles, that.stateHandles)
        && Double.compare(managedMemoryFraction, that.managedMemoryFraction) == 0
        && java.util.Objects.equals(delegateFactory, that.delegateFactory)
        && java.util.Objects.equals(canonicalProjectionFactory, that.canonicalProjectionFactory);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + java.util.Objects.hashCode(environment);
    result = 31 * result + java.util.Objects.hashCode(jobId);
    result = 31 * result + java.util.Objects.hashCode(operatorIdentifier);
    result = 31 * result + java.util.Objects.hashCode(keySerializer);
    result = 31 * result + numberOfKeyGroups;
    result = 31 * result + java.util.Objects.hashCode(keyGroupRange);
    result = 31 * result + java.util.Objects.hashCode(stateHandles);
    result = 31 * result + Double.hashCode(managedMemoryFraction);
    result = 31 * result + java.util.Objects.hashCode(delegateFactory);
    result = 31 * result + java.util.Objects.hashCode(canonicalProjectionFactory);
    return result;
  }

  @Override
  public String toString() {
    return "KeyedBackendContext[environment="
        + environment
        + ", jobId="
        + jobId
        + ", operatorIdentifier="
        + operatorIdentifier
        + ", keySerializer="
        + keySerializer
        + ", numberOfKeyGroups="
        + numberOfKeyGroups
        + ", keyGroupRange="
        + keyGroupRange
        + ", stateHandles="
        + stateHandles
        + ", managedMemoryFraction="
        + managedMemoryFraction
        + ", delegateFactory="
        + delegateFactory
        + ", canonicalProjectionFactory="
        + canonicalProjectionFactory
        + "]";
  }
}
