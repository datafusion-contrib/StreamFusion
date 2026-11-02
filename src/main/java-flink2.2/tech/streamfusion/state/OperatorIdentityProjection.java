package tech.streamfusion.state;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.StringUtils;

/** Projects complete canonical metadata at the storage boundary; state payloads stay untouched. */
public final class OperatorIdentityProjection implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Map<OperatorID, OperatorID> exportIds;
  private final Map<OperatorID, OperatorID> importIds;
  private final Set<OperatorID> emptyNativeHelpers;
  private final Map<OperatorID, EmptyHelper> helperInputs;

  public OperatorIdentityProjection(Map<String, String> nativeToStock) {
    this(nativeToStock, Set.of(), Map.of());
  }

  public OperatorIdentityProjection(Map<String, String> nativeToStock, Set<String> emptyHelpers) {
    this(nativeToStock, emptyHelpers, Map.of());
  }

  public OperatorIdentityProjection(
      Map<String, String> nativeToStock, Map<String, EmptyHelper> helpers) {
    this(nativeToStock, helpers.keySet(), helpers);
  }

  private OperatorIdentityProjection(
      Map<String, String> nativeToStock,
      Set<String> emptyHelpers,
      Map<String, EmptyHelper> inputs) {
    Map<OperatorID, OperatorID> exports = new HashMap<>();
    Map<OperatorID, OperatorID> imports = new HashMap<>();
    nativeToStock.forEach(
        (nativeId, stockId) -> {
          OperatorID source = new OperatorID(StringUtils.hexStringToByte(nativeId));
          OperatorID target = new OperatorID(StringUtils.hexStringToByte(stockId));
          if (imports.put(target, source) != null) {
            throw new IllegalArgumentException(
                "multiple native operators map to stock operator " + stockId);
          }
          exports.put(source, target);
        });
    exportIds = Map.copyOf(exports);
    importIds = Map.copyOf(imports);
    var helpers = new HashSet<OperatorID>();
    for (String helper : emptyHelpers) {
      var id = new OperatorID(StringUtils.hexStringToByte(helper));
      if (exportIds.containsKey(id))
        throw new IllegalArgumentException("Logical operator cannot be omitted: " + id);
      helpers.add(id);
    }
    emptyNativeHelpers = Set.copyOf(helpers);
    Map<OperatorID, EmptyHelper> parsed = new HashMap<>();
    inputs.forEach(
        (id, input) -> parsed.put(new OperatorID(StringUtils.hexStringToByte(id)), input));
    helperInputs = Map.copyOf(parsed);
  }

  public static final class EmptyHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<OperatorID> inputs;
    private final int parallelism;
    private final int maxParallelism;
    private final boolean forwardsSubtasks;

    public EmptyHelper(
        String input, int parallelism, int maxParallelism, boolean forwardsSubtasks) {
      this(List.of(input), parallelism, maxParallelism, forwardsSubtasks);
    }

    public EmptyHelper(
        List<String> inputs, int parallelism, int maxParallelism, boolean forwardsSubtasks) {
      if (parallelism < 1) throw new IllegalArgumentException("Missing helper parallelism");
      if (inputs.isEmpty()) throw new IllegalArgumentException("Missing helper inputs");
      this.inputs =
          inputs.stream()
              .map(input -> new OperatorID(StringUtils.hexStringToByte(input)))
              .collect(java.util.stream.Collectors.toList());
      this.parallelism = parallelism;
      this.maxParallelism = maxParallelism;
      this.forwardsSubtasks = forwardsSubtasks;
    }
  }

  public CheckpointMetadata exportMetadata(CheckpointMetadata metadata) throws IOException {
    return project(
        metadata, exportIds, emptyNativeHelpers == null ? Set.of() : emptyNativeHelpers, false);
  }

  public CheckpointMetadata importMetadata(CheckpointMetadata metadata) throws IOException {
    return project(metadata, importIds, Set.of(), true);
  }

  private CheckpointMetadata project(
      CheckpointMetadata metadata,
      Map<OperatorID, OperatorID> ids,
      Set<OperatorID> emptyHelpers,
      boolean restoring)
      throws IOException {
    var properties = metadata.getCheckpointProperties();
    if (properties == null
        || !(properties.getCheckpointType() instanceof SavepointType)
        || ((SavepointType) properties.getCheckpointType()).getFormatType()
            != SavepointFormatType.CANONICAL) {
      return metadata;
    }
    var operators = new ArrayList<OperatorState>();
    var emitted = new HashSet<OperatorID>();
    for (OperatorState original : metadata.getOperatorStates()) {
      OperatorID target = ids.get(original.getOperatorID());
      if (target == null) {
        boolean durable =
            original.getCoordinatorState() != null
                || original.getStates().stream().anyMatch(state -> state.hasState());
        boolean finished =
            original.isFullyFinished()
                || original.getStates().stream().anyMatch(state -> state.isFinished());
        if (durable || finished && !emptyHelpers.contains(original.getOperatorID())) {
          throw new IOException(
              "Canonical savepoint has state without an operator identity mapping: "
                  + original.getOperatorID());
        }
        continue;
      }
      if (!emitted.add(target))
        throw new IOException("Duplicate projected operator identity: " + target);
      OperatorState copy;
      if (original.isFullyFinished()) {
        copy =
            new FullyFinishedOperatorState(
                original.getOperatorName().orElse(null),
                original.getOperatorUid().orElse(null),
                target,
                original.getParallelism(),
                original.getMaxParallelism());
      } else {
        copy =
            new OperatorState(
                original.getOperatorName().orElse(null),
                original.getOperatorUid().orElse(null),
                target,
                original.getParallelism(),
                original.getMaxParallelism());
        original.getSubtaskStates().forEach(copy::putState);
        // Flink 2.2's copyWithNewIDs omits coordinator state.
        copy.setCoordinatorState(original.getCoordinatorState());
      }
      operators.add(copy);
    }
    if (restoring) restoreHelperFinishedState(operators);
    return new CheckpointMetadata(
        metadata.getCheckpointId(),
        operators,
        metadata.getMasterStates(),
        metadata.getCheckpointProperties());
  }

  private void restoreHelperFinishedState(ArrayList<OperatorState> operators) {
    if (helperInputs == null || helperInputs.isEmpty()) return;
    Map<OperatorID, OperatorState> available = new HashMap<>();
    for (OperatorState operator : operators) available.put(operator.getOperatorID(), operator);
    boolean progress;
    do {
      progress = false;
      for (var binding : helperInputs.entrySet()) {
        if (available.containsKey(binding.getKey())) continue;
        EmptyHelper helper = binding.getValue();
        List<OperatorState> inputs =
            helper.inputs.stream()
                .map(available::get)
                .collect(java.util.stream.Collectors.toList());
        if (inputs.stream().anyMatch(java.util.Objects::isNull)) continue;
        boolean fullyFinished = inputs.stream().allMatch(OperatorState::isFullyFinished);
        boolean partiallyFinished =
            !fullyFinished
                && helper.forwardsSubtasks
                && inputs.stream().allMatch(input -> input.getParallelism() == helper.parallelism)
                && java.util.stream.IntStream.range(0, helper.parallelism)
                    .anyMatch(
                        subtask -> inputs.stream().allMatch(input -> finished(input, subtask)));
        if (!fullyFinished && !partiallyFinished) continue;
        int max =
            helper.maxParallelism > 0 ? helper.maxParallelism : inputs.get(0).getMaxParallelism();
        if (max < helper.parallelism)
          max =
              org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeDefaultMaxParallelism(
                  helper.parallelism);
        OperatorState restored =
            fullyFinished
                ? new FullyFinishedOperatorState(
                    "native boundary", null, binding.getKey(), helper.parallelism, max)
                : new OperatorState(
                    "native boundary", null, binding.getKey(), helper.parallelism, max);
        if (partiallyFinished) {
          for (int subtask = 0; subtask < helper.parallelism; subtask++) {
            final int index = subtask;
            restored.putState(
                subtask,
                inputs.stream().allMatch(input -> finished(input, index))
                    ? FinishedOperatorSubtaskState.INSTANCE
                    : OperatorSubtaskState.builder().build());
          }
        }
        operators.add(restored);
        available.put(binding.getKey(), restored);
        progress = true;
      }
    } while (progress);
  }

  private static boolean finished(OperatorState input, int subtask) {
    return input.isFullyFinished()
        || input.getState(subtask) != null && input.getState(subtask).isFinished();
  }
}
