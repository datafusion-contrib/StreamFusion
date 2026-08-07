package tech.streamfusion.operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * Runtime-only keyed-context selector for a columnar batch.
 *
 * <p>This is deliberately separate from the planner utilities that construct it. Flink serializes
 * key selectors into the job graph, and deserializing a lambda whose implementation class also has
 * Calcite-typed planning methods would otherwise require Calcite on the JobManager data plane.
 */
public final class ArrowBatchSubtaskKeySelector implements KeySelector<ArrowBatch, Integer> {

  private static final long serialVersionUID = 1L;

  private final int[] stateKeys;

  public ArrowBatchSubtaskKeySelector(int maxParallelism, int parallelism) {
    this.stateKeys = stateKeysForSubtasks(maxParallelism, parallelism);
  }

  @Override
  public Integer getKey(ArrowBatch batch) {
    return stateKeys[batch.destination() >= 0 ? batch.destination() : 0];
  }

  /** One representative JVM key owned by each downstream subtask. */
  public static int[] stateKeysForSubtasks(int maxParallelism, int parallelism) {
    int[] keys = new int[parallelism];
    boolean[] found = new boolean[parallelism];
    int remaining = parallelism;
    for (int candidate = 0; remaining > 0; candidate++) {
      int keyGroup = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(candidate, maxParallelism);
      int subtask =
          KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
              maxParallelism, parallelism, keyGroup);
      if (!found[subtask]) {
        keys[subtask] = candidate;
        found[subtask] = true;
        remaining--;
      }
    }
    return keys;
  }
}
