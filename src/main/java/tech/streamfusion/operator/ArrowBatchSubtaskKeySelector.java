package tech.streamfusion.operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * Runtime-only keyed-context selector for a columnar batch. Each exchange record carries an exact
 * key group; this selector supplies an ordinary JVM integer key assigned to that same group so
 * Flink's keyed context and the native raw keyed-state layout agree.
 *
 * <p>This is deliberately separate from the planner utilities that construct it. Flink serializes
 * key selectors into the job graph, and deserializing a lambda whose implementation class also has
 * Calcite-typed planning methods would otherwise require Calcite on the JobManager data plane.
 */
public final class ArrowBatchSubtaskKeySelector implements KeySelector<ArrowBatch, Integer> {

  private static final long serialVersionUID = 1L;

  private final int[] stateKeysByKeyGroup;

  public ArrowBatchSubtaskKeySelector(int maxParallelism) {
    this.stateKeysByKeyGroup = stateKeysForKeyGroups(maxParallelism);
  }

  @Override
  public Integer getKey(ArrowBatch batch) {
    int keyGroup = batch.keyGroup() >= 0 ? batch.keyGroup() : 0;
    if (keyGroup >= stateKeysByKeyGroup.length) {
      throw new IllegalArgumentException(
          "Arrow batch key group "
              + keyGroup
              + " exceeds max parallelism "
              + stateKeysByKeyGroup.length);
    }
    return stateKeysByKeyGroup[keyGroup];
  }

  /** One representative JVM integer key whose hash is assigned to each exact Flink key group. */
  static int[] stateKeysForKeyGroups(int maxParallelism) {
    KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
    int[] keys = new int[maxParallelism];
    boolean[] found = new boolean[maxParallelism];
    int remaining = maxParallelism;
    for (int candidate = 0; remaining > 0; candidate++) {
      int keyGroup = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(candidate, maxParallelism);
      if (!found[keyGroup]) {
        keys[keyGroup] = candidate;
        found[keyGroup] = true;
        remaining--;
      }
    }
    return keys;
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
