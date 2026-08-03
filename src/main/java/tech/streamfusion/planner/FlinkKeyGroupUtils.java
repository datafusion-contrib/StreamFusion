package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

/** Small planning-time helpers shared by the native exchange and raw keyed-state operators. */
public final class FlinkKeyGroupUtils {

  private FlinkKeyGroupUtils() {}

  /** Logical timestamp precision per projected key, with {@code -1} for every other type. */
  static int[] timestampPrecisions(RelDataType inputType, int[] keyColumns) {
    List<Integer> precisions = new ArrayList<>();
    for (int keyColumn : keyColumns) {
      appendTimestampPrecisions(
          inputType.getFieldList().get(keyColumn).getType(), precisions);
    }
    return precisions.stream().mapToInt(Integer::intValue).toArray();
  }

  private static void appendTimestampPrecisions(
      RelDataType type, List<Integer> precisions) {
    SqlTypeName typeName = type.getSqlTypeName();
    precisions.add(
        typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            ? type.getPrecision()
            : -1);
    switch (typeName) {
      case ARRAY:
        appendTimestampPrecisions(type.getComponentType(), precisions);
        break;
      case MAP:
        appendTimestampPrecisions(type.getKeyType(), precisions);
        appendTimestampPrecisions(type.getValueType(), precisions);
        break;
      case MULTISET:
        appendTimestampPrecisions(type.getComponentType(), precisions);
        precisions.add(-1); // the Arrow map's occurrence-count value is an internal INT
        break;
      case ROW:
        for (org.apache.calcite.rel.type.RelDataTypeField field : type.getFieldList()) {
          appendTimestampPrecisions(field.getType(), precisions);
        }
        break;
      default:
        break;
    }
  }

  /** The same default Flink uses for an unset keyed transformation's maximum parallelism. */
  static int defaultMaxParallelism(int parallelism) {
    return KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
  }

  /**
   * One ordinary JVM key per downstream subtask, used only to establish Flink's keyed-operator
   * context for a columnar batch. Native state itself is partitioned by every row's BinaryRow key.
   */
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

  /**
   * Establishes the Flink keyed-operator context for a native keyed transformation. Raw keyed
   * state uses the exchange's BinaryRow key groups; the selector only maps each whole columnar
   * batch to an ordinary JVM key owned by its subtask — no managed keyed state reads it.
   */
  static void applyColumnarKeying(
      OneInputTransformation<ArrowBatch, ArrowBatch> transformation, int maxParallelism) {
    transformation.setMaxParallelism(maxParallelism);
    transformation.setStateKeySelector(
        subtaskStateKeySelector(maxParallelism, transformation.getParallelism()));
    transformation.setStateKeyType(Types.INT);
    NativeManagedMemory.declareOperatorWeight(transformation);
  }

  static void applyColumnarKeying(
      TwoInputTransformation<ArrowBatch, ArrowBatch, ArrowBatch> transformation,
      int maxParallelism) {
    KeySelector<ArrowBatch, Integer> stateKeySelector =
        subtaskStateKeySelector(maxParallelism, transformation.getParallelism());
    transformation.setMaxParallelism(maxParallelism);
    transformation.setStateKeySelectors(stateKeySelector, stateKeySelector);
    transformation.setStateKeyType(Types.INT);
    NativeManagedMemory.declareOperatorWeight(transformation);
  }

  private static KeySelector<ArrowBatch, Integer> subtaskStateKeySelector(
      int maxParallelism, int parallelism) {
    int[] stateKeys = stateKeysForSubtasks(maxParallelism, parallelism);
    return batch -> stateKeys[batch.destination() >= 0 ? batch.destination() : 0];
  }
}
