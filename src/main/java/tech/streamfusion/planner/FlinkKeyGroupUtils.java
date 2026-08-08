package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSubtaskKeySelector;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

  /** Honors Flink's program-wide key-group count, falling back to Flink's normal default. */
  static int maxParallelism(StreamExecutionEnvironment env, int parallelism) {
    int configured = env.getConfig().getMaxParallelism();
    return configured > 0 ? configured : defaultMaxParallelism(parallelism);
  }

  /**
   * One ordinary JVM key per downstream subtask, used only to establish Flink's keyed-operator
   * context for tests that construct destination-subtask-tagged batches directly. Native exchange
   * records use an exact key-group selector instead.
   */
  public static int[] stateKeysForSubtasks(int maxParallelism, int parallelism) {
    return ArrowBatchSubtaskKeySelector.stateKeysForSubtasks(maxParallelism, parallelism);
  }

  /**
   * Establishes the Flink keyed-operator context for a native keyed transformation. Raw keyed
   * state uses the exchange's BinaryRow key groups; the selector maps each whole, single-key-group
   * columnar batch to an ordinary JVM key in that exact group — no managed keyed state reads it.
   */
  static void applyColumnarKeying(
      OneInputTransformation<ArrowBatch, ArrowBatch> transformation, int maxParallelism) {
    transformation.setMaxParallelism(maxParallelism);
    transformation.setStateKeySelector(
        subtaskStateKeySelector(maxParallelism));
    transformation.setStateKeyType(Types.INT);
  }

  static void applyColumnarKeying(
      TwoInputTransformation<ArrowBatch, ArrowBatch, ArrowBatch> transformation,
      int maxParallelism) {
    KeySelector<ArrowBatch, Integer> stateKeySelector =
        subtaskStateKeySelector(maxParallelism);
    transformation.setMaxParallelism(maxParallelism);
    transformation.setStateKeySelectors(stateKeySelector, stateKeySelector);
    transformation.setStateKeyType(Types.INT);
  }

  private static KeySelector<ArrowBatch, Integer> subtaskStateKeySelector(int maxParallelism) {
    return new ArrowBatchSubtaskKeySelector(maxParallelism);
  }
}
