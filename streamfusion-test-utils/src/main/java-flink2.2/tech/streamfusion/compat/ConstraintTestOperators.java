package tech.streamfusion.compat;

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcerExecutor;
import org.apache.flink.table.runtime.operators.sink.constraint.EnforcerException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public final class ConstraintTestOperators {
  private ConstraintTestOperators() {}

  public static Class<? extends RuntimeException> notNullExceptionType() {
    return EnforcerException.class;
  }

  public static OneInputStreamOperator<RowData, RowData> notNullError(String... fieldNames) {
    LogicalType[] types =
        IntStream.range(0, fieldNames.length)
            .mapToObj(index -> new BigIntType(false))
            .toArray(LogicalType[]::new);
    ConstraintEnforcerExecutor executor =
        ConstraintEnforcerExecutor.create(
                RowType.of(types, fieldNames),
                ExecutionConfigOptions.NotNullEnforcer.ERROR,
                ExecutionConfigOptions.TypeLengthEnforcer.IGNORE,
                ExecutionConfigOptions.NestedEnforcer.IGNORE)
            .orElseThrow();
    return new ConstraintEnforcer(executor, "test-not-null-enforcer");
  }

  public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> failingOutputHarness(
      OneInputStreamOperator<IN, OUT> operator, RuntimeException failure) throws Exception {
    return failingOutputHarness(operator, failure, ignored -> {});
  }

  public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> failingOutputHarness(
      OneInputStreamOperator<IN, OUT> operator, RuntimeException failure, Consumer<OUT> consume)
      throws Exception {
    OneInputStreamOperatorTestHarness<IN, OUT> harness =
        new OneInputStreamOperatorTestHarness<>(operator);
    harness.setOutputCreator(
        ignored ->
            new MockOutput<OUT>(new ArrayList<>()) {
              @Override
              public void collect(StreamRecord<OUT> record) {
                consume.accept(record.getValue());
                throw failure;
              }
            });
    return harness;
  }
}
