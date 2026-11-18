package tech.streamfusion.compat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer;

public final class ConstraintTestOperators {
  private ConstraintTestOperators() {}

  public static Class<? extends RuntimeException> notNullExceptionType() {
    return TableException.class;
  }

  public static OneInputStreamOperator<RowData, RowData> notNullError(String... fieldNames) {
    ConstraintEnforcer.Builder builder = ConstraintEnforcer.newBuilder();
    builder.addNotNullConstraint(
        ExecutionConfigOptions.NotNullEnforcer.ERROR,
        IntStream.range(0, fieldNames.length).toArray(),
        Arrays.asList(fieldNames),
        fieldNames);
    return builder.build();
  }

  public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> failingOutputHarness(
      OneInputStreamOperator<IN, OUT> operator, RuntimeException failure) throws Exception {
    return failingOutputHarness(operator, failure, ignored -> {});
  }

  public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> failingOutputHarness(
      OneInputStreamOperator<IN, OUT> operator, RuntimeException failure, Consumer<OUT> consume)
      throws Exception {
    return new OneInputStreamOperatorTestHarness<IN, OUT>(operator) {
      @Override
      public void setup(TypeSerializer<OUT> serializer) {
        super.setup(serializer);
        getOperator()
            .setup(
                mockTask,
                getStreamConfig(),
                new MockOutput<OUT>(new ArrayList<>()) {
                  @Override
                  public void collect(StreamRecord<OUT> record) {
                    consume.accept(record.getValue());
                    throw failure;
                  }
                });
      }
    };
  }
}
