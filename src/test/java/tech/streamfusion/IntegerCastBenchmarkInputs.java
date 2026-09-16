package tech.streamfusion;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/** Shared CAST fixtures, checked before timing and by the regular SQL parity suite. */
final class IntegerCastBenchmarkInputs {
  static final String PIPELINE_EXPRESSION =
      "LPAD(CAST(CAST(SUBSTR(s, 11, 2) AS INT) / 15 * 15 AS VARCHAR), 2, '0')";

  private IntegerCastBenchmarkInputs() {}

  static TableEnvironment environment(String input, long rows, int nullEvery) {
    String[] samples =
        switch (input) {
          case "cast_integer" ->
              new String[] {"0", "42", "-12345", "2147483647", "-2147483648", " +00123 ", "12.9"};
          case "cast_timestamp" ->
              new String[] {"timestamp:00", "timestamp:17", "timestamp:29", "timestamp:59"};
          default -> throw new IllegalArgumentException("Unknown integer cast fixture: " + input);
        };
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        env.fromSequence(0, rows - 1)
            .map(
                i ->
                    Row.of(
                        nullEvery > 0 && i % nullEvery == 0
                            ? null
                            : samples[(int) (i % samples.length)]))
            .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING)),
        Schema.newBuilder().column("s", DataTypes.STRING()).build());
    return tables;
  }

  static void verifyPipeline() throws Exception {
    NativeParity.assertKindedParity(
        () -> environment("cast_timestamp", 4, 0),
        "SELECT s, SUBSTR(s, 11, 2), " + PIPELINE_EXPRESSION + " FROM inputs",
        List.of(
            List.of("+I", "timestamp:00", "00", "00"),
            List.of("+I", "timestamp:17", "17", "15"),
            List.of("+I", "timestamp:29", "29", "15"),
            List.of("+I", "timestamp:59", "59", "45")));
  }
}
