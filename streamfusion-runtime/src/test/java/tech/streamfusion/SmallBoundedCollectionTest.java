package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SmallBoundedCollectionTest {
  @Test
  @Timeout(30)
  void outputLargerThanTheCollectBufferFallsBackToLiveDraining() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.runSmallBounded(
              () -> environment(metrics), "SELECT id+1,payload FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(64, runs.host().rows().size());
      assertEquals(64, runs.nativeRun().rows().size());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    byte[] payload = new byte[128 * 1024];
    var input =
        env.fromSequence(0, 63)
            .map(id -> Row.of(id, payload))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "payload"}, Types.LONG, Types.PRIMITIVE_ARRAY(Types.BYTE)));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            input,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("payload", DataTypes.BYTES())
                .build()));
    return table;
  }
}
