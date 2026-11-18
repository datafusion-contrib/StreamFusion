package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TemporalDistinctBenchmark {
  @Test
  void temporalDistinct() throws Exception {
    for (String name : System.getProperty("distinct.types", "DATE,TIME,TIMESTAMP").split(",")) {
      var valueType = name.equals("DATE") ? DATE() : name.equals("TIME") ? TIME(0) : TIMESTAMP(9);
      var type = ROW(FIELD("k", INT()), FIELD("v", valueType));
      int keys = Integer.getInteger("basic.keys", 64);
      int cardinality = Integer.getInteger("distinct.cardinality", 128);
      BasicTypeBenchmark.run(name, "SELECT k, COUNT(DISTINCT v) FROM src GROUP BY k", false, true,
          "NativeColumnarGroupAggregate", type, i -> {
            int v = (int) (i / keys % cardinality);
            Object value = name.equals("DATE") ? LocalDate.ofEpochDay(v - 64)
                : name.equals("TIME") ? LocalTime.ofNanoOfDay(v * 1_000_000L)
                : LocalDateTime.of(1969,12,31,23,59,59).plusNanos(v);
            return Row.of((int) (i % keys), i % 7 == 0 ? null : value);
          });
    }
  }
}
