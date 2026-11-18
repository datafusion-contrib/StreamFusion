package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayDeque;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkListAggPartialLayoutContractTest {
  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,true,false",
    "true,false,false",
    "true,true,false",
    "false,false,true",
    "false,true,true",
    "true,false,true",
    "true,true,true"
  })
  void releasedPartialLayout(boolean updating, boolean literal, boolean distinct) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    var source =
        env.fromElements(Row.of(1L, "a"))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.STRING));
    table.createTemporaryView(
        "src",
        updating
            ? table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all())
            : table.fromDataStream(source));
    var query =
        table.sqlQuery(
            "SELECT k,LISTAGG("
                + (distinct ? "DISTINCT " : "")
                + "v"
                + (literal ? ", '::'" : "")
                + "),COUNT(*) FROM src GROUP BY k");
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(table.toChangelogStream(query).getTransformation());
    boolean found = false;
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (next instanceof OneInputTransformation<?, ?>
          && next.getName().contains("LocalGroupAggregate")) {
        var row = ((InternalTypeInfo<?>) next.getOutputType()).toRowType();
        System.out.println(
            "[list-partial-layout] updating="
                + updating
                + " literal="
                + literal
                + " distinct="
                + distinct
                + " "
                + row.asSummaryString());
        assertEquals(
            updating ? LogicalTypeRoot.STRUCTURED_TYPE : LogicalTypeRoot.VARCHAR,
            row.getTypeAt(1).getTypeRoot());
        if (updating) assertEquals(literal ? 3 : 2, row.getTypeAt(1).getChildren().size());
        found = true;
      }
      pending.addAll(next.getInputs());
    }
    assertTrue(found);
  }
}
