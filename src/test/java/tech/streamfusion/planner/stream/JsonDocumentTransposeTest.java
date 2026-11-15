package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.junit.jupiter.api.Test;

class JsonDocumentTransposeTest {
  @Test
  void jsonDocumentTransposesCannotReuseGeneralStringRepresentation() {
    var types = new JavaTypeFactoryImpl();
    var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
    var rowType = types.builder().add("s", types.createSqlType(SqlTypeName.VARCHAR)).build();
    var input = LogicalValues.createEmpty(cluster, rowType);
    var general =
        new StreamPhysicalRowDataToArrow(
            cluster,
            cluster.traitSetOf(FlinkConventions.STREAM_PHYSICAL()),
            input,
            false,
            null,
            true);
    var json = general.withJsonDocuments();
    assertFalse(general.getDigest().contains("jsonDocuments=true"));
    assertTrue(json.getDigest().contains("jsonDocuments=true"));
    assertTrue(
        json.copy(json.getTraitSet(), List.of(input)).getDigest().contains("jsonDocuments=true"));
    var cached = general.withStringCaches().withPreferredBatchRows(2048);
    assertFalse(general.getDigest().contains("stringCaches=true"));
    assertTrue(cached.getDigest().contains("stringCaches=true"));
    assertTrue(
        cached
            .copy(cached.getTraitSet(), List.of(input))
            .getDigest()
            .contains("stringCaches=true"));
    assertTrue(cached.withJsonDocuments().getDigest().contains("stringCaches=true"));
  }
}
