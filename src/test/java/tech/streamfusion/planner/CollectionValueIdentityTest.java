package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.common.ExpressionInput;

class CollectionValueIdentityTest {
  @Test
  void sharedAdmissionUsesProducerPropertiesAndRetainsUnknownColumnarFallback() {
    var types = new JavaTypeFactoryImpl();
    var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
    RelNode source =
        LogicalValues.createEmpty(
            cluster,
            types.builder().add("s", types.createSqlType(SqlTypeName.VARCHAR, 20)).build());
    var unknown = new ColumnarProducer(source);
    var represented = new RepresentedProducer(source);
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      assertTrue(CollectionExpressionSemantics.unverifiedInput(consumer(unknown)));
      assertFalse(CollectionExpressionSemantics.unverifiedInput(consumer(represented)));
    }
    assertFalse(CollectionExpressionSemantics.stringCarrierProducer(unknown));
    assertTrue(CollectionExpressionSemantics.stringCarrierProducer(represented));
    assertFalse(JvmFunctionConversions.aliasedProducer(unknown));
    assertTrue(JvmFunctionConversions.aliasedProducer(consumer(represented)));
  }

  @Test
  void sensitiveConsumersRequireAnExplicitlyRepresentedColumnarProducer() {
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      var types = new JavaTypeFactoryImpl();
      var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
      RelNode source =
          LogicalValues.createEmpty(
              cluster,
              types.builder().add("s", types.createSqlType(SqlTypeName.VARCHAR, 20)).build());
      var ordinary =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, false);
      var represented =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, false, null, true);
      assertTrue(CollectionExpressionSemantics.unverifiedInput(consumer(ordinary)));
      assertFalse(CollectionExpressionSemantics.unverifiedInput(consumer(represented)));
      var encoded =
          RexExpression.encodeProjections(
              List.of(RexInputRef.of(0, source.getRowType())), List.of("s"));
      assertNotNull(encoded);
      var ordinaryCalc =
          new StreamPhysicalNativeCalc(
              source.getCluster(), source.getTraitSet(), represented, source.getRowType(), encoded);
      assertTrue(CollectionExpressionSemantics.unverifiedInput(consumer(ordinaryCalc)));
    }
  }

  private static LogicalCalc consumer(RelNode input) {
    var rex = input.getCluster().getRexBuilder();
    var array =
        rex.makeCall(
            SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, RexInputRef.of(0, input.getRowType()));
    var output = rex.getTypeFactory().builder().add("a", array.getType()).build();
    return LogicalCalc.create(
        input, RexProgram.create(input.getRowType(), List.of(array), null, output, rex));
  }

  private static class ColumnarProducer extends SingleRel implements ColumnarOutput {
    ColumnarProducer(RelNode input) {
      super(input.getCluster(), input.getTraitSet(), input);
    }
  }

  private static final class RepresentedProducer extends ColumnarProducer
      implements ExpressionInput {
    RepresentedProducer(RelNode input) {
      super(input);
    }

    @Override
    public boolean unverifiedCollectionInput() {
      return false;
    }

    @Override
    public boolean carriesStringValues() {
      return true;
    }

    @Override
    public boolean aliasesFunctionResults() {
      return true;
    }
  }
}
