package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class CollectionValueIdentityTest {
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
      assertTrue(CollectionValueIdentity.unverifiedInput(consumer(ordinary)));
      assertFalse(CollectionValueIdentity.unverifiedInput(consumer(represented)));
      var encoded =
          RexExpression.encodeProjections(
              List.of(RexInputRef.of(0, source.getRowType())), List.of("s"));
      assertNotNull(encoded);
      var ordinaryCalc =
          new StreamPhysicalNativeCalc(
              source.getCluster(), source.getTraitSet(), represented, source.getRowType(), encoded);
      assertTrue(CollectionValueIdentity.unverifiedInput(consumer(ordinaryCalc)));
    }
  }

  @Test
  void aNullabilityOnlyCollectionCastPassesItsColumnThrough() {
    // Flink casts a NOT NULL connector map into a nullable declared column; on 2.2 it runs that
    // cast as the identity, so the value needs no represented producer. Flink 1.18 rebuilds the
    // map into a fresh HashMap, reordering it, so there the cast keeps its checks, as a narrowing
    // cast or one that changes the key type does on every line.
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      var types = new JavaTypeFactoryImpl();
      var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
      var map =
          types.createMapType(
              types.createTypeWithNullability(types.createSqlType(SqlTypeName.VARCHAR, 20), true),
              types.createTypeWithNullability(
                  types.createSqlType(SqlTypeName.VARBINARY, 20), true));
      RelNode source = LogicalValues.createEmpty(cluster, types.builder().add("m", map).build());
      var producer =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, false);
      RelNode nullableSource =
          LogicalValues.createEmpty(
              cluster,
              types.builder().add("m", types.createTypeWithNullability(map, true)).build());
      var nullableProducer =
          new StreamPhysicalRowDataToArrow(
              nullableSource.getCluster(), nullableSource.getTraitSet(), nullableSource, false);
      var rex = cluster.getRexBuilder();
      var widened =
          rex.makeAbstractCast(
              types.createTypeWithNullability(map, true),
              RexInputRef.of(0, producer.getRowType()));
      var narrowed =
          rex.makeAbstractCast(map, RexInputRef.of(0, nullableProducer.getRowType()));
      var rekeyed =
          rex.makeAbstractCast(
              types.createTypeWithNullability(
                  types.createMapType(
                      types.createTypeWithNullability(
                          types.createSqlType(SqlTypeName.VARCHAR, 10), true),
                      types.createTypeWithNullability(
                          types.createSqlType(SqlTypeName.VARBINARY, 20), true)),
                  true),
              RexInputRef.of(0, producer.getRowType()));

      var widenedCalc =
          LogicalCalc.create(
              producer,
              RexProgram.create(
                  producer.getRowType(),
                  List.of(widened),
                  null,
                  types.builder().add("m", widened.getType()).build(),
                  rex));
      var narrowedCalc =
          LogicalCalc.create(
              nullableProducer,
              RexProgram.create(
                  nullableProducer.getRowType(),
                  List.of(narrowed),
                  null,
                  types.builder().add("m", narrowed.getType()).build(),
                  rex));
      var rekeyedCalc =
          LogicalCalc.create(
              producer,
              RexProgram.create(
                  producer.getRowType(),
                  List.of(rekeyed),
                  null,
                  types.builder().add("m", rekeyed.getType()).build(),
                  rex));

      assertEquals(
          org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18"),
          CollectionValueIdentity.unverifiedInput(widenedCalc));
      assertTrue(CollectionValueIdentity.unverifiedInput(narrowedCalc));
      assertTrue(CollectionValueIdentity.unverifiedInput(rekeyedCalc));
    }
  }

  @Test
  void anIdentityCollectionCastEncodesAsTheColumnTheGateTreatsItAs() {
    // The gate counts a cast Flink runs as the identity as the column it reads, so the encoder must
    // emit that column too, even for a map or array the native collection casts convert. Flink 2.2
    // runs a nullability-only widening as the identity; 1.18 resolves its container cast rules
    // first, so there the cast keeps its checks and its own encoding.
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      boolean identity =
          !org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18");
      var types = new JavaTypeFactoryImpl();
      var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(types));
      var rex = cluster.getRexBuilder();
      var map =
          types.createMapType(
              types.createTypeWithNullability(types.createSqlType(SqlTypeName.VARCHAR, 20), true),
              types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true));
      var array =
          types.createArrayType(
              types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true), -1);
      RelNode source = LogicalValues.createEmpty(cluster, types.builder().add("m", map).build());
      var producer =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, false);
      var mapColumn = RexInputRef.of(0, producer.getRowType());
      var widenedMap = rex.makeAbstractCast(types.createTypeWithNullability(map, true), mapColumn);
      var arrayColumn = rex.makeInputRef(array, 0);
      var widenedArray =
          rex.makeAbstractCast(types.createTypeWithNullability(array, true), arrayColumn);
      var widenedMapCalc =
          LogicalCalc.create(
              producer,
              RexProgram.create(
                  producer.getRowType(),
                  List.of(widenedMap),
                  null,
                  types.builder().add("m", widenedMap.getType()).build(),
                  rex));

      var mapColumnEncoding = RexExpression.encodeProjections(List.of(mapColumn), List.of("m"));
      var widenedMapEncoding = RexExpression.encodeProjections(List.of(widenedMap), List.of("m"));
      var arrayColumnEncoding = RexExpression.encodeProjections(List.of(arrayColumn), List.of("a"));
      var widenedArrayEncoding =
          RexExpression.encodeProjections(List.of(widenedArray), List.of("a"));

      assertEquals(!identity, CollectionValueIdentity.unverifiedInput(widenedMapCalc));
      assertNotNull(mapColumnEncoding);
      assertEquals(
          identity,
          widenedMapEncoding != null
              && Arrays.equals(mapColumnEncoding.kinds(), widenedMapEncoding.kinds())
              && Arrays.equals(mapColumnEncoding.payload(), widenedMapEncoding.payload()));
      assertNotNull(arrayColumnEncoding);
      assertEquals(
          identity,
          widenedArrayEncoding != null
              && Arrays.equals(arrayColumnEncoding.kinds(), widenedArrayEncoding.kinds())
              && Arrays.equals(arrayColumnEncoding.payload(), widenedArrayEncoding.payload()));
    }
  }

  @Test
  void aMapFunctionReadsAnIdentityCollectionCastLikeItsColumn() {
    // A map function that keeps Flink's collection representation copies a map column before the
    // call, as Flink's generated field access does; reading the column through a cast Flink runs
    // as the identity must keep that copy. Flink 1.18 runs no collection cast as the identity.
    var config = new Configuration();
    config.setString("streamfusion.experimental.collectionExpressions", "true");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      boolean identity =
          !org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18");
      var types = new JavaTypeFactoryImpl();
      var rex = new RexBuilder(types);
      var key = types.createTypeWithNullability(types.createSqlType(SqlTypeName.VARCHAR, 20), true);
      var map =
          types.createMapType(
              key, types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), true));
      var nullableMap = types.createTypeWithNullability(map, true);
      var keys = types.createTypeWithNullability(types.createArrayType(key, -1), true);
      var mapKeys =
          new SqlFunction(
              "MAP_KEYS", SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.SYSTEM);
      var mapUnion =
          new SqlFunction(
              "MAP_UNION", SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.SYSTEM);
      var column = rex.makeInputRef(map, 0);
      var other = rex.makeInputRef(nullableMap, 1);
      var widened = rex.makeAbstractCast(nullableMap, column);

      var keysOfColumn =
          RexExpression.encodeProjections(
              List.of(rex.makeCall(keys, mapKeys, List.of(column))), List.of("k"));
      var keysOfCast =
          RexExpression.encodeProjections(
              List.of(rex.makeCall(keys, mapKeys, List.of(widened))), List.of("k"));
      var unionOfColumn =
          RexExpression.encodeProjections(
              List.of(rex.makeCall(nullableMap, mapUnion, List.of(column, other))), List.of("u"));
      var unionOfCast =
          RexExpression.encodeProjections(
              List.of(rex.makeCall(nullableMap, mapUnion, List.of(widened, other))), List.of("u"));

      assertNotNull(keysOfColumn);
      assertEquals(
          identity,
          keysOfCast != null
              && Arrays.equals(keysOfColumn.kinds(), keysOfCast.kinds())
              && Arrays.equals(keysOfColumn.payload(), keysOfCast.payload()));
      assertNotNull(unionOfColumn);
      assertEquals(
          identity,
          unionOfCast != null
              && Arrays.equals(unionOfColumn.kinds(), unionOfCast.kinds())
              && Arrays.equals(unionOfColumn.payload(), unionOfCast.payload()));
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
}
