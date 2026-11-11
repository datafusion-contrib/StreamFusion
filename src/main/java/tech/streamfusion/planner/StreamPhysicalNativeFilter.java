package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.planner.common.NativePlanDetails;

/**
 * Physical node standing in for a pure filter the native operator runs. It keeps the replaced
 * node's output type (the input type — a filter does not rewrite columns) and traits, and carries
 * the encoded predicate expression the operator compiles and applies. Stateless, so it needs no
 * watermark.
 */
public class StreamPhysicalNativeFilter extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int[] projection;
  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final NativeUdf.Binding udfBinding;

  StreamPhysicalNativeFilter(Calc calc, RexExpression condition) {
    this(
        calc.getCluster(),
        calc.getTraitSet(),
        calc.getInput(),
        calc.getRowType(),
        FilterCalcMatcher.projection(calc),
        condition.kinds(),
        condition.payload(),
        condition.childCounts(),
        condition.longs(),
        condition.doubles(),
        condition.strings(),
        condition.udfBinding(),
        NativePlanDetails.from(calc).withFunctions(condition));
  }

  public StreamPhysicalNativeFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] projection,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      NativeUdf.Binding udfBinding) {
    this(
        cluster,
        traitSet,
        input,
        outputRowType,
        projection,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings,
        udfBinding,
        NativePlanDetails.EMPTY);
  }

  private StreamPhysicalNativeFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] projection,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      NativeUdf.Binding udfBinding,
      NativePlanDetails planDetails) {
    super(cluster, traitSet, input, outputRowType, planDetails);
    this.projection = projection;
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
    this.udfBinding = udfBinding;
  }

  boolean preservesCollections() {
    return CollectionValueIdentity.scalarArrayProducer(getInput());
  }

  @Override
  public boolean carriesStringValues() {
    return CollectionExpressionSemantics.stringCarrierProducer(getInput());
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  StreamPhysicalNativeFilter withProjection(RelTraitSet traits, RelDataType output, int[] columns) {
    int[] combined = java.util.Arrays.stream(columns).map(index -> projection[index]).toArray();
    RelNode input = getInput();
    int[] predicate = payload;
    if (input instanceof StreamPhysicalNativeUnnest) {
      boolean[] used = new boolean[input.getRowType().getFieldCount()];
      for (int column : combined) used[column] = true;
      for (int column : RexExpression.referencedInputs(kinds, payload)) used[column] = true;
      int[] kept =
          java.util.stream.IntStream.range(0, used.length).filter(index -> used[index]).toArray();
      if (kept.length < used.length) {
        int[] remap = new int[used.length];
        java.util.Arrays.fill(remap, -1);
        var type = getCluster().getTypeFactory().builder();
        for (int index = 0; index < kept.length; index++) {
          remap[kept[index]] = index;
          var field = input.getRowType().getFieldList().get(kept[index]);
          type.add(field.getName(), field.getType());
        }
        input =
            ((StreamPhysicalNativeUnnest) input)
                .withProjection(input.getTraitSet(), type.build(), kept);
        combined = java.util.Arrays.stream(combined).map(index -> remap[index]).toArray();
        predicate = RexExpression.remapInputPayload(kinds, payload, remap);
      }
    }
    return new StreamPhysicalNativeFilter(
        getCluster(),
        traits,
        input,
        output,
        combined,
        kinds,
        predicate,
        childCounts,
        longs,
        doubles,
        strings,
        udfBinding,
        planDetails.withProjection(input.getRowType(), output, combined));
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeFilter(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        projection,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings,
        udfBinding,
        planDetails);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFilterExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        projection,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings,
        udfBinding);
  }
}
