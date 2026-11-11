package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Transpose entering a columnar region: a rowwise input becomes Arrow batches. Inserted by the
 * transition pass where a columnar operator consumes from a rowwise one. The logical row type is
 * unchanged — only the physical carrier becomes columnar. When the edge carries a changelog, {@code
 * carryRowKind} keeps each row's {@code RowKind} as a hidden column so the native consumer sees it
 * (an insert-only edge omits it — every row is an INSERT).
 *
 * <p>When {@code prunedType} is set the transpose also prunes (nested projection pushdown): it is
 * the input type narrowed to the columns and struct sub-fields a downstream native calc reads, so
 * only those become Arrow columns — the unread fields of a wide source row never get materialized.
 * The consumer's top-level column references are remapped to the compacted positions by the
 * planner.
 */
public class StreamPhysicalRowDataToArrow extends StreamPhysicalNativeSingleRel
    implements ColumnarOutput {

  private final boolean carryRowKind;
  private final RelDataType prunedType;
  private final boolean preserveCollections;
  private final int preferredBatchRows;
  private final boolean jsonDocuments;
  private final boolean preserveStringCaches;
  private final int[] byteStringColumns;

  public StreamPhysicalRowDataToArrow(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean carryRowKind) {
    this(cluster, traitSet, input, carryRowKind, null, false);
  }

  public StreamPhysicalRowDataToArrow(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      boolean carryRowKind,
      RelDataType prunedType) {
    this(cluster, traitSet, input, carryRowKind, prunedType, false);
  }

  public StreamPhysicalRowDataToArrow(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      boolean carryRowKind,
      RelDataType prunedType,
      boolean preserveCollections) {
    this(cluster, traitSet, input, carryRowKind, prunedType, preserveCollections, 1024);
  }

  private StreamPhysicalRowDataToArrow(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      boolean carryRowKind,
      RelDataType prunedType,
      boolean preserveCollections,
      int preferredBatchRows) {
    this(cluster, traitSet, input, carryRowKind, prunedType, preserveCollections,
        preferredBatchRows, false, false);
  }

  private StreamPhysicalRowDataToArrow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      boolean carryRowKind, RelDataType prunedType, boolean preserveCollections,
      int preferredBatchRows, boolean jsonDocuments, boolean preserveStringCaches) {
    this(cluster, traitSet, input, carryRowKind, prunedType, preserveCollections,
        preferredBatchRows, jsonDocuments, preserveStringCaches, new int[0]);
  }

  private StreamPhysicalRowDataToArrow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      boolean carryRowKind, RelDataType prunedType, boolean preserveCollections,
      int preferredBatchRows, boolean jsonDocuments, boolean preserveStringCaches,
      int[] byteStringColumns) {
    super(cluster, traitSet, input);
    this.preferredBatchRows = preferredBatchRows;
    this.carryRowKind = carryRowKind;
    this.prunedType = prunedType;
    this.preserveCollections = preserveCollections;
    this.jsonDocuments = jsonDocuments;
    this.preserveStringCaches = preserveStringCaches;
    this.byteStringColumns = byteStringColumns.clone();
  }

  StreamPhysicalRowDataToArrow withByteStringColumns(int[] columns) {
    return new StreamPhysicalRowDataToArrow(getCluster(), getTraitSet(), getInput(), carryRowKind,
        prunedType, preserveCollections, preferredBatchRows, jsonDocuments, preserveStringCaches,
        columns);
  }

  StreamPhysicalRowDataToArrow withJsonDocuments() {
    return jsonDocuments ? this : new StreamPhysicalRowDataToArrow(getCluster(), getTraitSet(),
        getInput(), carryRowKind, prunedType, preserveCollections, preferredBatchRows, true,
        preserveStringCaches, byteStringColumns);
  }

  StreamPhysicalRowDataToArrow withStringCaches() {
    return preserveStringCaches ? this : new StreamPhysicalRowDataToArrow(getCluster(), getTraitSet(),
        getInput(), carryRowKind, prunedType, preserveCollections, preferredBatchRows, jsonDocuments,
        true, byteStringColumns);
  }

  StreamPhysicalRowDataToArrow withPreferredBatchRows(int rows) {
    return rows == preferredBatchRows
        ? this
        : new StreamPhysicalRowDataToArrow(
            getCluster(),
            getTraitSet(),
            getInput(),
            carryRowKind,
            prunedType,
            preserveCollections,
            rows,
            jsonDocuments,
            preserveStringCaches,
            byteStringColumns);
  }

  StreamPhysicalRowDataToArrow withCollectionRepresentation() {
    return preserveCollections
        ? this
        : new StreamPhysicalRowDataToArrow(
            getCluster(),
            getTraitSet(),
            getInput(),
            carryRowKind,
            prunedType,
            true,
            preferredBatchRows,
            jsonDocuments,
            preserveStringCaches,
            byteStringColumns);
  }

  boolean preservesCollections() {
    return preserveCollections;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return prunedType != null ? prunedType : getInput().getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalRowDataToArrow(
        getCluster(),
        traitSet,
        inputs.get(0),
        carryRowKind,
        prunedType,
        preserveCollections,
        preferredBatchRows,
        jsonDocuments,
        preserveStringCaches,
        byteStringColumns);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new RowDataToArrowExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        carryRowKind,
        prunedType == null
            ? null
            : FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        preserveCollections,
        preferredBatchRows,
        jsonDocuments,
        preserveStringCaches).withByteStringColumns(byteStringColumns);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    // The rendered field count makes pruning visible in an explain ("why is my transpose wide?");
    // display-only — the digest already carries the full row type.
    return super.explainTerms(pw)
        .itemIf("collectionRepresentation", true, preserveCollections)
        .itemIf("jsonDocuments", true, jsonDocuments)
        .itemIf("stringCaches", true, preserveStringCaches)
        .itemIf("byteStrings", java.util.Arrays.toString(byteStringColumns), byteStringColumns.length > 0)
        .itemIf("preferredBatchRows", preferredBatchRows, preferredBatchRows != 1024)
        .itemIf(
            "fields",
            getRowType().getFieldCount(),
            pw.getDetailLevel() != org.apache.calcite.sql.SqlExplainLevel.DIGEST_ATTRIBUTES);
  }
}
