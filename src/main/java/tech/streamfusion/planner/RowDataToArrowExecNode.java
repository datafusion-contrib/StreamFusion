package tech.streamfusion.planner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.planner.common.CommonRowDataToArrowExecNode;

/** Streaming mode marker for the shared row-to-Arrow implementation. */
public class RowDataToArrowExecNode extends CommonRowDataToArrowExecNode
    implements StreamExecNode<ArrowBatch> {
  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType) {
    super(tableConfig, inputProperty, rowType, description, carryRowKind, sourceType);
  }

  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType,
      boolean preserveCollections) {
    super(
        tableConfig,
        inputProperty,
        rowType,
        description,
        carryRowKind,
        sourceType,
        preserveCollections);
  }

  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType,
      boolean preserveCollections,
      int preferredBatchRows) {
    super(
        tableConfig,
        inputProperty,
        rowType,
        description,
        carryRowKind,
        sourceType,
        preserveCollections,
        preferredBatchRows);
  }

  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType,
      boolean preserveCollections,
      int preferredBatchRows,
      boolean jsonDocuments,
      boolean preserveStringCaches) {
    super(
        tableConfig,
        inputProperty,
        rowType,
        description,
        carryRowKind,
        sourceType,
        preserveCollections,
        preferredBatchRows,
        jsonDocuments,
        preserveStringCaches);
  }

  @Override
  public RowDataToArrowExecNode withBinaryRowKeys() {
    super.withBinaryRowKeys();
    return this;
  }

  @Override
  public RowDataToArrowExecNode withByteStringColumns(int[] columns) {
    super.withByteStringColumns(columns);
    return this;
  }
}
