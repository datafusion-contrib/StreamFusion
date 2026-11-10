package tech.streamfusion.planner;

import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.NestedRepresentation;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarShareOperator;
import tech.streamfusion.operator.RowDataToArrowOperator;
import tech.streamfusion.operator.SynchronousArrowInput;

/** Wraps the row→Arrow transpose operator into the plan, producing the columnar batch stream. */
public class RowDataToArrowExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "row-to-arrow";

  private final int batchRows;

  private final RowType rowType;
  private final boolean carryRowKind;
  private final RowType sourceType;
  private final byte[] arrowSchema;
  private final boolean preserveStringCaches;
  private boolean binaryRowKeys;
  private FileSourceGroup fileSource;

  boolean acceptsNativeFileSource() {
    return sourceType == null && !carryRowKind && !preserveStringCaches && !binaryRowKeys;
  }

  void allowNativeFileSource(FileSourceGroup group) {
    fileSource = group;
  }

  static final class FileSourceGroup {
    private final BatchExecTableSourceScan scan;
    private final int consumers;
    private boolean attempted;
    private Transformation<ArrowBatch> converted;

    FileSourceGroup(BatchExecTableSourceScan scan, int consumers) {
      this.scan = scan;
      this.consumers = consumers;
    }

    Transformation<ArrowBatch> translate(
        PlannerBase planner, Transformation<RowData> input, RowType output, int rows) {
      if (attempted) return converted;
      attempted = true;
      for (var extension :
          NativePlannerExtensions.load(planner.getFlinkContext().getClassLoader()).extensions()) {
        converted =
            extension.lowerBatchFileSource(
                scan, input, output, rows, planner.getTableConfig().getConfiguration());
        if (converted == null) continue;
        if (consumers > 1)
          converted =
              new OneInputTransformation<>(
                  converted,
                  "NativeFileSourceShare",
                  new NativeColumnarShareOperator(consumers),
                  ArrowBatchTypeInformation.INSTANCE,
                  input.getParallelism());
        return converted;
      }
      return null;
    }
  }

  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType) {
    this(tableConfig, inputProperty, rowType, description, carryRowKind, sourceType, false);
  }

  public RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType,
      boolean preserveCollections) {
    this(
        tableConfig,
        inputProperty,
        rowType,
        description,
        carryRowKind,
        sourceType,
        preserveCollections,
        1024);
  }

  RowDataToArrowExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType rowType,
      String description,
      boolean carryRowKind,
      RowType sourceType,
      boolean preserveCollections,
      int preferredBatchRows) {
    this(
        tableConfig,
        inputProperty,
        rowType,
        description,
        carryRowKind,
        sourceType,
        preserveCollections,
        preferredBatchRows,
        false,
        false);
  }

  RowDataToArrowExecNode(
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
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-row-to-arrow_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        rowType,
        description);
    this.batchRows = NativeConfig.transposeBatchRows(tableConfig, preferredBatchRows);
    this.rowType = rowType;
    this.carryRowKind = carryRowKind;
    this.sourceType = sourceType;
    this.preserveStringCaches = preserveStringCaches;
    var schema = ArrowConversion.toArrowInputSchema(rowType, preserveCollections);
    if (jsonDocuments) {
      var fields =
          schema.getFields().stream()
              .map(
                  field -> {
                    if (!NestedRepresentation.isString(field)) return field;
                    var metadata = new HashMap<>(field.getMetadata());
                    metadata.put(NestedRepresentation.JSON_DOCUMENT, "1");
                    return new Field(
                        field.getName(),
                        new FieldType(
                            field.isNullable(), field.getType(), field.getDictionary(), metadata),
                        field.getChildren());
                  })
              .collect(Collectors.toList());
      schema = new Schema(fields, schema.getCustomMetadata());
    }
    this.arrowSchema = schema.serializeAsMessage();
  }

  public RowDataToArrowExecNode withBinaryRowKeys() {
    binaryRowKeys = true;
    return this;
  }

  static Schema binaryRowKeySchema(RowType type) {
    return ArrowConversion.toBinaryRowKeySchema(type);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<RowData> input =
        (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    if (fileSource != null && acceptsNativeFileSource()) {
      var converted = fileSource.translate(planner, input, rowType, batchRows);
      if (converted != null) return converted;
    }
    return ordinaryTranspose(input, config);
  }

  private Transformation<ArrowBatch> ordinaryTranspose(
      Transformation<RowData> input, ExecNodeConfig config) {
    return ExecNodeUtil.createOneInputTransformation(
        SynchronousArrowInput.forTranspose(input, sourceType == null ? rowType : sourceType),
        createTransformationMeta(TRANSFORMATION, config),
        new RowDataToArrowOperator(
            rowType,
            batchRows,
            carryRowKind,
            sourceType,
            preserveStringCaches,
            binaryRowKeys ? binaryRowKeySchema(rowType).serializeAsMessage() : arrowSchema,
            config.get(org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE)
                == org.apache.flink.api.common.RuntimeExecutionMode.BATCH),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
