package tech.streamfusion.operator;

import java.util.stream.IntStream;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowReader;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Transpose leaving a columnar region: reads each {@link ArrowBatch} back into rows. Sits where a
 * native columnar operator feeds a rowwise (host) one, so the Arrow→row conversion happens once at
 * the boundary. It consumes (and closes) each batch it receives.
 *
 * <p>Primitive, binary and cache-free scalar string outputs use Flink's generated binary-row
 * projection so downstream copying avoids generic field getters and boxing. Other outputs retain
 * the reusable Arrow-backed row view.
 * Chained Flink operators may retain a collected {@code RowData}. With object reuse disabled,
 * Flink's chained output copies the reusable row synchronously; with reuse enabled this boundary
 * supplies the owned copy. Network outputs serialize synchronously before the batch closes.
 */
public class ArrowToRowDataOperator extends FlinkStreamOperator<RowData>
    implements OneInputStreamOperator<ArrowBatch, RowData> {
  private final RowType rowType;
  private final boolean borrowStrings =
      Boolean.getBoolean("streamfusion.experimental.borrowedStringExit");
  private final GeneratedProjection generatedProjection;
  private final boolean requiresBinaryRowCache;
  private final boolean hasStringFields;
  private transient Projection<RowData, BinaryRowData> projection;
  private transient RowDataSerializer outputSerializer;
  private transient Counter numInputBatches;
  private transient Counter numOutputRows;
  private transient Counter convertTime;

  public ArrowToRowDataOperator(RowType rowType) {
    this(rowType, false);
  }

  public ArrowToRowDataOperator(RowType rowType, boolean cachedBinaryRows) {
    this.rowType = rowType;
    hasStringFields = rowType.getChildren().stream().anyMatch(type ->
        type.getTypeRoot() == org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR
            || type.getTypeRoot() == org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR);
    requiresBinaryRowCache =
        cachedBinaryRows
            && rowType.getChildren().stream()
                .anyMatch(
                    type ->
                        type.getTypeRoot()
                            == org.apache.flink.table.types.logical.LogicalTypeRoot.ROW);
    boolean projectBinaryRow =
        rowType.getChildren().stream()
            .allMatch(
                type -> {
                  switch (type.getTypeRoot()) {
                    case ROW:
                      return cachedBinaryRows;
                    case BINARY:
                    case VARBINARY:
                    case CHAR:
                    case VARCHAR:
                    case BOOLEAN:
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT:
                    case DOUBLE:
                    case DATE:
                    case TIME_WITHOUT_TIME_ZONE:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_DAY_TIME:
                      return true;
                    case DECIMAL:
                      return ((DecimalType) type).getPrecision() <= 18;
                    default:
                      return false;
                  }
                });
    generatedProjection =
        projectBinaryRow
            ? ProjectionCodeGenerator.generateProjection(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                "NativeExitRow",
                rowType,
                rowType,
                IntStream.range(0, rowType.getFieldCount()).toArray())
            : null;
  }

  @Override
  public void open() throws Exception {
    super.open();
    projection =
        generatedProjection == null
            ? null
            : generatedProjection.newInstance(getUserCodeClassloader());
    outputSerializer =
        getExecutionConfig().isObjectReuseEnabled() ? new RowDataSerializer(rowType) : null;
    numInputBatches = getMetricGroup().counter("numInputBatches");
    numOutputRows = getMetricGroup().counter("numOutputRows");
    convertTime = getMetricGroup().counter("convertTime");
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    numInputBatches.inc();
    long started = System.nanoTime();
    try (VectorSchemaRoot root = element.getValue().root()) {
      boolean project = projection != null && (!requiresBinaryRowCache || hasBinaryRowCaches(root))
          && (!hasStringFields || tech.streamfusion.arrow.StringCacheColumns.readers(
              root, rowType.getFieldCount()) == null
              && root.getFieldVectors().stream().limit(rowType.getFieldCount())
                  .noneMatch(vector -> tech.streamfusion.arrow.NestedRepresentation.contains(vector.getField())));
      ArrowReader reader =
          borrowStrings || project && hasStringFields
              ? ArrowConversion.createBorrowedStringReader(root, rowType)
              : ArrowConversion.createArrowReader(root, rowType, project && requiresBinaryRowCache);
      TinyIntVector kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      int rowCount = root.getRowCount();
      numOutputRows.inc(rowCount);
      for (int i = 0; i < rowCount; i++) {
        RowData row = reader.read(i);
        if (kinds != null) {
          row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
        }
        if (project) {
          BinaryRowData projected = projection.apply(row);
          projected.setRowKind(row.getRowKind());
          row = projected;
        }
        output.collect(
            new StreamRecord<>(outputSerializer == null ? row : outputSerializer.copy(row)));
      }
    }
    convertTime.inc(System.nanoTime() - started);
  }

  private boolean hasBinaryRowCaches(VectorSchemaRoot root) {
    for (int field = 0; field < rowType.getFieldCount(); field++) {
      if (rowType.getTypeAt(field).getTypeRoot()
          != org.apache.flink.table.types.logical.LogicalTypeRoot.ROW) continue;
      var rows = (org.apache.arrow.vector.complex.StructVector) root.getVector(field);
      var cache = tech.streamfusion.arrow.NestedRepresentation.binaryRows(rows);
      var kinds = tech.streamfusion.arrow.NestedRepresentation.kinds(rows);
      if (cache == null || kinds == null) return false;
      for (int row = 0; row < root.getRowCount(); row++) {
        if (rows.isNull(row)) {
          if (!rowType.getTypeAt(field).isNullable()) return false;
        } else if (cache.isNull(row)
            || kinds.isNull(row)
            || (kinds.get(row)
                    & (tech.streamfusion.arrow.NestedRepresentation.BINARY_ROW
                        | tech.streamfusion.arrow.NestedRepresentation.NESTED_ROW))
                == 0) return false;
      }
    }
    return true;
  }
}
