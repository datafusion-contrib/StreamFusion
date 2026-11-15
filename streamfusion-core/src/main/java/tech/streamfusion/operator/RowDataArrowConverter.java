package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowReader;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.arrow.StringCacheColumns;

/**
 * Converts whole {@link RowData} rows of a fixed schema to and from an Arrow {@link VectorSchemaRoot},
 * one Arrow vector per column. The actual per-type conversion is {@link ArrowConversion} (vendored from
 * Flink's own Arrow↔RowData machinery), which handles every logical type including nested ROW/ARRAY/MAP
 * and binary; this class is the thin boundary wrapper that adds the {@code $row_kind$} carriage and the
 * operator-eligibility predicate.
 *
 * <p>A changelog stream's {@link RowKind} lives as per-row metadata on {@code RowData}, for which Arrow
 * has no equivalent; to carry it across the boundary it is written as a hidden byte column ({@link
 * #ROW_KIND_COLUMN}) when requested, and read back onto each row. See divergences/13.
 */
public final class RowDataArrowConverter {

  /**
   * Name of the hidden column carrying Flink's four-way {@link RowKind} as a byte. The {@code $}
   * prefix follows Flink's convention for system columns so it cannot collide with a SQL field.
   */
  public static final String ROW_KIND_COLUMN = "$row_kind$";

  private RowDataArrowConverter() {}

  /**
   * Whether every column can cross the row/Arrow boundary. Operators using a narrower retained-row
   * codec must additionally check that codec's supported types.
   * This is deliberately aligned with {@link ArrowConversion}: an admitted type must make the full
   * RowData → Arrow → RowData round trip before an operator is allowed to retain or key it.
   */
  public static boolean supports(RowType rowType) {
    return rowType.getChildren().stream().allMatch(RowDataArrowConverter::isSupportedType);
  }

  private static boolean isSupportedType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
      case DECIMAL:
        return true;
      case ARRAY:
      case MULTISET:
      case MAP:
      case ROW:
        // Nested types ride the Arrow boundary, and a keyed/stateful operator carries them through
        // its row state via DataFusion ScalarValue (List/Struct/Map); the reconstructed array is cast
        // back to the declared column type on emit (scalars_to_array). Admitted recursively to
        // supported leaves. Whether an operator can *order* a nested value is gated separately (a MAX
        // over an array, or ORDER BY an array, still falls back — Flink rejects those too).
        return type.getChildren().stream().allMatch(RowDataArrowConverter::isSupportedType);
      default:
        return false;
    }
  }

  /** Builds an Arrow batch holding {@code rows} under {@code rowType}. The caller closes the root. */
  public static VectorSchemaRoot write(
      List<RowData> rows, RowType rowType, BufferAllocator allocator) {
    return write(rows, rowType, allocator, false);
  }

  /**
   * As {@link #write(List, RowType, BufferAllocator)}, but when {@code withRowKind} also appends
   * the {@link #ROW_KIND_COLUMN} so a changelog stream's {@link RowKind} survives the boundary.
   */
  public static VectorSchemaRoot write(
      List<RowData> rows, RowType rowType, BufferAllocator allocator, boolean withRowKind) {
    return write(rows, rowType, allocator, withRowKind, false);
  }

  /** Draft string-representation transport; planner admission requires complete pipeline validation. */
  public static VectorSchemaRoot write(List<RowData> rows, RowType rowType,
      BufferAllocator allocator, boolean withRowKind, boolean preserveStringCaches) {
    VectorSchemaRoot dataRoot =
        VectorSchemaRoot.create(ArrowConversion.toArrowSchema(rowType), allocator);
    TinyIntVector kinds = null;
    StringCacheColumns.Writer caches = null;
    try {
      if (preserveStringCaches) caches = new StringCacheColumns.Writer(rowType, allocator);
      ArrowWriter<RowData> writer = ArrowConversion.createRowDataArrowWriter(dataRoot, rowType);
      int position = 0;
      for (RowData row : rows) {
        if (caches != null) caches.write(row, position++);
        writer.write(row);
      }
      writer.finish();
      if (!withRowKind && caches == null) return dataRoot;
      // Append the hidden RowKind byte column alongside the data vectors (a fresh root over the
      // same
      // vectors plus the kinds vector — the caller closes this root, freeing them all once).
      List<FieldVector> columns = new ArrayList<>();
      if (withRowKind) {
        kinds = new TinyIntVector(ROW_KIND_COLUMN, allocator);
        kinds.allocateNew(rows.size());
        for (int i = 0; i < rows.size(); i++) kinds.set(i, rows.get(i).getRowKind().toByteValue());
        kinds.setValueCount(rows.size());
        columns.add(kinds);
      }
      if (caches != null) columns.addAll(caches.finish(rows.size()));
      return withExtraColumns(dataRoot, columns, rows.size());
    } catch (RuntimeException | Error failure) {
      try {
        org.apache.flink.util.IOUtils.closeAll(dataRoot, kinds, caches);
      } catch (Exception | Error closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  /** Transfers the original vectors and companions to one root without deriving nested fields anew. */
  static VectorSchemaRoot withExtraColumns(
      VectorSchemaRoot root, List<FieldVector> extra, int rows) {
    var columns = new ArrayList<>(root.getFieldVectors());
    columns.addAll(extra);
    var fields = new ArrayList<>(root.getSchema().getFields());
    for (var vector : extra) fields.add(vector.getField());
    return new VectorSchemaRoot(
        new org.apache.arrow.vector.types.pojo.Schema(fields, root.getSchema().getCustomMetadata()),
        columns,
        rows);
  }

  /**
   * Reads the rows of an Arrow batch back into independent {@link RowData}s under {@code rowType}.
   * The Arrow buffers are read through the vendored columnar reader (no per-cell hand-copy), then
   * each row is deep-copied off the buffers so it stays valid after the batch is released — the
   * caller (a transpose at a columnar→rowwise edge) owns and closes the batch as soon as it has
   * read it. A trailing {@link #ROW_KIND_COLUMN}, if present, is read back onto each row's {@link
   * RowKind}. (A true zero-copy view would require the batch to outlive the rows, which a native
   * columnar sink would allow — https://github.com/datafusion-contrib/StreamFusion/issues/17.)
   */
  public static List<RowData> read(VectorSchemaRoot root, RowType rowType) {
    ArrowReader reader = ArrowConversion.createArrowReader(root, rowType);
    RowDataSerializer serializer = new RowDataSerializer(rowType);
    int rowCount = root.getRowCount();
    TinyIntVector kinds = (TinyIntVector) root.getVector(ROW_KIND_COLUMN);
    List<RowData> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      RowData row = serializer.copy(reader.read(i));
      if (kinds != null) {
        row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
      }
      rows.add(row);
    }
    return rows;
  }
}
