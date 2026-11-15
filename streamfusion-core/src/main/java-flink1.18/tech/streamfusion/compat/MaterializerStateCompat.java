package tech.streamfusion.compat;

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Released 1.18 materializers use only the legacy values list. */
public final class MaterializerStateCompat {
  private MaterializerStateCompat() {}

  public static boolean supportsV2() {
    return false;
  }

  private static UnsupportedOperationException unavailable() {
    return new UnsupportedOperationException("Flink 1.18 has no V2 materializer state");
  }

  public static GeneratedRecordEqualiser equaliser(RowType type) {
    throw unavailable();
  }

  public static GeneratedHashFunction hash(RowType type) {
    throw unavailable();
  }

  public static CompositeSerializer<?> nodes(TypeSerializer<RowData> rows) {
    throw unavailable();
  }

  public static CompositeSerializer<?> indexes() {
    throw unavailable();
  }

  public static CompositeSerializer<?> highest() {
    throw unavailable();
  }

  public static TypeSerializer<?> key(
      RowDataSerializer rows, GeneratedRecordEqualiser eq, GeneratedHashFunction hash) {
    throw unavailable();
  }
}
