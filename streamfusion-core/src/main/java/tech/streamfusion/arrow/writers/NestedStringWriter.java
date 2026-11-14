package tech.streamfusion.arrow.writers;

import java.util.function.BiFunction;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryStringData;
import tech.streamfusion.arrow.NestedRepresentation;
import tech.streamfusion.arrow.Utf16Cache;

/** Preserves caches before UTF-8 encoding can materialize a lazy StringData. */
public final class NestedStringWriter<T> extends ArrowFieldWriter<T> {
  private final BiFunction<T, Integer, StringData> getter;
  private final ListVector cache;
  private final Utf16Cache.Writer cacheWriter;
  private final ArrowFieldWriter<RowData> utf8;
  private final GenericRowData row = new GenericRowData(1);
  private final TinyIntVector copyClass;
  private final boolean jsonDocument;

  private NestedStringWriter(StructVector vector, BiFunction<T, Integer, StringData> getter) {
    super(vector);
    this.getter = getter;
    cache = (ListVector) vector.getChild("java");
    cacheWriter = new Utf16Cache.Writer(cache);
    utf8 = VarCharWriter.forRow((VarCharVector) vector.getChild("utf8"));
    copyClass = (TinyIntVector) vector.getChild("array_copy_class");
    jsonDocument = "1".equals(vector.getField().getMetadata().get(NestedRepresentation.JSON_DOCUMENT));
  }

  public static NestedStringWriter<RowData> forRow(StructVector vector) {
    return new NestedStringWriter<>(
        vector, (row, pos) -> row.isNullAt(pos) ? null : row.getString(pos));
  }

  public static NestedStringWriter<ArrayData> forArray(StructVector vector) {
    return new NestedStringWriter<>(
        vector, (row, pos) -> row.isNullAt(pos) ? null : row.getString(pos));
  }

  @Override
  public void doWrite(T input, int ordinal) {
    StringData value = getter.apply(input, ordinal);
    if (copyClass != null) copyClass.setSafe(getCount(), input instanceof BinaryArrayData ? 1 : 0);
    var parent = (StructVector) getValueVector();
    if (value == null) parent.setNull(getCount());
    else parent.setIndexDefined(getCount());
    String java =
        value instanceof BinaryStringData ? ((BinaryStringData) value).getJavaObject() : null;
    if (jsonDocument && java != null && redundantJsonCache((BinaryStringData) value, java)) java = null;
    cacheWriter.write(getCount(), java);
    row.setField(0, value);
    utf8.write(row, 0);
    row.setField(0, null);
  }

  private static boolean redundantJsonCache(BinaryStringData value, String java) {
    // A lazy, valid Java string will encode losslessly; JSON parsing cannot observe its cache.
    if (value.getBinarySection() == null) {
      for (int i = 0; i < java.length(); i++) {
        char unit = java.charAt(i);
        if (Character.isHighSurrogate(unit)) {
          if (++i == java.length() || !Character.isLowSurrogate(java.charAt(i))) return false;
        } else if (Character.isLowSurrogate(unit)) return false;
      }
      return true;
    }
    if (value.getSizeInBytes() != java.length()) return false;
    for (int i = 0; i < java.length(); i++) {
      char unit = java.charAt(i);
      if (unit > 0x7f || value.byteAt(i) != unit) return false;
    }
    return true;
  }

  @Override
  public void finish() {
    utf8.finish();
    cache.setValueCount(getCount());
    if (copyClass != null) copyClass.setValueCount(getCount());
    super.finish();
  }

  @Override
  public void reset() {
    super.reset();
    utf8.reset();
  }
}
