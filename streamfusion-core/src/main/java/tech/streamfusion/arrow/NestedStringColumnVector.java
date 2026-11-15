package tech.streamfusion.arrow;

import org.apache.arrow.vector.TinyIntVector;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import tech.streamfusion.arrow.vectors.ArrowVarCharColumnVector;

/** UTF-8 data plus its optional original Java UTF-16 cache, both owned by the Arrow batch. */
public final class NestedStringColumnVector implements BytesColumnVector {
  private final StructVector vector;
  private final VarCharVector utf8;
  private final ArrowVarCharColumnVector bytes;
  private final ListVector cache;
  private final TinyIntVector copyClass;

  public NestedStringColumnVector(StructVector vector) {
    this.vector = vector;
    utf8 = (VarCharVector) vector.getChild("utf8");
    bytes = new ArrowVarCharColumnVector(utf8);
    cache = (ListVector) vector.getChild("java");
    copyClass = (TinyIntVector) vector.getChild("array_copy_class");
  }

  public StringData getString(int index) {
    var binary = BinaryStringData.fromBytes(utf8.get(index));
    if (!cache.isNull(index)) binary.setJavaObject(Utf16Cache.read(cache, index));
    return binary;
  }

  @Override
  public Bytes getBytes(int index) {
    return bytes.getBytes(index);
  }

  @Override
  public boolean isNullAt(int index) {
    return vector.isNull(index);
  }

  public boolean binaryArray(int index) {
    return copyClass != null && !copyClass.isNull(index) && copyClass.get(index) != 0;
  }
}
