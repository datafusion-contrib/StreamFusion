package tech.streamfusion.arrow.writers;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;

/** Records the released serializer's array copy class even when its first logical value is NULL. */
public final class ArrayElementWriter extends ArrowFieldWriter<ArrayData> {
  private final ArrowFieldWriter<ArrayData> value;
  private final TinyIntVector copyClass;

  public ArrayElementWriter(StructVector vector, ArrowFieldWriter<ArrayData> value) {
    super(vector);
    this.value = value;
    copyClass = (TinyIntVector) vector.getChild("array_copy_class");
  }

  @Override
  public void doWrite(ArrayData input, int ordinal) {
    ((StructVector) getValueVector()).setIndexDefined(getCount());
    copyClass.setSafe(getCount(), input instanceof BinaryArrayData ? 1 : 0);
    value.write(input, ordinal);
  }

  @Override
  public void finish() {
    value.finish();
    copyClass.setValueCount(getCount());
    super.finish();
  }

  @Override
  public void reset() {
    super.reset();
    value.reset();
  }
}
