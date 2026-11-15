package tech.streamfusion.planner;

import java.lang.reflect.Array;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

/** Retains released array conversions after Flink copies nullable columnar arrays into Object[]. */
public final class ScalarArrayConverter implements DataStructureConverter<ArrayData, Object> {
  private static final long serialVersionUID = 1L;

  private final DataStructureConverter<Object, Object> delegate;
  private final ArrayData.ElementGetter getter;
  private final ScalarArrayConverter nested;
  private final Class<?> internalArrayClass;

  public ScalarArrayConverter(DataType type) {
    delegate = DataStructureConverters.getConverter(type);
    DataType element = type.getChildren().get(0);
    LogicalType elementType = element.getLogicalType();
    getter = ArrayData.createElementGetter(elementType);
    nested = elementType.is(LogicalTypeRoot.ARRAY) ? new ScalarArrayConverter(element) : null;
    internalArrayClass =
        Array.newInstance(LogicalTypeUtils.toInternalConversionClass(elementType), 0).getClass();
  }

  @Override
  public void open(ClassLoader classLoader) {
    delegate.open(classLoader);
    if (nested != null) nested.open(classLoader);
  }

  @Override
  public ArrayData toInternal(Object value) {
    return (ArrayData) delegate.toInternal(value);
  }

  @Override
  public Object toExternal(ArrayData value) {
    return delegate.toExternal(typedArray(value));
  }

  private ArrayData typedArray(ArrayData value) {
    if (!(value instanceof GenericArrayData) || ((GenericArrayData) value).isPrimitiveArray())
      return value;
    Object[] values = ((GenericArrayData) value).toObjectArray();
    if (nested == null && values.getClass() == internalArrayClass) return value;
    Object[] typed =
        (Object[]) Array.newInstance(internalArrayClass.getComponentType(), value.size());
    for (int i = 0; i < typed.length; i++) {
      Object element = getter.getElementOrNull(value, i);
      typed[i] =
          element == null || nested == null ? element : nested.typedArray((ArrayData) element);
    }
    return new GenericArrayData(typed);
  }
}
