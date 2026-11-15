package tech.streamfusion.planner;

import java.io.Serializable;
import java.lang.reflect.Array;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

/** Keeps released Java conversions compatible with copied columnar arrays. */
public final class ExternalContainerConverter implements DataStructureConverter<Object, Object> {
  private static final long serialVersionUID = 1L;
  private final DataStructureConverter<Object, Object> delegate;
  private final Container internal;

  public ExternalContainerConverter(DataType type) {
    delegate = DataStructureConverters.getConverter(type);
    internal = new Container(type);
  }

  static boolean containsArray(DataType type) {
    return type.getLogicalType().is(LogicalTypeRoot.ARRAY)
        || type.getChildren().stream().anyMatch(ExternalContainerConverter::containsArray);
  }

  @Override
  public void open(ClassLoader classLoader) {
    delegate.open(classLoader);
  }

  @Override
  public Object toInternal(Object value) {
    return delegate.toInternal(value);
  }

  @Override
  public Object toExternal(Object value) {
    return delegate.toExternal(internal.normalize(value));
  }

  private static final class Container implements Serializable {
    private static final long serialVersionUID = 1L;
    private final LogicalTypeRoot root;
    private final boolean containsArray;
    private final Container[] children;
    private final RowData.FieldGetter[] fields;
    private final ArrayData.ElementGetter element;
    private final Class<?> elementClass;

    Container(DataType type) {
      root = type.getLogicalType().getTypeRoot();
      containsArray = containsArray(type);
      if (root == LogicalTypeRoot.MAP) {
        children = type.getChildren().stream().map(Container::array).toArray(Container[]::new);
      } else children = type.getChildren().stream().map(Container::new).toArray(Container[]::new);
      fields =
          root == LogicalTypeRoot.ROW
              ? java.util.stream.IntStream.range(0, children.length)
                  .mapToObj(
                      i ->
                          RowData.createFieldGetter(
                              type.getChildren().get(i).getLogicalType().copy(true), i))
                  .toArray(RowData.FieldGetter[]::new)
              : null;
      var child = root == LogicalTypeRoot.ARRAY ? type.getChildren().get(0).getLogicalType() : null;
      element = child == null ? null : ArrayData.createElementGetter(child.copy(true));
      elementClass = child == null ? null : LogicalTypeUtils.toInternalConversionClass(child);
    }

    private static Container array(DataType element) {
      return new Container(org.apache.flink.table.api.DataTypes.ARRAY(element));
    }

    Object normalize(Object value) {
      if (value == null || !containsArray) return value;
      switch (root) {
        case ARRAY:
          return normalizeArray((ArrayData) value);
        case ROW:
          var row = (RowData) value;
          GenericRowData result = null;
          for (int i = 0; i < children.length; i++) {
            if (result == null && !children[i].containsArray) continue;
            Object original = fields[i].getFieldOrNull(row);
            Object normalized = children[i].normalize(original);
            if (result == null && original != normalized) {
              result = new GenericRowData(row.getRowKind(), children.length);
              for (int j = 0; j < i; j++) result.setField(j, fields[j].getFieldOrNull(row));
            }
            if (result != null) result.setField(i, normalized);
          }
          return result == null ? row : result;
        case MAP:
          var map = (MapData) value;
          var originalKeys = map.keyArray();
          var originalValues = map.valueArray();
          var keys = (ArrayData) children[0].normalize(originalKeys);
          var values = (ArrayData) children[1].normalize(originalValues);
          return keys == originalKeys && values == originalValues
              ? map
              : new ArrayMap(keys, values);
        default:
          return value;
      }
    }

    private ArrayData normalizeArray(ArrayData array) {
      Object[] typed = null;
      if (array instanceof GenericArrayData) {
        var generic = (GenericArrayData) array;
        if (!generic.isPrimitiveArray()
            && generic.toObjectArray().getClass().getComponentType() != elementClass)
          typed = (Object[]) Array.newInstance(elementClass, array.size());
      }
      if (typed == null && !children[0].containsArray) return array;
      for (int i = 0; i < array.size(); i++) {
        Object original = element.getElementOrNull(array, i);
        Object normalized = children[0].normalize(original);
        if (typed == null && original != normalized) {
          typed = (Object[]) Array.newInstance(elementClass, array.size());
          for (int j = 0; j < i; j++) typed[j] = element.getElementOrNull(array, j);
        }
        if (typed != null) typed[i] = normalized;
      }
      return typed == null ? array : new GenericArrayData(typed);
    }
  }

  private static final class ArrayMap implements MapData {
    private final ArrayData keys;
    private final ArrayData values;

    ArrayMap(ArrayData keys, ArrayData values) {
      this.keys = keys;
      this.values = values;
    }

    @Override
    public int size() {
      return keys.size();
    }

    @Override
    public ArrayData keyArray() {
      return keys;
    }

    @Override
    public ArrayData valueArray() {
      return values;
    }
  }
}
