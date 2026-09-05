package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

/**
 * What a native output column may look like and still be read as its declared Flink type: exactly
 * the Arrow type the row type converts to, except that the timestamp and time vectors convert any
 * unit on read.
 */
class ArrowConversionReadsAsTest {

  @Test
  void acceptsTheConvertedType() {
    assertTrue(
        ArrowConversion.readsAs(
            Field.nullable("d", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            DataTypes.DOUBLE().getLogicalType()));
  }

  @Test
  void rejectsADifferentWidth() {
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("f", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            DataTypes.DOUBLE().getLogicalType()));
  }

  @Test
  void rejectsADifferentDecimalScale() {
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("d", new ArrowType.Decimal(10, 2, 128)),
            DataTypes.DECIMAL(10, 3).getLogicalType()));
  }

  @Test
  void acceptsAnyTimestampUnitAndZone() {
    // PROCTIME() is stamped natively as millisecond UTC where the row type converts to nanoseconds.
    assertTrue(
        ArrowConversion.readsAs(
            Field.nullable("pt", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            DataTypes.TIMESTAMP_LTZ(3).getLogicalType()));
    assertTrue(
        ArrowConversion.readsAs(
            Field.nullable("t", new ArrowType.Time(TimeUnit.SECOND, 32)),
            DataTypes.TIME(3).getLogicalType()));
  }

  @Test
  void holdsNestedElementsToTheSameRule() {
    Field intElement = Field.nullable("element", new ArrowType.Int(32, true));
    Field ints = new Field("a", Field.nullable("a", ArrowType.List.INSTANCE).getFieldType(), List.of(intElement));
    assertTrue(ArrowConversion.readsAs(ints, DataTypes.ARRAY(DataTypes.INT()).getLogicalType()));
    assertFalse(ArrowConversion.readsAs(ints, DataTypes.ARRAY(DataTypes.BIGINT()).getLogicalType()));
  }
}
