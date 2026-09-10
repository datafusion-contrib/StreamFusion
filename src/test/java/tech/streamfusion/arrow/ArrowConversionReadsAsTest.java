package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * What a native output column may look like and still be read as its declared Flink type: exactly
 * the Arrow type the row type converts to, except that the timestamp and time vectors convert any
 * unit on read and a day-time interval may arrive in either encoding.
 */
class ArrowConversionReadsAsTest {

  /**
   * An interval-typed expression arrives as a native interval array rather than the milliseconds
   * the row type converts to. Rejecting it here declines the whole projection, so the vector that
   * reads the encoding would never be reached.
   */
  @Test
  void acceptsANativeDayTimeIntervalForMilliseconds() {
    assertTrue(
        ArrowConversion.readsAs(
            Field.nullable("i", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
            DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType()));
  }

  /** A year-month interval is months in an Int(32); the day-time encoding is not that. */
  @Test
  void rejectsANativeDayTimeIntervalForMonths() {
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("i", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
            DataTypes.INTERVAL(DataTypes.MONTH()).getLogicalType()));
  }

  /**
   * A timestamp difference evaluates as a duration, which stays out: Flink floors each operand to
   * milliseconds before subtracting, so narrowing the finished difference diverges on sub-millisecond
   * operands. It declines at plan time and runs on the host instead.
   */
  @Test
  void rejectsANativeDuration() {
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("d", new ArrowType.Duration(TimeUnit.NANOSECOND)),
            DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType()));
  }

  /**
   * A day-time interval converts to the same Int(64) a BIGINT does, so admitting on the Arrow type
   * alone would feed a BIGINT column to a vector that folds days and millis.
   */
  @Test
  void rejectsANativeIntervalForABigint() {
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("i", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
            DataTypes.BIGINT().getLogicalType()));
  }

  /**
   * A map nests as one Arrow struct child but two Flink children, and a multiset as one of each with
   * different meanings, so anything walking the two side by side misaligns or runs off the end. These
   * reach the check from any Calc projecting such a column.
   */
  @Test
  void walksNestedShapesWhoseFlinkAndArrowChildrenDiffer() {
    LogicalType map = DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()).getLogicalType();
    Field mapField =
        ArrowConversion.toArrowSchema(RowType.of(new LogicalType[] {map}, new String[] {"c"}))
            .getFields()
            .get(0);
    assertTrue(ArrowConversion.readsAs(mapField, map), map.toString());

    LogicalType multiset = DataTypes.MULTISET(DataTypes.STRING()).getLogicalType();
    Field multisetField =
        ArrowConversion.toArrowSchema(RowType.of(new LogicalType[] {multiset}, new String[] {"c"}))
            .getFields()
            .get(0);
    assertTrue(ArrowConversion.readsAs(multisetField, multiset), multiset.toString());

    LogicalType mapOfRow =
        DataTypes.MAP(DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())), DataTypes.STRING())
            .getLogicalType();
    Field mapOfRowField =
        ArrowConversion.toArrowSchema(RowType.of(new LogicalType[] {mapOfRow}, new String[] {"c"}))
            .getFields()
            .get(0);
    assertTrue(ArrowConversion.readsAs(mapOfRowField, mapOfRow), mapOfRow.toString());

    LogicalType arrayOfMap =
        DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())).getLogicalType();
    Field arrayOfMapField =
        ArrowConversion.toArrowSchema(
                RowType.of(new LogicalType[] {arrayOfMap}, new String[] {"c"}))
            .getFields()
            .get(0);
    assertTrue(ArrowConversion.readsAs(arrayOfMapField, arrayOfMap), arrayOfMap.toString());
  }

  /**
   * Year-month is not admitted here: its canonical form is months in an Int(32), and the encoder
   * declines those literals before a batch exists, so there is no reader to reach.
   */
  @Test
  void rejectsANativeYearMonthInterval() {
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("i", new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
            DataTypes.INTERVAL(DataTypes.MONTH()).getLogicalType()));
    assertFalse(
        ArrowConversion.readsAs(
            Field.nullable("i", new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
            DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType()));
  }

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
