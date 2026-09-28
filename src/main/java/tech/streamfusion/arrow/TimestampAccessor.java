package tech.streamfusion.arrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.TimestampData;

/**
 * A borrowed timestamp view exposing Flink's milliseconds and nanoseconds within the millisecond.
 */
public final class TimestampAccessor {
  private static final String COMPONENT_KEY = "streamfusion.timestamp.component";
  private static final String TIMEZONE_KEY = "streamfusion.timestamp.timezone";
  private static final List<Field> FIELDS = List.of(component("millis", 64), component("nano_of_milli", 32));
  private final ValueVector vector;
  private final TimeStampVector primitive;
  private final BigIntVector millis;
  private final IntVector nanos;
  private final TimeUnit unit;

  private static Field component(String name, int bits) {
    return new Field(name, new FieldType(false, new ArrowType.Int(bits, true), null,
        Map.of(COMPONENT_KEY, name)), null);
  }

  public static List<Field> fields() { return FIELDS; }

  public static Field field(String name, boolean nullable) {
    return new Field(name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null), FIELDS);
  }

  public static boolean isComponentTimestamp(Field field) {
    if (!(field.getType() instanceof ArrowType.Struct) || field.getChildren().size() != 2) return false;
    for (int i = 0; i < 2; i++) {
      Field actual = field.getChildren().get(i);
      Field expected = FIELDS.get(i);
      if (!actual.getName().equals(expected.getName())
          || !actual.getType().equals(expected.getType())
          || actual.isNullable()
          || !expected.getName().equals(actual.getMetadata().get(COMPONENT_KEY))) {
        return false;
      }
    }
    return true;
  }

  /** Annotates connector output without changing the timestamp's value or buffer layout. */
  public static Field withTimezone(Field field, String timezone) {
    if (isComponentTimestamp(field)) {
      List<Field> children = new ArrayList<>(field.getChildren());
      Field first = children.get(0);
      Map<String, String> metadata = new HashMap<>(first.getMetadata());
      if (timezone == null) {
        metadata.remove(TIMEZONE_KEY);
      } else {
        metadata.put(TIMEZONE_KEY, timezone);
      }
      children.set(0, new Field(first.getName(),
          new FieldType(false, first.getType(), null, metadata), null));
      return new Field(field.getName(), field.getFieldType(), children);
    }
    ArrowType.Timestamp type = (ArrowType.Timestamp) field.getType();
    FieldType target = new FieldType(field.isNullable(),
        new ArrowType.Timestamp(type.getUnit(), timezone),
        field.getFieldType().getDictionary(), field.getMetadata());
    return new Field(field.getName(), target, null);
  }

  public static boolean isTimestamp(ValueVector vector) {
    return vector instanceof TimeStampVector || isComponentTimestamp(vector.getField());
  }

  /** Writes both components without changing the logical precision or range. */
  public static void set(ValueVector vector, int row, TimestampData value) {
    new TimestampAccessor(vector).set(row, value);
  }

  /** Reuses the validated layout and child vectors across rows and buffer resets. */
  public void set(int row, TimestampData value) {
    if (primitive == null) {
      millis.setSafe(row, value == null ? 0 : value.getMillisecond());
      nanos.setSafe(row, value == null ? 0 : value.getNanoOfMillisecond());
      StructVector struct = (StructVector) vector;
      if (value == null) {
        struct.setNull(row);
      } else {
        struct.setIndexDefined(row);
      }
      return;
    }
    if (value == null) { primitive.setNull(row); return; }
    long milliseconds = value.getMillisecond();
    int fraction = value.getNanoOfMillisecond();
    long encoded;
    switch (unit) {
      case SECOND: encoded = Math.floorDiv(milliseconds, 1000L); break;
      case MILLISECOND: encoded = milliseconds; break;
      case MICROSECOND:
        encoded = TimestampConversion.toMicros(milliseconds, fraction);
        break;
      case NANOSECOND: encoded = TimestampConversion.toNanos(value); break;
      default: throw new IllegalArgumentException("Unsupported timestamp unit");
    }
    primitive.setSafe(row, encoded);
  }

  public TimestampAccessor(ValueVector vector) {
    if (!isTimestamp(vector)) {
      throw new IllegalArgumentException("Expected an Arrow timestamp, got " + vector.getField());
    }
    this.vector = vector;
    if (vector instanceof TimeStampVector) {
      primitive = (TimeStampVector) vector;
      unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
      millis = null;
      nanos = null;
    } else {
      StructVector struct = (StructVector) vector;
      primitive = null;
      unit = null;
      millis = (BigIntVector) struct.getChild("millis");
      nanos = (IntVector) struct.getChild("nano_of_milli");
    }
  }

  public boolean isNull(int row) {
    return vector.isNull(row);
  }

  public long getMillis(int row) {
    return millis != null ? millis.get(row) : toMillis(primitive.get(row));
  }

  public int getNanoOfMillisecond(int row) {
    if (nanos != null) return nanos.get(row);
    switch (unit) {
      case MICROSECOND:
        return (int) Math.floorMod(primitive.get(row), 1000L) * 1000;
      case NANOSECOND:
        return (int) Math.floorMod(primitive.get(row), 1_000_000L);
      default:
        return 0;
    }
  }

  public TimestampData getTimestamp(int row) {
    return TimestampData.fromEpochMillis(getMillis(row), getNanoOfMillisecond(row));
  }

  /** SQL NULL when no row has a timestamp; a raw Long.MIN_VALUE is still a valid timestamp. */
  public Long maxMillis(int rows) {
    long max = Long.MIN_VALUE;
    boolean found = false;
    for (int row = 0; row < rows; row++) {
      if (!isNull(row)) {
        max = Math.max(max, millis != null ? millis.get(row) : primitive.get(row));
        found = true;
      }
    }
    // Unit conversion is monotonic, so only the maximum needs conversion.
    return found ? (millis != null ? max : toMillis(max)) : null;
  }

  private long toMillis(long value) {
    switch (unit) {
      case SECOND:
        return Math.multiplyExact(value, 1000L);
      case MILLISECOND:
        return value;
      case MICROSECOND:
        return Math.floorDiv(value, 1000L);
      case NANOSECOND:
        return Math.floorDiv(value, 1_000_000L);
      default:
        throw new IllegalStateException("Unsupported Arrow timestamp unit " + unit);
    }
  }
}
