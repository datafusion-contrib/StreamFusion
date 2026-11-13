package tech.streamfusion.operator;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;

public final class CanonicalRankScalarFixture {
  private CanonicalRankScalarFixture() {}

  public static Stream<String> shapes() {
    return Stream.of("tinyint", "smallint", "float", "double", "boolean", "char", "decimal",
        "wide-decimal", "date", "time", "timestamp", "timestamp-nanos", "ltz",
        "binary", "varbinary", "months", "millis").map(name -> "scalar-" + name);
  }

  public static String sql(String shape) {
    switch (shape) {
      case "scalar-tinyint": return "TINYINT";
      case "scalar-smallint": return "SMALLINT";
      case "scalar-float": return "FLOAT";
      case "scalar-double": return "DOUBLE";
      case "scalar-boolean": return "BOOLEAN";
      case "scalar-char": return "CHAR(32)";
      case "scalar-decimal": return "DECIMAL(18,4)";
      case "scalar-wide-decimal": return "DECIMAL(38,7)";
      case "scalar-date": return "DATE";
      case "scalar-time": return "TIME(3)";
      case "scalar-timestamp": return "TIMESTAMP(3)";
      case "scalar-timestamp-nanos": return "TIMESTAMP(9)";
      case "scalar-ltz": return "TIMESTAMP_LTZ(3)";
      case "scalar-binary": return "BINARY(4)";
      case "scalar-varbinary": return "VARBINARY(64)";
      case "scalar-months": return "INTERVAL YEAR TO MONTH";
      case "scalar-millis": return "INTERVAL DAY TO SECOND(3)";
      default: throw new AssertionError(shape);
    }
  }

  static LogicalType type(String shape) {
    switch (shape) {
      case "scalar-tinyint": return new TinyIntType();
      case "scalar-smallint": return new SmallIntType();
      case "scalar-float": return new FloatType();
      case "scalar-double": return new DoubleType();
      case "scalar-boolean": return new BooleanType();
      case "scalar-char": return new CharType(32);
      case "scalar-decimal": return new DecimalType(18, 4);
      case "scalar-wide-decimal": return new DecimalType(38, 7);
      case "scalar-date": return new DateType();
      case "scalar-time": return new TimeType(3);
      case "scalar-timestamp": return new TimestampType(3);
      case "scalar-timestamp-nanos": return new TimestampType(9);
      case "scalar-ltz": return new LocalZonedTimestampType(3);
      case "scalar-binary": return new BinaryType(4);
      case "scalar-varbinary": return new VarBinaryType(64);
      case "scalar-months": return new YearMonthIntervalType(
          YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH);
      case "scalar-millis": return new DayTimeIntervalType(
          DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 3);
      default: throw new AssertionError(shape);
    }
  }

  static Object value(String shape, Integer value) {
    if (value == null) return null;
    switch (shape) {
      case "scalar-tinyint": return value.byteValue();
      case "scalar-smallint": return value.shortValue();
      case "scalar-float": return value.floatValue();
      case "scalar-double": return value.doubleValue();
      case "scalar-boolean": return (value & 1) != 0;
      case "scalar-char": return StringData.fromString(String.format(java.util.Locale.ROOT, "%03d", value));
      case "scalar-decimal":
      case "scalar-wide-decimal":
        var decimal = (DecimalType) type(shape);
        return DecimalData.fromBigDecimal(BigDecimal.valueOf(value, decimal.getScale()),
            decimal.getPrecision(), decimal.getScale());
      case "scalar-date":
      case "scalar-months": return value;
      case "scalar-time": return Math.floorMod(value, 86_400_000);
      case "scalar-timestamp":
      case "scalar-ltz": return TimestampData.fromEpochMillis(value);
      case "scalar-timestamp-nanos": return TimestampData.fromEpochMillis(value,
          Math.floorMod(value * 997, 1_000_000));
      case "scalar-binary":
      case "scalar-varbinary": return ByteBuffer.allocate(4).putInt(value).array();
      case "scalar-millis": return value.longValue();
      default: throw new AssertionError(shape);
    }
  }

  static Object score(String shape, Integer score) {
    if (score != null && shape.equals("scalar-float")) {
      if (score == 10) return -0.0f;
      if (score == 20) return 0.0f;
      if (score == 50) return Float.intBitsToFloat(0x7fc00011);
    }
    if (score != null && shape.equals("scalar-double")) {
      if (score == 10) return -0.0d;
      if (score == 20) return 0.0d;
      if (score == 50) return Double.longBitsToDouble(0x7ff8000000000011L);
    }
    return value(shape, score);
  }
}
