package tech.streamfusion.format;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.avro.AvroFormatProvider;
import tech.streamfusion.format.avroconfluent.AvroConfluentFormatProvider;
import tech.streamfusion.format.avroconfluent.DebeziumAvroConfluentFormatProvider;
import java.util.Map;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * The Avro providers must decline — not crash job submission for — every table whose type or
 * options the native decode cannot reproduce. Flink's own factory throws for the underivable
 * types at submission, so a declined table reaches the identical Flink failure; a mis-admitted
 * one would decode wrongly typed batches.
 */
class AvroDecodeGateTest {

  private static final RowType SUPPORTED =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
          new String[] {"id", "name", "score"});

  private static boolean bareAvro(RowType rowType, Map<String, String> options, boolean skipErrors) {
    return new AvroFormatProvider()
        .supports(new NativeFormatContext(rowType, rowType, options, skipErrors));
  }

  private static boolean confluent(RowType rowType) {
    Map<String, String> options =
        Map.of("format", "avro-confluent", "avro-confluent.url", "http://localhost:8081");
    return new AvroConfluentFormatProvider()
        .supports(new NativeFormatContext(rowType, rowType, options, false));
  }

  private static boolean debeziumConfluent(RowType rowType) {
    return debeziumConfluent(
        rowType,
        Map.of(
            "format", "debezium-avro-confluent",
            "debezium-avro-confluent.url", "http://localhost:8081"));
  }

  private static boolean debeziumConfluent(RowType rowType, Map<String, String> options) {
    return new DebeziumAvroConfluentFormatProvider()
        .supports(new NativeFormatContext(rowType, rowType, options, false));
  }

  @Test
  void admitsTheReconciledTypeFamily() {
    assertTrue(bareAvro(SUPPORTED, Map.of("format", "avro"), false));
    assertTrue(confluent(SUPPORTED));
    RowType nested =
        RowType.of(
            new LogicalType[] {
              RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"a"}),
              new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType())
            },
            new String[] {"nested", "tags"});
    assertTrue(bareAvro(nested, Map.of("format", "avro"), false));
  }

  @Test
  void declinesTypesFlinkCannotDeriveASchemaFor() {
    // Each of these makes Flink's own factory throw at submission; the native path must decline
    // so the table falls back and reproduces that exact failure.
    LogicalType[] underivable = {
      new LocalZonedTimestampType(3), // TIMESTAMP_LTZ under the legacy mapping
      new TimestampType(6), // precision beyond the legacy mapping
      new TimeType(6),
      new MapType(new IntType(), new BigIntType()) // non-string map key
    };
    for (LogicalType type : underivable) {
      RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"c"});
      assertFalse(bareAvro(rowType, Map.of("format", "avro"), false), type.toString());
      assertFalse(confluent(rowType), type.toString());
      assertFalse(debeziumConfluent(rowType), type.toString());
    }
  }

  @Test
  void debeziumEnvelopeSharesTheConfluentGates() {
    // The gate runs over the derived envelope (nullable images + op), so the physical row's
    // acceptance carries through: supported types admit, underivable and lenient shapes decline.
    assertTrue(debeziumConfluent(SUPPORTED));
    assertFalse(
        debeziumConfluent(
            RowType.of(new LogicalType[] {new LocalZonedTimestampType(3)}, new String[] {"c"})));
    assertFalse(
        debeziumConfluent(RowType.of(new LogicalType[] {new TimeType(0)}, new String[] {"c"})));
    // The registry-option fallbacks apply verbatim: an explicit reader schema stays on Flink.
    assertFalse(
        debeziumConfluent(
            SUPPORTED,
            Map.of(
                "format", "debezium-avro-confluent",
                "debezium-avro-confluent.url", "http://localhost:8081",
                "debezium-avro-confluent.schema", "{}")));
    // No url at all (another format's options in the map) also declines.
    assertFalse(debeziumConfluent(SUPPORTED, Map.of("format", "debezium-avro-confluent")));
  }

  @Test
  void admitsTheReconciledScalarFamily() {
    LogicalType[] reconciled = {
      new TinyIntType(),
      new SmallIntType(),
      new DateType(),
      new TimeType(3),
      new TimestampType(3),
      new DecimalType(10, 2),
      new VarBinaryType(VarBinaryType.MAX_LENGTH),
      new MultisetType(new VarCharType(VarCharType.MAX_LENGTH))
    };
    for (LogicalType type : reconciled) {
      RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"c"});
      assertTrue(bareAvro(rowType, Map.of("format", "avro"), false), type.toString());
      assertTrue(confluent(rowType), type.toString());
    }
  }

  @Test
  void declinesBoundaryShapesFlinkDecodesMoreLenientlyThanArrowCanCarry() {
    // Flink keeps an avro time-millis value's full millis in a TIME(0) column (the boundary's
    // second-precision form would truncate), and accepts any-length bytes into BINARY(n) (the
    // boundary's fixed-size form cannot).
    LogicalType[] lenient = {new TimeType(0), new BinaryType(4)};
    for (LogicalType type : lenient) {
      RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"c"});
      assertFalse(bareAvro(rowType, Map.of("format", "avro"), false), type.toString());
      assertFalse(confluent(rowType), type.toString());
    }
  }

  @Test
  void declinesUnreproducedOptions() {
    assertFalse(bareAvro(SUPPORTED, Map.of("format", "avro", "avro.encoding", "json"), false));
    assertTrue(bareAvro(SUPPORTED, Map.of("format", "avro", "avro.encoding", "binary"), false));
    assertFalse(bareAvro(SUPPORTED, Map.of("format", "avro"), true)); // ignore-parse-errors
    // The prefixed form under value.format resolves the same way.
    assertFalse(
        bareAvro(SUPPORTED, Map.of("value.format", "avro", "value.avro.encoding", "json"), false));
  }

  @Test
  void correctedTimestampMappingFollowsFlinksOwnAcceptance() {
    Map<String, String> nonLegacy =
        Map.of("format", "avro", "avro.timestamp_mapping.legacy", "false");
    assertTrue(bareAvro(SUPPORTED, nonLegacy, false));
    // The corrected mapping unlocks TIMESTAMP_LTZ and micros-precision timestamps at the top level.
    LogicalType[] corrected = {
      new TimestampType(6), new LocalZonedTimestampType(3), new LocalZonedTimestampType(6)
    };
    for (LogicalType type : corrected) {
      RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"c"});
      assertTrue(bareAvro(rowType, nonLegacy, false), type.toString());
      assertFalse(bareAvro(rowType, Map.of("format", "avro"), false), type.toString());
    }
    // Shapes Flink's own factory still rejects under the corrected mapping: precision beyond
    // micros; TIMESTAMP_LTZ inside a nested row (the converter factory drops the flag for nested
    // rows) or a collection (the schema derivation drops it there).
    LogicalType[] rejected = {
      new TimestampType(9),
      new LocalZonedTimestampType(9),
      RowType.of(new LogicalType[] {new LocalZonedTimestampType(3)}, new String[] {"lt"}),
      new ArrayType(new LocalZonedTimestampType(3))
    };
    for (LogicalType type : rejected) {
      RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"c"});
      assertFalse(bareAvro(rowType, nonLegacy, false), type.toString());
    }
  }
}
