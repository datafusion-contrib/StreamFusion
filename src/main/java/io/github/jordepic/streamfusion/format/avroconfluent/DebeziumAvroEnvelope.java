package io.github.jordepic.streamfusion.format.avroconfluent;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Flink's Debezium envelope row type over the table's physical row (its deserializer's own
 * derivation calls). Lives outside the provider because the derivation passes a {@code RowType}
 * where Flink declares {@code LogicalType}: the bytecode verifier must load both to prove that
 * assignability, and provider classes must stay linkable with no Flink on the classpath (the
 * extension-JAR probe, pinned by {@code NativeFormatProviderContractTest}); this class only links
 * once a provider method runs, inside a planner JVM that has Flink.
 */
final class DebeziumAvroEnvelope {

  private DebeziumAvroEnvelope() {}

  static RowType rowType(RowType physical) {
    if (physical == null) {
      return null;
    }
    DataType image = TypeConversions.fromLogicalToDataType(physical).nullable();
    return (RowType)
        DataTypes.ROW(
                DataTypes.FIELD("before", image),
                DataTypes.FIELD("after", image),
                DataTypes.FIELD("op", DataTypes.STRING()))
            .getLogicalType();
  }
}
