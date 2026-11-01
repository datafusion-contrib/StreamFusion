package tech.streamfusion.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;

/** Typed, stock-encoded literal arguments for an OVER operator's internal Arrow columns. */
public final class OverConstantArguments {
  private final RowType type;
  private final byte[] values;

  private OverConstantArguments(RowType type, byte[] values) {
    this.type = type;
    this.values = values;
  }

  public static Optional<OverConstantArguments> create(List<RexLiteral> literals, boolean countStar)
      throws IOException {
    return create(
        literals,
        countStar,
        java.util.stream.IntStream.range(0, literals.size())
            .boxed()
            .collect(java.util.stream.Collectors.toSet()));
  }

  public static Optional<OverConstantArguments> create(
      List<RexLiteral> literals, boolean countStar, java.util.Set<Integer> referenced)
      throws IOException {
    var fields = new ArrayList<RowType.RowField>();
    var row = new GenericRowData(literals.size() + (countStar ? 1 : 0));
    for (int i = 0; i < literals.size(); i++) {
      var literal = literals.get(i);
      var type = FlinkTypeFactory.toLogicalType(literal.getType());
      if (!type.is(LogicalTypeFamily.NUMERIC)) {
        if (referenced.contains(i)) return Optional.empty();
        fields.add(new RowType.RowField("constant_" + i, new BigIntType(false)));
        row.setField(i, 0L);
        continue;
      }
      fields.add(new RowType.RowField("constant_" + i, type));
      row.setField(i, literal.isNull() ? null : value(literal, type));
    }
    if (countStar) {
      fields.add(new RowType.RowField("count_star", new BigIntType(false)));
      row.setField(literals.size(), 1L);
    }
    var type = new RowType(fields);
    var encoded = new DataOutputSerializer(32);
    new RowDataSerializer(type).serialize(row, encoded);
    return Optional.of(new OverConstantArguments(type, encoded.getCopyOfBuffer()));
  }

  private static Object value(RexLiteral literal, LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return literal.getValueAs(Byte.class);
      case SMALLINT:
        return literal.getValueAs(Short.class);
      case INTEGER:
        return literal.getValueAs(Integer.class);
      case BIGINT:
        return literal.getValueAs(Long.class);
      case FLOAT:
        return literal.getValueAs(Float.class);
      case DOUBLE:
        return literal.getValueAs(Double.class);
      case DECIMAL:
        var decimal = (DecimalType) type;
        return DecimalData.fromBigDecimal(
            literal.getValueAs(java.math.BigDecimal.class),
            decimal.getPrecision(),
            decimal.getScale());
      default:
        throw new IllegalArgumentException("Unsupported OVER literal type: " + type);
    }
  }

  public RowType type() {
    return type;
  }

  public byte[] encodedValues() {
    return values.clone();
  }

  public RowType appendTo(RowType input) {
    var fields = new ArrayList<>(input.getFields());
    var names = new java.util.HashSet<>(input.getFieldNames());
    for (var field : type.getFields()) {
      String name = field.getName();
      while (!names.add(name)) name += "_";
      fields.add(new RowType.RowField(name, field.getType()));
    }
    return new RowType(fields);
  }
}
