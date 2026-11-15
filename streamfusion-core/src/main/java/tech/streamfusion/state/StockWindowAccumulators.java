package tech.streamfusion.state;

final class StockWindowAccumulators {
  private StockWindowAccumulators() {}

  static Object zero(org.apache.flink.table.types.logical.LogicalType type) {
    switch (type.getTypeRoot()) {
      case TINYINT:
        return (byte) 0;
      case SMALLINT:
        return (short) 0;
      case INTEGER:
        return 0;
      case BIGINT:
        return 0L;
      case FLOAT:
        return 0F;
      case DOUBLE:
        return 0D;
      case DECIMAL:
        var decimal = (org.apache.flink.table.types.logical.DecimalType) type;
        return org.apache.flink.table.data.DecimalData.fromBigDecimal(
            java.math.BigDecimal.ZERO, decimal.getPrecision(), decimal.getScale());
      default:
        throw new IllegalStateException("unsupported numeric accumulator: " + type);
    }
  }
}
