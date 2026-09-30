package tech.streamfusion.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

/** Keeps native sinks behind Flink's row-level constraint-enforcement boundary. */
final class SinkConstraintGate {
  private SinkConstraintGate() {}

  static String fallbackReason(StreamPhysicalSink sink) {
    return fallbackReason(sink, false);
  }

  static String fallbackReason(StreamPhysicalSink sink, boolean enforcesNotNullErrors) {
    ResolvedCatalogTable table =
        (ResolvedCatalogTable) sink.contextResolvedTable().getResolvedTable();
    RowType targetType =
        (RowType) table.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    RelDataType inputType = sink.getInput().getRowType();
    ExecutionConfigOptions.NotNullEnforcer notNullEnforcer =
        ShortcutUtils.unwrapTableConfig(sink)
            .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);

    // Flink adds the check for every required target field, but a non-nullable relational input
    // makes it provably inert and does not need to exclude the native sink.
    for (int i = 0; i < targetType.getFieldCount(); i++) {
      if (!targetType.getTypeAt(i).isNullable()
          && inputType.getFieldList().get(i).getType().isNullable()) {
        if (enforcesNotNullErrors
            && notNullEnforcer == ExecutionConfigOptions.NotNullEnforcer.ERROR) {
          continue;
        }
        return "nullable input for NOT NULL column "
            + targetType.getFieldNames().get(i)
            + " requires Flink's ConstraintEnforcer ("
            + ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key()
            + "="
            + notNullEnforcer
            + ")";
      }
    }

    ExecutionConfigOptions.TypeLengthEnforcer typeLengthEnforcer =
        ShortcutUtils.unwrapTableConfig(sink)
            .get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER);
    if (typeLengthEnforcer == ExecutionConfigOptions.TypeLengthEnforcer.IGNORE) {
      return null;
    }
    for (RowType.RowField field : targetType.getFields()) {
      if (hasLengthConstraint(field.getType())) {
        return "target column "
            + field.getName()
            + " uses "
            + field.getType().asSummaryString()
            + " and requires Flink's ConstraintEnforcer ("
            + ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key()
            + "="
            + typeLengthEnforcer
            + ")";
      }
    }
    return null;
  }

  private static boolean hasLengthConstraint(LogicalType type) {
    return type.is(LogicalTypeRoot.CHAR)
        || type.is(LogicalTypeRoot.BINARY)
        || (type.is(LogicalTypeRoot.VARCHAR)
            && ((VarCharType) type).getLength() < VarCharType.MAX_LENGTH)
        || (type.is(LogicalTypeRoot.VARBINARY)
            && ((VarBinaryType) type).getLength() < VarBinaryType.MAX_LENGTH);
  }
}
