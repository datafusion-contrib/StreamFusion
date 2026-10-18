package tech.streamfusion.iceberg;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.StructLikeMap;

/**
 * Memoizes host routing for immutable integer partition tuples, including bucket/time transforms.
 */
final class IcebergPartitionGroups implements KeySelector<RowData, Integer> {
  private static final int MAX_ENTRIES = 1024;
  private final PartitionKey key;
  private final int maxParallelism;
  private final transient RowDataWrapper rows;
  private final transient StructLikeMap<Integer> groups;

  private IcebergPartitionGroups(
      PartitionKey key, Schema schema, RowType type, PartitionSpec spec, int maxParallelism) {
    this.key = key.copy();
    this.maxParallelism = maxParallelism;
    rows = new RowDataWrapper(type, schema.asStruct());
    groups = StructLikeMap.create(spec.partitionType());
  }

  static KeySelector<RowData, Integer> create(KeySelector<RowData, ?> stock, int maxParallelism) {
    if (stock.getClass() == PartitionKeySelector.class) {
      try {
        var key =
            (PartitionKey)
                IcebergSinkAdapter.field(PartitionKeySelector.class, "partitionKey").get(stock);
        var spec = (PartitionSpec) IcebergSinkAdapter.field(PartitionKey.class, "spec").get(key);
        if (spec.isPartitioned()
            && spec.partitionType().fields().stream()
                .allMatch(field -> field.type().typeId() == Type.TypeID.INTEGER)) {
          var schema =
              (Schema) IcebergSinkAdapter.field(PartitionKeySelector.class, "schema").get(stock);
          var type =
              (RowType)
                  IcebergSinkAdapter.field(PartitionKeySelector.class, "flinkSchema").get(stock);
          return new IcebergPartitionGroups(key, schema, type, spec, maxParallelism);
        }
      } catch (IllegalAccessException failure) {
        throw new IllegalStateException(
            "Cannot inspect released Iceberg partition selector", failure);
      }
    }
    return row -> KeyGroupRangeAssignment.assignToKeyGroup(stock.getKey(row), maxParallelism);
  }

  @Override
  public Integer getKey(RowData row) {
    key.partition(rows.wrap(row));
    Integer group = groups.get(key);
    if (group == null) {
      group = KeyGroupRangeAssignment.assignToKeyGroup(key.toPath(), maxParallelism);
      if (groups.size() == MAX_ENTRIES) groups.clear();
      groups.put(key.copy(), group);
    }
    return group;
  }
}
