package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergPartitionGroupsTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 128, 8192})
  void nullableBucketsIntegerTuplesAndCacheEvictionPreserveHostGroups(int max) throws Exception {
    var schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "text", Types.StringType.get()));
    for (var spec :
        java.util.List.of(
            PartitionSpec.builderFor(schema).bucket("id", 4).build(),
            PartitionSpec.builderFor(schema).identity("id").bucket("text", 8).build(),
            PartitionSpec.builderFor(schema).identity("text").build())) {
      var stock = new PartitionKeySelector(spec, schema, FlinkSchemaUtil.convert(schema));
      var cached = IcebergHashRouter.grouping(stock, max);
      for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < 2053; i++) {
          var row =
              GenericRowData.of(
                  i % 11 == 0 ? null : i - 1024,
                  i % 7 == 0 ? null : StringData.fromString("é /%= " + i % 19));
          assertEquals(
              KeyGroupRangeAssignment.assignToKeyGroup(stock.getKey(row), max), cached.getKey(row));
        }
      }
    }
  }
}
