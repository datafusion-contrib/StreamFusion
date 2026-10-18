package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Set;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergDynamicGeneratorTest {
  @Test
  void identifierReuseDoesNotRetainPayloadOrStaleDestinationMetadata() throws Exception {
    var schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    var bucket = PartitionSpec.builderFor(schema).bucket("id", 4).build();
    var output = new ArrayList<DynamicRecord>();
    var collector =
        new Collector<DynamicRecord>() {
          public void collect(DynamicRecord record) {
            output.add(record);
          }

          public void close() {}
        };
    var generator =
        NativeIcebergDynamicSink.generator(
            schema,
            row -> {
              int id = row.getInt(0);
              return new NativeIcebergDynamicSink.Destination(
                  "bench.t" + id / 2,
                  id % 2 == 0 ? "main" : "audit",
                  id % 2 == 0 ? bucket : PartitionSpec.unpartitioned(),
                  id % 2 == 0 ? DistributionMode.HASH : null,
                  id % 2 + 1,
                  id % 2 != 0,
                  id % 2 == 0 ? Set.of() : Set.of("id"));
            },
            true);
    for (int i = 0; i < 4106; i++) generator.generate(GenericRowData.of(i), collector);
    for (int i = 0; i < output.size(); i++) {
      var record = output.get(i);
      assertEquals("bench.t" + i / 2, record.tableIdentifier().toString());
      assertEquals(i, record.rowData().getInt(0));
      assertEquals(i % 2 == 0 ? "main" : "audit", record.branch());
      assertEquals(i % 2 == 0 ? bucket : PartitionSpec.unpartitioned(), record.spec());
      assertEquals(i % 2 == 0 ? DistributionMode.HASH : null, record.distributionMode());
      assertEquals(i % 2 != 0, record.upsertMode());
      assertEquals(i % 2 == 0 ? Set.of() : Set.of("id"), record.equalityFields());
      if (i % 2 != 0) assertSame(output.get(i - 1).tableIdentifier(), record.tableIdentifier());
    }
    generator.generate(GenericRowData.of(0), collector);
    assertEquals(output.get(0).tableIdentifier(), output.get(output.size() - 1).tableIdentifier());
    assertNotSame(output.get(0).tableIdentifier(), output.get(output.size() - 1).tableIdentifier());
  }
}
