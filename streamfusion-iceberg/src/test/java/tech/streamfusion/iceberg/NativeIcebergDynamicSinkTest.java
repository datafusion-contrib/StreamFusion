package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;

class NativeIcebergDynamicSinkTest {
  @TempDir Path temp;

  @ParameterizedTest
  @CsvSource({
    "false,2,false,false",
    "true,2,false,false",
    "false,2,true,false",
    "true,2,true,false",
    "false,3,true,false",
    "true,3,true,false",
    "false,2,false,true",
    "true,2,false,true"
  })
  void forwardAndShuffleKeepArrowThroughHostCreationAndCommit(
      boolean immediate, int format, boolean keyed, boolean stockTarget) throws Exception {
    var hadoop = new org.apache.hadoop.conf.Configuration();
    String warehouse = temp.resolve("warehouse").toUri().toString();
    var schema =
        new Schema(
            java.util.List.of(
                Types.NestedField.required(keyed ? 1 : 17, "id", Types.IntegerType.get())),
            keyed ? Set.of(1) : Set.of());
    var spec = PartitionSpec.builderFor(schema).bucket("id", 4).build();
    try (var catalog = new HadoopCatalog(hadoop, warehouse)) {
      catalog.createNamespace(Namespace.of("test"));
      if (keyed || stockTarget)
        for (String name : new String[] {"forward", "shuffled"}) {
          var properties = new java.util.HashMap<String, String>();
          properties.put("format-version", Integer.toString(format));
          if (stockTarget && name.equals("shuffled"))
            properties.put(
                org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1");
          catalog.createTable(TableIdentifier.of("test", name), schema, spec, properties);
        }
      var config =
          Configuration.fromMap(
              Map.of(
                  "restart-strategy.type", "none", "taskmanager.memory.task.off-heap.size", "1gb"));
      var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
      env.setParallelism(2);
      env.setMaxParallelism(16);
      env.enableCheckpointing(100);
      var input =
          env.fromSequence(0, 7)
              .map(
                  index -> {
                    var id = new IntVector("id", NativeAllocator.SHARED);
                    id.allocateNew(251);
                    for (int row = 0; row < 251; row++)
                      id.set(row, keyed ? row : Math.toIntExact(index * 251 + row));
                    id.setValueCount(251);
                    return new ArrowBatch(VectorSchemaRoot.of(id));
                  })
              .returns(ArrowBatchTypeInformation.INSTANCE);
      org.junit.jupiter.api.function.Executable append =
          () ->
              NativeIcebergDynamicSink.append(
                  input,
                  schema,
                  row -> {
                    boolean forward = row.getInt(0) % 2 == 0;
                    return new NativeIcebergDynamicSink.Destination(
                        forward ? "test.forward" : "test.shuffled",
                        keyed && !forward ? "audit" : "main",
                        spec,
                        forward ? null : DistributionMode.HASH,
                        2,
                        keyed,
                        keyed ? Set.of("id") : Set.of());
                  },
                  CatalogLoader.hadoop("test", hadoop, Map.of("warehouse", warehouse)),
                  config,
                  Map.of(
                      "dynamic-sink.immediate-table-update",
                      Boolean.toString(immediate),
                      "dynamic-sink.cache-max-size",
                      "1",
                      "target-file-size-bytes",
                      "1"),
                  "native-dynamic");
      if (keyed && !immediate) {
        assertTrue(
            assertThrows(IllegalArgumentException.class, append)
                .getMessage()
                .contains("immediate table updates"));
        return;
      }
      assertDoesNotThrow(append);
      env.execute("native-dynamic-forward-and-shuffle");
      var seen = new HashSet<Integer>();
      for (String name : new String[] {"forward", "shuffled"}) {
        var table = catalog.loadTable(TableIdentifier.of("test", name));
        String branch = keyed && name.equals("shuffled") ? "audit" : "main";
        try (var rows =
            IcebergGenerics.read(table).useSnapshot(table.snapshot(branch).snapshotId()).build()) {
          for (var row : rows) {
            int id = (Integer) row.getField("id");
            assertEquals(name.equals("forward"), id % 2 == 0);
            assertTrue(seen.add(id));
          }
        }
        try (var files = table.newScan().useRef(branch).planFiles()) {
          for (var task : files) {
            try (var reader =
                org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
                    org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile
                        .fromPath(new org.apache.hadoop.fs.Path(task.file().location()), hadoop))) {
              assertTrue(
                  reader
                      .getFooter()
                      .getFileMetaData()
                      .getCreatedBy()
                      .contains(
                          stockTarget && name.equals("shuffled") ? "parquet-mr" : "parquet-rs"));
            }
          }
        }
      }
      assertEquals(keyed ? 251 : 2008, seen.size());
      assertEquals("", NativeIceberg.liveNativeHandles());
    }
  }
}
