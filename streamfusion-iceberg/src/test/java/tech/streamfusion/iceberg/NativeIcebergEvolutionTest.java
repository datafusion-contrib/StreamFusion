package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeIcebergEvolutionTest {
  @TempDir Path temp;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fieldIdsSurviveRenamesPromotionsAndAddedNullableColumns(boolean deletes) throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, deletes);
    table.updateSchema().renameColumn("id", "key").renameColumn("payload", "renamed").commit();
    table
        .updateSchema()
        .updateColumn("key", Types.LongType.get())
        .addColumn("added", Types.IntegerType.get())
        .commit();
    var keys = new ArrayList<Long>();
    int addedId = table.schema().findField("added").fieldId();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var reader =
            new IcebergNativeTaskReader(
                task, table.schema(), List.of(addedId, 2, 1), table.io(), 7, 13)) {
          org.apache.arrow.vector.VectorSchemaRoot next;
          while ((next = reader.next()) != null) {
            try (var root = next) {
              for (int row = 0; row < root.getRowCount(); row++) {
                long id = (Long) root.getVector("key").getObject(row);
                keys.add(id);
                assertTrue(root.getVector("added").isNull(row));
                var payload = root.getVector("renamed").getObject(row);
                assertEquals(
                    id % 7 == 0 ? null : "v-é🐍-" + id,
                    payload == null ? null : payload.toString());
              }
            }
          }
        }
      }
    }
    assertEquals(
        java.util.stream.LongStream.range(0, 101)
            .filter(id -> !deletes || (id != 17 && id != 23))
            .skip(13)
            .boxed()
            .toList(),
        keys);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }
}
