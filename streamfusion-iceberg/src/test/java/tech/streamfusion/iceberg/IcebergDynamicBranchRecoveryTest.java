package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.TableCreator;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergDynamicBranchRecoveryTest {
  @TempDir Path temporary;

  @Test
  void aConcurrentBranchCreationRefreshesTheReleasedNegativeCache() throws Exception {
    var schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    var spec = PartitionSpec.unpartitioned();
    var identifier = TableIdentifier.of("test", "records");
    try (var catalog = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(),
        temporary.resolve("warehouse").toUri().toString())) {
      catalog.createNamespace(Namespace.of("test"));
      var table = catalog.createTable(identifier, schema, spec);
      var cacheType = Class.forName("org.apache.iceberg.flink.sink.dynamic.TableMetadataCache");
      var constructor = cacheType.getDeclaredConstructor(
          Catalog.class, int.class, long.class, int.class, boolean.class, boolean.class);
      constructor.setAccessible(true);
      Object cache = constructor.newInstance(catalog, 1, 60000L, 10, true, false);
      var branch = cacheType.getDeclaredMethod("branch", TableIdentifier.class, String.class);
      branch.setAccessible(true);
      assertNull(branch.invoke(cache, identifier, "audit"));
      table.manageSnapshots().createBranch("audit").commit();
      var updaterType = Class.forName("org.apache.iceberg.flink.sink.dynamic.TableUpdater");
      var createUpdater = updaterType.getDeclaredConstructor(
          cacheType, Catalog.class, boolean.class, boolean.class);
      createUpdater.setAccessible(true);
      Object updater = createUpdater.newInstance(cache, catalog, true, false);
      var update = updaterType.getDeclaredMethod("update", TableIdentifier.class, String.class,
          Schema.class, PartitionSpec.class, TableCreator.class);
      update.setAccessible(true);
      var failure = assertThrows(InvocationTargetException.class,
          () -> update.invoke(updater, identifier, "audit", schema, spec, TableCreator.DEFAULT));
      var conflict = assertInstanceOf(IllegalArgumentException.class, failure.getCause());
      assertEquals("Ref audit already exists", conflict.getMessage());
      assertTrue(IcebergDynamicBranchRecovery.refresh(updater, identifier, "audit", conflict));
      assertNotNull(update.invoke(updater, identifier, "audit", schema, spec, TableCreator.DEFAULT));
      assertEquals("audit", branch.invoke(cache, identifier, "audit"));
      assertFalse(IcebergDynamicBranchRecovery.refresh(updater, identifier, "other", conflict));
      assertFalse(IcebergDynamicBranchRecovery.refresh(updater, identifier, "audit",
          new IllegalArgumentException("Ref audit already exists")));
      assertFalse(IcebergDynamicBranchRecovery.refresh(updater, identifier, "audit",
          new IllegalArgumentException("invalid schema")));
    }
  }
}
