package tech.streamfusion.paimon;

import java.lang.reflect.Field;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;

/** Installs the production decorator in unchanged, pinned upstream file-store fixtures. */
public final class PaimonCompactionTestAdapter {
  private PaimonCompactionTestAdapter() {}

  public static void install(Object originalStore, Object originalWrite) throws Exception {
    if (!(originalWrite instanceof KeyValueFileStoreWrite)) return;
    var store = (KeyValueFileStore) originalStore;
    var base = KeyValueFileStore.class.getSuperclass();
    var table =
        new PrimaryKeyFileStoreTable(
            (FileIO) field(base, "fileIO").get(store),
            store.pathFactory().root(),
            ((TableSchema) field(base, "schema").get(store)).copy(store.options().toMap()),
            (CatalogEnvironment) field(base, "catalogEnvironment").get(store));
    var counters = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
    ((KeyValueFileStoreWrite) originalWrite)
        .withCompactRewriterFactory(
            (partition, bucket, stock) ->
                new NativePaimonCompactRewriter(table, partition, bucket, stock, counters));
  }

  private static Field field(Class<?> type, String name) throws ReflectiveOperationException {
    var field = type.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
