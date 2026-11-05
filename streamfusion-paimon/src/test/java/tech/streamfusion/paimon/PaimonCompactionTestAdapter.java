package tech.streamfusion.paimon;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

/** Installs the production decorators in unchanged, released upstream fixtures. */
public final class PaimonCompactionTestAdapter {
  private static final Map<Object, Context> WRITES =
      Collections.synchronizedMap(new WeakHashMap<>());

  private PaimonCompactionTestAdapter() {}

  private static final class Context {
    final FileStoreTable table;
    final NativePaimonCompactRewriter.Counters counters =
        new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());

    Context(FileStoreTable table) {
      this.table = table;
    }
  }

  public static void install(Object originalStore, Object originalWrite) throws Exception {
    if (!(originalStore instanceof FileStore) || !(originalWrite instanceof AbstractFileStoreWrite))
      return;
    var store = (FileStore<?>) originalStore;
    var base = KeyValueFileStore.class.getSuperclass();
    var table =
        FileStoreTableFactory.create(
            (FileIO) NativePaimonMaintenance.field(base, "fileIO").get(store),
            store.pathFactory().root(),
            ((TableSchema) NativePaimonMaintenance.field(base, "schema").get(store))
                .copy(store.options().toMap()),
            (CatalogEnvironment)
                NativePaimonMaintenance.field(base, "catalogEnvironment").get(store));
    var context = new Context(table);
    WRITES.put(originalWrite, context);
    PaimonTestCompat.installCompactionFactory(originalWrite, table, context.counters);
  }

  public static void bucket(Object write, Object partition, int bucket, Object container)
      throws Exception {
    var context = WRITES.get(write);
    if (context == null) return;
    NativePaimonStoreSinkWrite.install(
        context.table,
        write,
        ((AbstractFileStoreWrite.WriterContainer<?>) container).writer,
        (BinaryRow) partition,
        bucket,
        context.counters);
  }

  public static Object appendWrite(Object table, Object write) {
    if (write instanceof PaimonAppendCompactWrite || !PaimonVersion.supportsAppendWrite(write))
      return write;
    return PaimonTestCompat.appendWrite(
        (FileStoreTable) table,
        write,
        new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup()));
  }
}
