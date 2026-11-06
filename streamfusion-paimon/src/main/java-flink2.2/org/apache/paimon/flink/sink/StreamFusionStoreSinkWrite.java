package org.apache.paimon.flink.sink;

import java.util.function.BiFunction;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;

/** Accesses the released 2.1 constructor without changing Paimon's writer lifecycle. */
public final class StreamFusionStoreSinkWrite {
  private StreamFusionStoreSinkWrite() {}

  public static boolean available() {
    return StreamFusionStoreSinkWrite.class.getClassLoader()
        == StoreSinkWriteImpl.class.getClassLoader();
  }

  public static StoreSinkWriteImpl create(
      FileStoreTable table,
      String commitUser,
      StoreSinkWriteState state,
      IOManager ioManager,
      boolean ignorePreviousFiles,
      boolean waitCompaction,
      boolean streaming,
      MemoryPoolFactory memoryPoolFactory,
      MetricGroup metrics,
      BiFunction<FileStoreTable, TableWriteImpl<?>, TableWriteImpl<?>> decorate) {
    return new StoreSinkWriteImpl(
        table,
        commitUser,
        state,
        ioManager,
        ignorePreviousFiles,
        waitCompaction,
        streaming,
        memoryPoolFactory,
        metrics,
        (fresh, user, writeId) -> decorate.apply(fresh, fresh.newWrite(user, writeId)));
  }
}
