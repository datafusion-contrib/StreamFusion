package tech.streamfusion.paimon;

import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;

/** Paimon 2.0 has no writer-construction hook; Flink 1.18 keeps stock maintenance. */
final class PaimonCompactionProvider {
  private PaimonCompactionProvider() {}

  static StoreSinkWrite.Provider wrap(
      FileStoreTable table,
      StoreSinkWrite.Provider stock,
      boolean streaming,
      boolean ignorePreviousFiles,
      boolean materializer) {
    return stock;
  }
}
