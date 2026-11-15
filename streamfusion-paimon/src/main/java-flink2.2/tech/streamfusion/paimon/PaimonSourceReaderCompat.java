package tech.streamfusion.paimon;

import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeSourceRecord;

/** Keeps the released reader lifecycle while tolerating concurrent fetcher retirement. */
abstract class PaimonSourceReaderCompat
    extends SingleThreadMultiplexSourceReaderBase<
        NativeSourceRecord, ArrowBatch, FileStoreSourceSplit, PaimonSourceSplitState> {
  protected PaimonSourceReaderCompat(
      Supplier<SplitReader<NativeSourceRecord, FileStoreSourceSplit>> supplier,
      RecordEmitter<NativeSourceRecord, ArrowBatch, PaimonSourceSplitState> emitter,
      Configuration config,
      SourceReaderContext context) {
    super(new Fetcher(supplier, config), emitter, config, context);
  }

  static class Fetcher
      extends SingleThreadFetcherManager<NativeSourceRecord, FileStoreSourceSplit> {
    Fetcher(
        Supplier<SplitReader<NativeSourceRecord, FileStoreSourceSplit>> supplier,
        Configuration config) {
      super(supplier, config);
    }

    @Override
    protected SplitFetcher<NativeSourceRecord, FileStoreSourceSplit> getRunningFetcher() {
      // The I/O thread can remove the last entry independently of the mailbox thread.
      Iterator<SplitFetcher<NativeSourceRecord, FileStoreSourceSplit>> iterator =
          fetchers.values().iterator();
      return iterator.hasNext() ? iterator.next() : null;
    }
  }
}
