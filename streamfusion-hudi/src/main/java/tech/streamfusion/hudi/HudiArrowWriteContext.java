package tech.streamfusion.hudi;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;

/** Hudi's ingestion context with an Arrow-preserving record boundary. */
final class HudiArrowWriteContext extends FlinkRowDataReaderContext {
  HudiArrowWriteContext(HoodieTableMetaClient meta) {
    super(
        meta.getStorageConf(),
        () -> InternalSchemaManager.DISABLED,
        java.util.List.of(),
        meta.getTableConfig(),
        Option.empty());
    recordContext = new HudiArrowRecordContext(meta.getTableConfig(), meta.getStorageConf());
  }
}
