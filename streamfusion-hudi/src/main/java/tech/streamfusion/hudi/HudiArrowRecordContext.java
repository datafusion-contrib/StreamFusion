package tech.streamfusion.hudi;

import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.format.FlinkRecordContext;

/** Stable Arrow references for Hudi's existing full-row merge implementations. */
class HudiArrowRecordContext extends FlinkRecordContext {
  HudiArrowRecordContext(HoodieTableConfig table, StorageConfiguration<?> storage) {
    super(table, storage);
  }

  @Override
  public RowData toBinaryRow(HoodieSchema schema, RowData record) {
    if (!(record instanceof HudiAppendIOFactory.Cursor))
      throw new IllegalArgumentException("Hudi merge expected a stable Arrow row view");
    return record;
  }

  @Override
  public RowData seal(RowData record) {
    return record;
  }

  @Override
  public org.apache.hudi.org.apache.avro.generic.GenericRecord convertToAvroRecord(
      RowData record, HoodieSchema schema) {
    throw new UnsupportedOperationException("Hudi Arrow payloads require columnar processing");
  }

  @Override
  public RowData convertAvroRecord(org.apache.hudi.org.apache.avro.generic.IndexedRecord record) {
    throw new UnsupportedOperationException("Hudi Arrow payloads require Parquet logs");
  }

  @Override
  public RowData constructEngineRecord(HoodieSchema schema, Object[] values) {
    throw new UnsupportedOperationException("Hudi Arrow merges require complete payloads");
  }

  @Override
  public RowData mergeWithEngineRecord(
      HoodieSchema schema, Map<Integer, Object> updates, BufferedRecord<RowData> base) {
    throw new UnsupportedOperationException("Partial Hudi updates require the stock path");
  }
}
