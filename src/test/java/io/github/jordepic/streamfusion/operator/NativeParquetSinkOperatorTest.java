package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeParquetSinkOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  void writesABatchPerFlushAndTheRemainderOnClose() throws Exception {
    Path directory = Files.createTempDirectory("streamfusion-sink");
    NativeParquetSinkOperator operator =
        new NativeParquetSinkOperator(SCHEMA, directory.toString(), 2);
    try (OneInputStreamOperatorTestHarness<RowData, Void> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();
      for (int i = 0; i < 5; i++) {
        harness.processElement(new StreamRecord<>(row(i, i * 10)));
      }
    }

    // Two full batches of two rows flush on the size threshold; the fifth flushes on close.
    File[] files = directory.toFile().listFiles((dir, name) -> name.endsWith(".parquet"));
    assertEquals(3, files.length, "two full batches plus the remainder");
    for (File file : files) {
      assertTrue(file.length() > 0, "parquet file should be non-empty");
    }
  }
}
