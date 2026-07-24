package io.github.jordepic.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchSerializer;
import io.github.jordepic.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import io.github.jordepic.streamfusion.state.PaimonStateBackend;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Test;

/**
 * The whole delegation chain, end to end: the backend discovers this module's compactor through
 * the ServiceLoader and at checkpoint barriers stock Java Paimon maintains the state table —
 * visible as a bounded run count and an unchanged changelog.
 */
class JavaPaimonStateCompactorTest {

  private static final int MAX_PARALLELISM = 128;
  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "v"});
  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key0", "result0"});

  @Test
  void barrierMaintenanceRunsThroughTheServiceLoader() throws Exception {
    NativeColumnarGroupAggregateOperator operator =
        new NativeColumnarGroupAggregateOperator(
            new int[] {0},
            new int[] {0},
            new int[] {1},
            new int[] {0},
            new int[] {-1},
            new int[] {-1},
            new int[] {-1},
            -1,
            true,
            false,
            0,
            new int[] {-1},
            MAX_PARALLELISM);
    File tableDir;
    try (BufferAllocator allocator = new RootAllocator();
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0)) {
      harness.setStateBackend(new PaimonStateBackend());
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      long sum = 0;
      for (int round = 1; round <= 8; round++) {
        VectorSchemaRoot batch =
            RowDataArrowConverter.write(
                List.of((RowData) GenericRowData.of(1L, (long) round)), INPUT, allocator);
        harness.processElement(new StreamRecord<>(new ArrowBatch(batch)));
        List<List<Object>> out = collect(harness);
        if (round == 1) {
          assertEquals(List.of(List.of(RowKind.INSERT, 1L, 1L)), out);
        } else {
          assertEquals(
              List.of(
                  List.of(RowKind.UPDATE_BEFORE, 1L, sum),
                  List.of(RowKind.UPDATE_AFTER, 1L, sum + round)),
              out);
        }
        sum += round;
        harness.snapshot(round, round);
        harness.notifyOfCompletedCheckpoint(round);
      }

      tableDir = findTableDirectory(harness.getEnvironment().getTaskManagerInfo().getTmpWorkingDirectory());

      // Eight barriers wrote eight level-0 runs into one bucket. Only Java Paimon's maintenance
      // could have merged any of them (the native store never compacts), so a bounded run count
      // is the witness that the whole delegation chain ran.
      // (The maintenance snapshot document itself is expired by the store's local GC, so the
      // commit user cannot serve as the witness.)
      FileStoreTable table =
          FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDir.getAbsolutePath()));
      for (Split split : table.newReadBuilder().newScan().plan().splits()) {
        int files = ((DataSplit) split).dataFiles().size();
        assertTrue(
            files <= 5,
            "expected Paimon maintenance to bound the bucket's runs (trigger 5), saw " + files);
      }
    }
  }

  private static File findTableDirectory(File tmpWorkingDirectory) throws Exception {
    try (Stream<java.nio.file.Path> walk =
        Files.walk(new File(tmpWorkingDirectory, "paimon-state").toPath())) {
      Optional<java.nio.file.Path> table =
          walk.filter(p -> p.getFileName().toString().equals("table"))
              .filter(p -> p.toFile().isDirectory())
              .findFirst();
      return table.orElseThrow(() -> new AssertionError("no state table directory")).toFile();
    }
  }

  private static List<List<Object>> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness) {
    List<List<Object>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, OUTPUT)) {
            rows.add(List.of(row.getRowKind(), row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    return rows;
  }
}
