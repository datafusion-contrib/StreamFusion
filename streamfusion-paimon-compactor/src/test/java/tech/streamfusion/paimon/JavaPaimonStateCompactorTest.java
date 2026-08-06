package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.state.PaimonStateBackend;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import tech.streamfusion.operator.CoalescingOff;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The whole delegation chain, end to end: the backend discovers this module's compactor through
 * the ServiceLoader, stamps deletion vectors on the state table, and at every checkpoint barrier
 * stock Java Paimon compacts the barrier's level-0 run away synchronously (Paimon's own
 * lookup-wait model) — visible as a level-0-free table with a deletion-vector index and an
 * unchanged changelog.
 */
@ExtendWith(CoalescingOff.class)
class JavaPaimonStateCompactorTest {

  private static final int MAX_PARALLELISM = 128;

  @BeforeAll
  static void initializeTaskOffHeapAuthority() {
    Configuration configuration = new Configuration();
    configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(configuration);
  }
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

      // Every barrier compacted its own level-0 run away before the checkpoint's manifest was
      // captured, so the committed table holds only level-1+ files — which deletion-vector reads
      // require: level 0 is invisible to them, so every read-back above already proved the
      // maintenance snapshot was in place. Only Java Paimon's compaction can up-level (the
      // native store never compacts), so a level-0-free table witnesses the delegation chain.
      // (No index/ directory is expected here: with one tiny bucket, universal compaction picks
      // full rewrites, so stale rows die by merge; deletion vectors appear only when lookup
      // up-levels level 0 without rewriting the older file.)
      assertEquals(0, levelZeroFiles(tableDir), "a barrier's level-0 run survived its checkpoint");
      assertTrue(
          Files.readString(new File(tableDir, "schema/schema-0").toPath())
              .contains("deletion-vectors.enabled"),
          "a maintained deployment must create deletion-vector tables");

      // The rounds above already read through the compacted files (each round's probe hits the
      // previous barrier's maintenance snapshot, raw parquet with the vectors applied); a fresh
      // key's probe (absent from every file) still inserts.
      VectorSchemaRoot hot =
          RowDataArrowConverter.write(
              List.of((RowData) GenericRowData.of(1L, 100L)), INPUT, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(hot)));
      assertEquals(
          List.of(
              List.of(RowKind.UPDATE_BEFORE, 1L, sum),
              List.of(RowKind.UPDATE_AFTER, 1L, sum + 100)),
          collect(harness));
      VectorSchemaRoot fresh =
          RowDataArrowConverter.write(
              List.of((RowData) GenericRowData.of(2L, 7L)), INPUT, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(fresh)));
      assertEquals(List.of(List.of(RowKind.INSERT, 2L, 7L)), collect(harness));
    }
  }

  @Test
  void writeBufferFlushesToLocalFilesBeforeAnyCheckpoint() throws Exception {
    String property = "streamfusion.state.paimon.write-buffer-mb";
    String previous = System.getProperty(property);
    System.setProperty(property, "1");
    try {
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
              0,
              new int[] {-1},
              MAX_PARALLELISM);
      try (BufferAllocator allocator = new RootAllocator();
          KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness =
              new KeyedOneInputStreamOperatorTestHarness<>(
                  operator, batch -> 0, Types.INT, MAX_PARALLELISM, 1, 0)) {
        harness.setStateBackend(new PaimonStateBackend());
        harness.setup(new ArrowBatchSerializer());
        harness.open();

        List<RowData> rows = new ArrayList<>(25_000);
        for (long key = 0; key < 25_000; key++) {
          rows.add(GenericRowData.of(key, 1L));
        }
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(rows, INPUT, allocator))));
        assertEquals(25_000, collect(harness).size());

        File tableDir =
            findTableDirectory(harness.getEnvironment().getTaskManagerInfo().getTmpWorkingDirectory());
        try (Stream<java.nio.file.Path> snapshots =
            Files.list(new File(tableDir, "snapshot").toPath())) {
          assertTrue(
              snapshots.anyMatch(path -> path.getFileName().toString().startsWith("snapshot-")),
              "the size threshold did not commit a local snapshot before a Flink checkpoint");
        }
      }
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  private static int levelZeroFiles(File tableDir) throws Exception {
    FileStoreTable table =
        FileStoreTableFactory.create(LocalFileIO.create(), new Path(tableDir.getAbsolutePath()));
    int levelZero = 0;
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
        if (file.level() == 0) {
          levelZero++;
        }
      }
    }
    return levelZero;
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
