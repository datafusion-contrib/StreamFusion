package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.config.ExecutionConfigOptions.*;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcerExecutor;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.*;

class DorisConstraintEnforcerTest {
  static final RowType TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(false),
            new VarCharType(3),
            new CharType(4),
            new VarBinaryType(2),
            new BinaryType(3),
            new ArrayType(new IntType())
          },
          new String[] {"id", "v", "c", "vb", "b", "a"});

  static GenericRowData row(Long id, String v, String c, byte[] vb, byte[] b) {
    var row =
        GenericRowData.of(
            id,
            v == null ? null : StringData.fromString(v),
            c == null ? null : StringData.fromString(c),
            vb,
            b,
            new GenericArrayData(new Integer[] {1, null, 3}));
    row.setRowKind(RowKind.UPDATE_AFTER);
    return row;
  }

  static ConstraintEnforcerExecutor executor(NotNullEnforcer nulls, TypeLengthEnforcer length) {
    return ConstraintEnforcerExecutor.create(TYPE, nulls, length, NestedEnforcer.IGNORE)
        .orElseThrow();
  }

  static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness(
      NotNullEnforcer nulls, TypeLengthEnforcer length) throws Exception {
    var executor = executor(nulls, length);
    boolean enforceLength = length != TypeLengthEnforcer.IGNORE;
    var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new NativeSinkConstraintEnforcer(
                TYPE,
                executor::enforce,
                new boolean[] {
                  false, enforceLength, enforceLength, enforceLength, enforceLength, false
                }));
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    return harness;
  }

  @Test
  void dropTrimPadAndRowKindsMatchReleasedExecutor() throws Exception {
    List<RowData> rows =
        List.of(
            row(1L, "😀éabc", "é", new byte[] {1, 2, 3}, new byte[] {9}),
            row(null, "bad", "bad", null, null),
            row(2L, "ab", "longer", new byte[] {4}, new byte[] {1, 2, 3, 4}));
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.DROP, TypeLengthEnforcer.TRIM_PAD)) {
      var root = RowDataArrowConverter.write(rows, TYPE, allocator, true);
      harness.processElement(new StreamRecord<>(new ArrowBatch(root, 17), 123));
      var record = (StreamRecord<?>) harness.getOutput().remove();
      assertEquals(123, record.getTimestamp());
      var batch = (ArrowBatch) record.getValue();
      assertEquals(17, batch.keyGroup());
      try (var output = batch.root()) {
        var actual = RowDataArrowConverter.read(output, TYPE);
        var stock = executor(NotNullEnforcer.DROP, TypeLengthEnforcer.TRIM_PAD);
        int i = 0;
        for (var input : rows) {
          var expected =
              stock.enforce(
                  new org.apache.flink.table.runtime.typeutils.RowDataSerializer(TYPE)
                      .toBinaryRow(input)
                      .copy());
          if (expected == null) continue;
          var result = actual.get(i++);
          assertEquals(RowKind.UPDATE_AFTER, result.getRowKind());
          assertEquals(expected.getLong(0), result.getLong(0));
          assertEquals(
              expected.isNullAt(1) ? null : expected.getString(1),
              result.isNullAt(1) ? null : result.getString(1));
          assertEquals(expected.getString(2), result.getString(2));
          assertArrayEquals(expected.getBinary(3), result.getBinary(3));
          assertArrayEquals(expected.getBinary(4), result.getBinary(4));
          assertTrue(result.getArray(5).isNullAt(1));
        }
        assertEquals(i, actual.size());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void nullableLengthGetterFailuresMatchReleasedFlink() throws Exception {
    var invalid = row(1L, null, "abcd", new byte[] {1}, new byte[] {1, 2, 3});
    var expected =
        assertThrows(
            NullPointerException.class,
            () -> executor(NotNullEnforcer.ERROR, TypeLengthEnforcer.TRIM_PAD).enforce(invalid));
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.ERROR, TypeLengthEnforcer.TRIM_PAD)) {
      var root = RowDataArrowConverter.write(List.of(invalid), TYPE, allocator, true);
      var actual =
          assertThrows(
              NullPointerException.class,
              () -> harness.processElement(new StreamRecord<>(new ArrowBatch(root))));
      assertEquals(expected.getMessage(), actual.getMessage());
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void stringCacheSidecarsFollowTrimmedAndFilteredValues() throws Exception {
    List<RowData> rows =
        List.of(
            row(1L, "long", "x", new byte[] {1}, new byte[] {2}),
            row(null, "skip", "skip", null, null),
            row(2L, "ab", "z", new byte[] {3}, new byte[] {4}));
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.DROP, TypeLengthEnforcer.TRIM_PAD)) {
      var caches = new tech.streamfusion.arrow.StringCacheColumns.Writer(TYPE, allocator);
      for (int i = 0; i < rows.size(); i++) caches.write(rows.get(i), i);
      var base = RowDataArrowConverter.write(rows, TYPE, allocator, true);
      var vectors = new ArrayList<org.apache.arrow.vector.FieldVector>(base.getFieldVectors());
      vectors.addAll(caches.finish(rows.size()));
      var root = new VectorSchemaRoot(vectors);
      root.setRowCount(rows.size());
      harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      try (var output =
          ((ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue()).root()) {
        var actual = RowDataArrowConverter.read(output, TYPE);
        assertEquals("lon", actual.get(0).getString(1).toString());
        assertEquals("x   ", actual.get(0).getString(2).toString());
        assertEquals("ab", actual.get(1).getString(1).toString());
        assertEquals("z   ", actual.get(1).getString(2).toString());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void droppingEveryRowReleasesTheInput() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.DROP, TypeLengthEnforcer.IGNORE)) {
      var root =
          RowDataArrowConverter.write(
              List.of(row(null, "a", "b", null, null)), TYPE, allocator, true);
      harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void validRowsKeepBatchIdentity() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.DROP, TypeLengthEnforcer.TRIM_PAD)) {
      var input =
          new ArrowBatch(
              RowDataArrowConverter.write(
                  List.of(row(1L, "abc", "abcd", new byte[] {1}, new byte[] {1, 2, 3})),
                  TYPE,
                  allocator,
                  true));
      harness.processElement(new StreamRecord<>(input));
      assertSame(input, ((StreamRecord<?>) harness.getOutput().remove()).getValue());
      input.root().close();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void lengthErrorEmitsTheValidPrefixWithOriginalException() throws Exception {
    var first = row(1L, "abc", "abcd", new byte[] {1}, new byte[] {1, 2, 3});
    var invalid = row(2L, "long", "abcd", new byte[] {1}, new byte[] {1, 2, 3});
    var expected =
        assertThrows(
            RuntimeException.class,
            () -> executor(NotNullEnforcer.ERROR, TypeLengthEnforcer.ERROR).enforce(invalid));
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.ERROR, TypeLengthEnforcer.ERROR)) {
      var input =
          new ArrowBatch(
              RowDataArrowConverter.write(List.of(first, invalid), TYPE, allocator, true));
      var actual =
          assertThrows(
              RuntimeException.class, () -> harness.processElement(new StreamRecord<>(input)));
      assertEquals(expected.getClass(), actual.getClass());
      assertEquals(expected.getMessage(), actual.getMessage());
      var prefix = (ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue();
      try (var output = prefix.root()) {
        assertEquals(1, output.getRowCount());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void trimmingSharesUnmodifiedSiblingBuffers() throws Exception {
    try (var allocator = new RootAllocator();
        var harness = harness(NotNullEnforcer.DROP, TypeLengthEnforcer.TRIM_PAD)) {
      var root =
          RowDataArrowConverter.write(
              List.of(row(1L, "long", "x", new byte[] {1}, new byte[] {2})), TYPE, allocator, true);
      long address = root.getVector(0).getDataBuffer().memoryAddress();
      harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      try (var output =
          ((ArrowBatch) ((StreamRecord<?>) harness.getOutput().remove()).getValue()).root()) {
        assertEquals(address, output.getVector(0).getDataBuffer().memoryAddress());
        assertEquals(
            "lon", RowDataArrowConverter.read(output, TYPE).get(0).getString(1).toString());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
