package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tech.streamfusion.paimon.PaimonMergeEngineTest.options;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonFieldAggregatorTest {
  @ParameterizedTest
  @ValueSource(strings = {"hll_sketch", "theta_sketch", "rbm32", "rbm64"})
  void unsupportedRetractsPreserveTheReleasedExceptionAndReleaseBuffers(String function)
      throws Exception {
    assertFailureParity(function, DataTypes.BYTES(), new byte[] {1}, new byte[] {2}, true, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"hll_sketch", "theta_sketch", "rbm32", "rbm64"})
  void malformedSketchesPreserveTheReleasedExceptionAndReleaseBuffers(String function)
      throws Exception {
    byte[] malformed = new byte[32];
    java.util.Arrays.fill(malformed, (byte) 0xff);
    assertFailureParity(function, DataTypes.BYTES(), malformed, malformed, false, false);
  }

  @Test
  void aLaterDeleteDoesNotHideAnEarlierAggregateFailure() throws Exception {
    assertFailureParity(
        "hll_sketch", DataTypes.BYTES(), new byte[] {1}, new byte[] {2}, false, true);
  }

  @Test
  void distinctNestedCollectionPreservesTheReleasedNullRowFailure() throws Exception {
    var nested = new RowType(List.of(new DataField(2, "n", DataTypes.INT())));
    assertFailureParity(
        "collect",
        DataTypes.ARRAY(nested),
        new GenericArray(new InternalRow[] {null}),
        new GenericArray(new InternalRow[] {GenericRow.of(1)}),
        false,
        false);
  }

  private static void assertFailureParity(
      String function, DataType valueType, Object a, Object b, boolean retract, boolean delete)
      throws Exception {
    var field = new DataField(1, "v", valueType);
    var type = new RowType(List.of(new DataField(0, "id", DataTypes.INT().notNull()), field));
    Map<String, String> mode =
        options(
            "merge-engine", "aggregation",
            "fields.v.aggregate-function", function,
            "fields.v.distinct", "true",
            "fields.v.ignore-retract", "false",
            "aggregation.remove-record-on-delete", Boolean.toString(delete));
    var table = PaimonMergeEngineTest.table(mode, type);
    assertNull(PaimonMergeOptions.unsupportedReason(table));
    var released = FieldAggregatorFactory.create(valueType, "v", function, table.coreOptions());
    Exception expected =
        assertThrows(
            Exception.class,
            () -> {
              Object accumulator = released.agg(null, a);
              if (retract) released.retract(accumulator, b);
              else released.agg(accumulator, b);
            });
    long sharedBytes = NativeAllocator.SHARED.getAllocatedMemory();
    try (var allocator = new RootAllocator()) {
      try (var buffer =
          new KeyedUpsertBuffer(
              allocator, new int[] {0}, 2, true, false, PaimonMergeOptions.encode(table))) {
        buffer.aggregators(PaimonMergeOptions.aggregators(table));
        var incoming = GenericRow.of(1, b);
        if (retract) incoming.setRowKind(RowKind.DELETE);
        var rows = new ArrayList<InternalRow>(List.of(GenericRow.of(1, a), incoming));
        if (delete) {
          var removed = GenericRow.of(1, (Object) null);
          removed.setRowKind(RowKind.DELETE);
          rows.add(removed);
        }
        buffer.push(
            RowDataArrowConverter.write(
                rows.stream().<RowData>map(FlinkRowData::new).collect(ListCollectors.toList()),
                LogicalTypeConversion.toLogicalType(type),
                allocator,
                true),
            0);
        Exception actual = assertThrows(Exception.class, buffer::flush);
        assertEquals(expected.getClass(), actual.getClass());
        // Generated equaliser class names are deliberately different for each kernel instance.
        if (!function.equals("collect")) assertEquals(expected.getMessage(), actual.getMessage());
        assertEquals(0, buffer.rows());
        assertNull(buffer.flush(), "the JNI thread can be used after the original Java exception");
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
    assertEquals(sharedBytes, NativeAllocator.SHARED.getAllocatedMemory());
  }
}
