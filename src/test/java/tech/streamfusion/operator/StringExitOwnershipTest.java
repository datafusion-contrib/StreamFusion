package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@EnabledIfSystemProperty(named = "streamfusion.experimental.borrowedStringExit", matches = "true")
class StringExitOwnershipTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void stringsRemainOwnedAcrossRowsBatchCloseAndBufferGrowth(boolean reuse, boolean caches)
      throws Exception {
    var schema = RowType.of(new VarCharType(16384), new VarCharType(16384), new IntType());
    String[] first = {"é🙂\u0000", "", null, "x".repeat(8193), caches ? "\ud800" : "tail", "last"};
    String[] second = {"large".repeat(2000), null, "", "other", caches ? "a\udfff" : "text", "終"};
    List<RowData> input = new ArrayList<>();
    for (int i = 0; i < first.length; i++) {
      var row =
          GenericRowData.of(StringData.fromString(first[i]), StringData.fromString(second[i]), i);
      row.setRowKind(RowKind.values()[i % RowKind.values().length]);
      input.add(row);
    }
    try (var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, RowData>(
            new ArrowToRowDataOperator(schema), new ArrowBatchSerializer())) {
      if (reuse) harness.getExecutionConfig().enableObjectReuse();
      harness.setup(new RowDataSerializer(schema));
      harness.open();
      for (int start = 0; start < input.size(); start += 2) {
        var root =
            RowDataArrowConverter.write(
                input.subList(start, start + 2), schema, NativeAllocator.SHARED, true, caches);
        harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
      }
      List<RowData> output = new ArrayList<>();
      for (Object value : harness.getOutput())
        if (value instanceof StreamRecord)
          output.add((RowData) ((StreamRecord<?>) value).getValue());
      assertEquals(input.size(), output.size());
      for (int i = 0; i < output.size(); i++) {
        var row = output.get(i);
        assertEquals(input.get(i).getRowKind(), row.getRowKind());
        assertEquals(i, row.getInt(2));
        for (int field = 0; field < 2; field++) {
          String expected = field == 0 ? first[i] : second[i];
          assertEquals(expected == null, row.isNullAt(field));
          if (expected != null) assertEquals(expected, row.getString(field).toString());
        }
      }
    }
  }
}
