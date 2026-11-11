package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;

/** Released dedup harnesses and the production JNI boundary shared by canonical state tests. */
final class DedupCanonicalTestSupport {
  private DedupCanonicalTestSupport() {}

  static long create(
      Path directory,
      boolean rocks,
      int[] keys,
      boolean rowtime,
      boolean first,
      long retention,
      boolean generateUpdateBefore,
      boolean generateInsert) {
    int maximum = keys.length == 0 ? 1 : 128;
    int[] precision =
        keys.length == 0
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1};
    return rocks
        ? Native.createRocksDBKeepLastDeduplicator(
            keys,
            precision,
            2,
            generateUpdateBefore,
            generateInsert,
            rowtime,
            first,
            false,
            false,
            retention,
            10000,
            1 << 24,
            directory.resolve("state").toString(),
            maximum,
            FlinkRocksDBOptions.from(new Configuration()).json(),
            0,
            new String[0],
            new String[0],
            0,
            maximum - 1,
            false,
            new byte[0][])
        : Native.createKeepLastDeduplicator(
            keys,
            precision,
            2,
            generateUpdateBefore,
            generateInsert,
            rowtime,
            first,
            false,
            false,
            retention,
            1 << 24);
  }

  static void close(long handle, boolean rocks) {
    if (rocks) Native.closeRocksDBKeepLastDeduplicator(handle);
    else Native.closeKeepLastDeduplicator(handle);
  }

  static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      RowType type,
      int[] keys,
      String mode,
      long retention,
      boolean stock,
      boolean generateUpdateBefore,
      boolean generateInsert)
      throws Exception {
    OneInputStreamOperator<RowData, RowData> operator;
    if (!stock) operator = new Projection();
    else if (mode.startsWith("row"))
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new RowTimeDeduplicateFunction(
                  InternalTypeInfo.of(type),
                  retention,
                  2,
                  generateUpdateBefore,
                  generateInsert,
                  mode.endsWith("last")));
    else if (mode.endsWith("first"))
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new ProcTimeDeduplicateKeepFirstRowFunction(retention));
    else
      operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              tech.streamfusion.compat.CanonicalTestCompat.keepLast(
                  InternalTypeInfo.of(type),
                  retention,
                  generateUpdateBefore,
                  generateInsert,
                  true,
                  new EqualiserCodeGenerator(type, DedupCanonicalTestSupport.class.getClassLoader())
                      .generateRecordEqualiser("CanonicalDedupEqualiser")));
    var selector =
        KeySelectorUtil.getRowDataSelector(
            DedupCanonicalTestSupport.class.getClassLoader(), keys, InternalTypeInfo.of(type));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), keys.length == 0 ? 1 : 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(type));
    return harness;
  }

  static RowData row(Long key, long value, long time) {
    return GenericRowData.of(key, value, TimestampData.fromEpochMillis(time));
  }

  static List<String> stockPush(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      RowType type,
      List<RowData> rows)
      throws Exception {
    harness.getOutput().clear();
    var serializer = new RowDataSerializer(type);
    for (var row : rows)
      harness.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
    return harness.extractOutputValues().stream()
        .map(DedupCanonicalTestSupport::value)
        .collect(java.util.stream.Collectors.toList());
  }

  static List<String> nativePush(
      long handle,
      boolean rocks,
      RowType type,
      List<RowData> rows,
      long time,
      boolean insertOnlyOutput) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var input = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var inputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, input, inputSchema);
      if (rocks)
        Native.pushRocksDBKeepLastDeduplicator(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            time,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      else
        Native.pushKeepLastDeduplicator(
            handle,
            input.memoryAddress(),
            inputSchema.memoryAddress(),
            time,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {
        if (result.getRowCount() == 0) return List.of();
        var reader = ArrowConversion.createArrowReader(result, type);
        var kinds = (TinyIntVector) result.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        if (insertOnlyOutput) assertNull(kinds);
        else assertNotNull(kinds);
        var values = new ArrayList<String>();
        for (int i = 0; i < result.getRowCount(); i++) {
          var row = reader.read(i);
          row.setRowKind(insertOnlyOutput ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(i)));
          values.add(value(row));
        }
        return values;
      }
    }
  }

  static String value(RowData row) {
    return row.getRowKind().shortString()
        + "|"
        + (row.isNullAt(0) ? "null" : row.getLong(0))
        + "|"
        + row.getLong(1)
        + "|"
        + (row.isNullAt(2) ? "null" : row.getTimestamp(2, 3).getMillisecond());
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
