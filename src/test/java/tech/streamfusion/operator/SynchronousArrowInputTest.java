package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SynchronousArrowInputTest {
  private static final RowType SCHEMA = RowType.of(new BigIntType());

  private static Transformation<RowData> source() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    return env.fromSequence(0, 1)
        .map((MapFunction<Long, RowData>) value -> GenericRowData.of(value))
        .returns(InternalTypeInfo.of(SCHEMA))
        .getTransformation();
  }

  @Test
  void borrowingIsConsumerLocalAndDuplicateKeepsItsContract() {
    var source = source();
    var originalType = source.getOutputType();
    var edge = SynchronousArrowInput.forTranspose(source, SCHEMA);
    assertNotSame(source, edge);
    assertSame(originalType, source.getOutputType());
    var inputType = assertInstanceOf(SynchronousArrowInput.InputType.class, edge.getOutputType());
    var borrowed = inputType.createSerializer();
    RowData row = GenericRowData.of(7L);
    assertSame(row, borrowed.copy(row));
    assertSame(row, borrowed.copy(row, GenericRowData.of(0L)));
    assertSame(row, borrowed.duplicate().copy(row));
    var original = ((InternalTypeInfo<RowData>) originalType).toRowSerializer();
    assertNotSame(row, original.copy(row));
    assertEquals(
        originalType,
        SynchronousArrowInput.forTranspose(source, SCHEMA).getInputs().get(0).getOutputType());
  }

  @Test
  void wireBytesMatchAndSnapshotRestoresConservativeCopying() throws Exception {
    var edge = SynchronousArrowInput.forTranspose(source(), SCHEMA);
    var inputType = (SynchronousArrowInput.InputType) edge.getOutputType();
    var borrowed = inputType.createSerializer();
    var original = InternalTypeInfo.of(SCHEMA).toRowSerializer();
    RowData row = GenericRowData.of(7L);
    row.setRowKind(RowKind.UPDATE_BEFORE);
    var standardBytes = new DataOutputSerializer(64);
    var borrowedBytes = new DataOutputSerializer(64);
    original.serialize(row, standardBytes);
    borrowed.serialize(row, borrowedBytes);
    assertArrayEquals(standardBytes.getCopyOfBuffer(), borrowedBytes.getCopyOfBuffer());
    var decoded = borrowed.deserialize(new DataInputDeserializer(standardBytes.getCopyOfBuffer()));
    assertEquals(7L, decoded.getLong(0));
    assertEquals(RowKind.UPDATE_BEFORE, decoded.getRowKind());
    var restored = borrowed.snapshotConfiguration().restoreSerializer();
    assertNotSame(row, restored.copy(row));
    assertEquals(7L, restored.copy(row).getLong(0));
  }

  @Test
  void virtualPartitionAndMismatchedRowsRetainOriginalInput() {
    var source = source();
    var partitioned = new PartitionTransformation<RowData>(source, new RebalancePartitioner<>());
    assertSame(partitioned, SynchronousArrowInput.forTranspose(partitioned, SCHEMA));
    assertSame(source, SynchronousArrowInput.forTranspose(source, RowType.of(new IntType())));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void realForkPreservesReusedArraysAndRetainedSiblingRows(boolean network) throws Exception {
    var schema =
        RowType.of(
            new BigIntType(), new org.apache.flink.table.types.logical.ArrayType(new IntType()));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    if (network) {
      env.disableOperatorChaining();
    }
    var input = env.fromSequence(0, 63).map(new ReusingRows()).returns(InternalTypeInfo.of(schema));
    var edge = SynchronousArrowInput.forTranspose(input.getTransformation(), schema);
    var nativeInput = new org.apache.flink.streaming.api.datastream.DataStream<RowData>(env, edge);
    var nativeRows =
        nativeInput
            .transform(
                "test-row-to-arrow",
                ArrowBatchTypeInformation.INSTANCE,
                new RowDataToArrowOperator(schema, 16, true, schema))
            .transform(
                "test-arrow-to-row",
                InternalTypeInfo.of(schema),
                new ArrowToRowDataOperator(schema));
    var nativeResults =
        nativeRows.map(
            (MapFunction<RowData, String>)
                row -> "n:" + row.getLong(0) + ":" + row.getArray(1).getInt(0));
    var siblingResults = input.map(new RetainingSibling());
    var actual = new java.util.ArrayList<String>();
    try (var results = nativeResults.union(siblingResults).executeAndCollect()) {
      results.forEachRemaining(actual::add);
    }
    var expected = new java.util.ArrayList<String>();
    for (int i = 0; i < 64; i++) {
      expected.add("n:" + i + ":" + i);
      expected.add("h:" + i + ":0:0");
    }
    java.util.Collections.sort(actual);
    java.util.Collections.sort(expected);
    assertEquals(expected, actual);
  }

  private static final class ReusingRows implements MapFunction<Long, RowData> {
    private transient GenericRowData row;
    private transient int[] array;

    @Override
    public RowData map(Long value) {
      if (row == null) {
        array = new int[1];
        row = GenericRowData.of(0L, new org.apache.flink.table.data.GenericArrayData(array));
      }
      row.setField(0, value);
      array[0] = value.intValue();
      return row;
    }
  }

  private static final class RetainingSibling implements MapFunction<RowData, String> {
    private transient RowData first;

    @Override
    public String map(RowData row) {
      if (first == null) {
        first = row;
      }
      return "h:" + row.getLong(0) + ":" + first.getLong(0) + ":" + first.getArray(1).getInt(0);
    }
  }
}
