/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.streamfusion.paimon;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.localmerge.HashMapLocalMerger;
import org.apache.paimon.mergetree.localmerge.LocalMerger;
import org.apache.paimon.mergetree.localmerge.SortBufferLocalMerger;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.RowKindFilter;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.KeyedUpsertBuffer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Merges updates before shuffle, retaining Java grouping where the intermediate rows are visible.
 */
public final class NativePaimonLocalMergeOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, BoundedOneInput {
  private final FileStoreTable table;
  private final boolean nativeMerge;
  private transient LocalMerger merger;
  private transient KeyedUpsertBuffer nativeMerger;
  private transient long nextSequence;
  private transient Projection keyProjection;
  private transient RowKindGenerator rowKindGenerator;
  private transient RowKindFilter rowKindFilter;
  private transient org.apache.flink.table.types.logical.RowType rowType;
  private transient long watermark;
  private transient boolean ended;

  public NativePaimonLocalMergeOperator(FileStoreTable table) {
    this(table, true);
  }

  NativePaimonLocalMergeOperator(FileStoreTable table, boolean allowNativeMerge) {
    this.table = table;
    this.nativeMerge = allowNativeMerge && usesNativeMerge(table);
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    var schema = table.schema();
    var options = table.coreOptions();
    var valueType = schema.logicalRowType();
    rowType = LogicalTypeConversion.toLogicalType(valueType);
    watermark = Long.MIN_VALUE;
    if (nativeMerge) {
      nativeMerger =
          new KeyedUpsertBuffer(
              NativeAllocator.SHARED,
              schema.projection(schema.primaryKeys()),
              rowType.getFieldCount(),
              true,
              options.ignoreDelete(),
              PaimonMergeOptions.encode(table));
      return;
    }
    keyProjection = CodeGenUtils.newProjection(valueType, schema.projection(schema.primaryKeys()));
    rowKindGenerator = RowKindGenerator.create(schema, options);
    rowKindFilter = RowKindFilter.of(options);
    var function = PrimaryKeyTableUtils.createMergeFunctionFactory(schema).create();
    var sequences = UserDefinedSeqComparator.create(valueType, options);
    var pool = new HeapMemorySegmentPool(options.localMergeBufferSize(), options.pageSize());
    boolean fixedValues =
        valueType.getFields().stream()
            .allMatch(
                field ->
                    schema.primaryKeys().contains(field.name())
                        || BinaryRow.isInFixedLengthPart(field.type()));
    if (fixedValues) {
      merger = new HashMapLocalMerger(valueType, schema.primaryKeys(), pool, function, sequences);
    } else {
      RowType keyType = PrimaryKeyTableUtils.addKeyNamePrefix(schema.logicalPrimaryKeysType());
      var sort =
          new SortBufferWriteBuffer(
              keyType,
              valueType,
              sequences,
              pool,
              false,
              MemorySize.MAX_VALUE,
              options.localSortMaxNumFileHandles(),
              options.spillCompressOptions(),
              null);
      merger = new SortBufferLocalMerger(sort, new KeyComparatorSupplier(keyType).get(), function);
    }
  }

  static boolean usesNativeMerge(FileStoreTable table) {
    var options = table.coreOptions();
    return options.changelogProducer() == CoreOptions.ChangelogProducer.NONE
        && (options.mergeEngine() == CoreOptions.MergeEngine.DEDUPLICATE
            || options.mergeEngine() == CoreOptions.MergeEngine.FIRST_ROW);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    if (nativeMerger != null) {
      VectorSchemaRoot root = element.getValue().root();
      if (root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN) == null) {
        root = NativeKeyValueSinkWrite.withInsertKinds(root);
      }
      nextSequence += nativeMerger.push(root, nextSequence);
      if (nativeMerger.bytes() >= table.coreOptions().localMergeBufferSize()) {
        flush();
      }
      return;
    }
    try (var root = element.getValue().root()) {
      TinyIntVector kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      int index = 0;
      for (InternalRow row : new ArrowBatchBundle(root, rowType)) {
        row.setRowKind(kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(index)));
        index++;
        RowKind kind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (rowKindFilter != null && !rowKindFilter.test(kind)) {
          continue;
        }
        row.setRowKind(RowKind.INSERT);
        BinaryRow key = keyProjection.apply(row);
        if (!merger.put(kind, key, row)) {
          flush();
          if (!merger.put(kind, key, row)) {
            row.setRowKind(kind);
            try (BatchOutput batches = new BatchOutput()) {
              batches.add(row);
              batches.emit();
            }
          }
        }
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) {
    watermark = mark.getTimestamp();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (!ended) {
      flush();
    }
  }

  @Override
  public void endInput() throws Exception {
    ended = true;
    flush();
  }

  private void flush() throws Exception {
    if (nativeMerger != null) {
      if (nativeMerger.rows() == 0) return;
      try (var merged = nativeMerger.flush()) {
        if (merged != null) {
          emitNativeRows(merged.root);
        }
      }
    } else {
      if (merger.isEmpty()) return;
      try (BatchOutput batches = new BatchOutput()) {
        merger.forEach(batches::add);
        batches.emit();
      }
      merger.clear();
    }
    if (watermark != Long.MIN_VALUE) {
      super.processWatermark(new Watermark(watermark));
      watermark = Long.MIN_VALUE;
    }
  }

  private void emitNativeRows(VectorSchemaRoot merged) {
    int keys = table.schema().primaryKeys().size();
    List<FieldVector> columns = new ArrayList<>();
    try {
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        var transfer = merged.getVector(keys + 2 + i).getTransferPair(NativeAllocator.SHARED);
        columns.add((FieldVector) transfer.getTo());
        transfer.transfer();
      }
      var kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
      columns.add(kinds);
      merged.getVector(keys + 1).makeTransferPair(kinds).transfer();
    } catch (Throwable failure) {
      columns.forEach(FieldVector::close);
      throw failure;
    }
    VectorSchemaRoot root = new VectorSchemaRoot(columns);
    root.setRowCount(merged.getRowCount());
    output.collect(new StreamRecord<>(new ArrowBatch(root)));
  }

  private final class BatchOutput implements AutoCloseable {
    private VectorSchemaRoot root;
    private TinyIntVector kinds;
    private ArrowWriter<RowData> writer;
    private int count;

    void add(InternalRow row) {
      if (root == null) {
        root =
            VectorSchemaRoot.create(ArrowConversion.toArrowSchema(rowType), NativeAllocator.SHARED);
        writer = ArrowConversion.createRowDataArrowWriter(root, rowType);
        kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
        kinds.allocateNew(4096);
      }
      writer.write(new FlinkRowData(row));
      kinds.set(count++, row.getRowKind().toByteValue());
      if (count == 4096) {
        emit();
      }
    }

    void emit() {
      if (root == null) {
        return;
      }
      writer.finish();
      kinds.setValueCount(count);
      List<FieldVector> columns = new ArrayList<>(root.getFieldVectors());
      columns.add(kinds);
      VectorSchemaRoot batch = new VectorSchemaRoot(columns);
      root = null;
      kinds = null;
      writer = null;
      count = 0;
      output.collect(new StreamRecord<>(new ArrowBatch(batch)));
    }

    @Override
    public void close() {
      if (root != null) root.close();
      if (kinds != null) kinds.close();
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (nativeMerger != null) nativeMerger.close();
      if (merger != null) merger.clear();
    } finally {
      super.close();
    }
  }
}
