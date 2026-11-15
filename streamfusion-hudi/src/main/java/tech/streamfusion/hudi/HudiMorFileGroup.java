package tech.streamfusion.hudi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Streams Hudi's merged row references into Arrow batches on the original split fetcher. */
final class HudiMorFileGroup implements AutoCloseable {
  private final RowType output;
  private final int rows;
  private final boolean streaming;
  private final HudiArrowReadContext context;
  private final HoodieFileGroupReader<RowData> reader;
  private final ClosableIterator<RowData> records;
  private boolean closed;

  HudiMorFileGroup(
      Configuration conf,
      HoodieSourceSplit split,
      String schema,
      RowType output,
      Path temporary,
      int rows)
      throws IOException {
    this.output = output;
    this.rows = rows;
    streaming = conf.get(FlinkOptions.READ_AS_STREAMING);
    var meta = StreamerUtil.metaClientForReader(conf, HadoopConfigurations.getHadoopConf(conf));
    var requested = HoodieSchemaConverter.convertToSchema((RowType) output.copy(false));
    context =
        new HudiArrowReadContext(
            meta,
            split.getInstantRange(),
            rows,
            StreamerUtil.getMaxCompactionMemoryInBytes(conf),
            split.getBasePath().orElse(null),
            requested);
    HoodieFileGroupReader<RowData> opened = null;
    try {
      var props =
          FlinkClientUtil.getReadProps(
              meta.getTableConfig(), FlinkWriteClients.getHoodieClientConfig(conf));
      props.put(HoodieReaderConfig.MERGE_TYPE.key(), conf.get(FlinkOptions.MERGE_TYPE));
      props.put(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), temporary.toString());
      opened =
          HoodieFileGroupReader.<RowData>builder()
              .withReaderContext(context)
              .withHoodieTableMetaClient(meta)
              .withLatestCommitTime(split.getLatestCommit())
              .withBaseFileOption(split.getBasePath().map(HoodieBaseFile::new))
              .withLogFiles(
                  split.getLogPaths().orElse(List.of()).stream()
                      .map(p -> new HoodieLogFile(new StoragePath(p))))
              .withPartitionPath(split.getPartitionPath())
              .withDataSchema(HoodieSchema.parse(schema))
              .withRequestedSchema(requested)
              .withInternalSchemaOpt(Option.empty())
              .withProps(props)
              .withShouldUseRecordPosition(false)
              .withEmitDelete(streaming)
              .build();
      records = opened.getClosableIterator();
      reader = opened;
    } catch (IOException | RuntimeException | Error failure) {
      try {
        org.apache.flink.util.IOUtils.closeAll(opened, context);
      } catch (Exception closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
  }

  VectorSchemaRoot next() throws IOException {
    if (closed) throw new IOException("Hudi file group is closed");
    var sources = new IntArrayList(rows);
    var positions = new IntArrayList(rows);
    var kinds = new IntArrayList(rows);
    List<RowData> deletedKeys = new ArrayList<>();
    while (sources.size() < rows && records.hasNext()) {
      var payload = records.next();
      if (payload instanceof HudiAppendIOFactory.Cursor) {
        var cursor = (HudiAppendIOFactory.Cursor) payload;
        sources.add(context.location(cursor));
        positions.add(cursor.rowId);
      } else {
        if (payload == null
            || (payload.getRowKind() != RowKind.DELETE
                && payload.getRowKind() != RowKind.UPDATE_BEFORE))
          throw new IOException("Hudi merge produced a payload outside the Arrow view");
        sources.add(-1);
        positions.add(deletedKeys.size());
        deletedKeys.add(payload);
      }
      kinds.add(streaming ? payload.getRowKind().toByteValue() : RowKind.INSERT.toByteValue());
    }
    if (sources.isEmpty()) return null;
    int firstDeleteBatch = context.inputs.size();
    addDeleteKeys(deletedKeys);
    int[] selectedSources = sources.toArray();
    int[] selectedRows = positions.toArray();
    for (int i = 0; i < selectedSources.length; i++) {
      if (selectedSources[i] != -1) continue;
      selectedSources[i] = firstDeleteBatch;
    }
    var selected =
        select(context.inputs, selectedSources, selectedRows, kinds.toArray(), streaming);
    try {
      context.selectionEmitted();
      return selected;
    } catch (Throwable failure) {
      selected.close();
      throw failure;
    }
  }

  private void addDeleteKeys(List<RowData> keys) {
    if (keys.isEmpty()) return;
    var nullable =
        new RowType(
            output.getFields().stream()
                .map(field -> new RowType.RowField(field.getName(), field.getType().copy(true)))
                .collect(java.util.stream.Collectors.toList()));
    context.retain(RowDataArrowConverter.write(keys, nullable, NativeAllocator.SHARED), nullable);
  }

  private VectorSchemaRoot select(
      List<HudiArrowReadContext.Input> inputs,
      int[] sources,
      int[] positions,
      int[] kinds,
      boolean streaming) {
    List<ArrowArray> arrays = new ArrayList<>();
    List<ArrowSchema> schemas = new ArrayList<>();
    try {
      var needed = new java.util.LinkedHashMap<Integer, Integer>();
      java.util.function.Function<Integer, Integer> nextSource = ignored -> needed.size();
      for (int i = 0; i < sources.length; i++)
        sources[i] = needed.computeIfAbsent(sources[i], nextSource);
      long[] arrayAddresses = new long[needed.size()];
      long[] schemaAddresses = new long[needed.size()];
      for (var selection : needed.entrySet()) {
        int i = selection.getValue();
        var input = inputs.get(selection.getKey());
        var vectors =
            output.getFieldNames().stream()
                .map(input.root::getVector)
                .collect(java.util.stream.Collectors.toList());
        var projected =
            new VectorSchemaRoot(
                ArrowConversion.toArrowSchema(output), vectors, input.root.getRowCount());
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        arrays.add(array);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        schemas.add(schema);
        Data.exportVectorSchemaRoot(
            NativeAllocator.SHARED, projected, NativeAllocator.DICTIONARIES, array, schema);
        arrayAddresses[i] = array.memoryAddress();
        schemaAddresses[i] = schema.memoryAddress();
      }
      var selected =
          ColumnarFileCodec.importBatch(
              (array, schema) -> {
                Native.interleaveBatches(
                    arrayAddresses, schemaAddresses, sources, positions, array, schema);
                return true;
              });
      if (!streaming) return selected;
      var kind = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
      try {
        kind.allocateNew(kinds.length);
        for (int row = 0; row < kinds.length; row++) kind.setSafe(row, kinds[row]);
        kind.setValueCount(kinds.length);
        var vectors = new ArrayList<>(selected.getFieldVectors());
        vectors.add(kind);
        // Ownership of the selected payload vectors passes to this root.
        return new VectorSchemaRoot(
            vectors.stream().map(v -> v.getField()).collect(java.util.stream.Collectors.toList()),
            vectors,
            kinds.length);
      } catch (Throwable failure) {
        kind.close();
        selected.close();
        throw failure;
      }
    } finally {
      for (var array : arrays) {
        if (array.snapshot().release != 0) array.release();
        array.close();
      }
      for (var schema : schemas) {
        if (schema.snapshot().release != 0) schema.release();
        schema.close();
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (closed) return;
    closed = true;
    org.apache.flink.util.IOUtils.closeAll(reader, context);
  }
}
