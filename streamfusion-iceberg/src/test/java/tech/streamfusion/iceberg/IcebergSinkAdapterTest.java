package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;

class IcebergSinkAdapterTest {
  @TempDir Path temp;
  private static final List<Integer> ACCEPTED = new CopyOnWriteArrayList<>();
  private static final List<Integer> SIZE_CHECKS = new CopyOnWriteArrayList<>();

  @Test
  void releasedSinkCommitsFakeAppendersAndRollsWithinAnArrowBatch() throws Exception {
    ACCEPTED.clear();
    SIZE_CHECKS.clear();
    String location = temp.resolve("table").toUri().toString();
    Table table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                location);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.configure(Configuration.fromMap(Map.of("restart-strategy.type", "none")));
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.setParallelism(1);
    env.enableCheckpointing(1000);
    var batches =
        env.fromSequence(0, 0)
            .map(ignored -> batch(2001))
            .returns(ArrowBatchTypeInformation.INSTANCE);
    IcebergSinkAdapter.append(
        batches,
        table,
        TableLoader.fromHadoopTable(location),
        new Configuration(),
        Map.of("distribution-mode", "none", "target-file-size-bytes", "1"),
        (stock, context) -> new FakeFactory(context));
    env.execute("released-iceberg-appender-hook");
    table.refresh();
    assertNotNull(table.currentSnapshot(), "stock committer must publish the fake data files");
    List<DataFile> files = new ArrayList<>();
    try (var scan = table.newScan().planFiles()) {
      for (var task : scan) files.add(task.file());
    }
    assertEquals(
        List.of(1L, 1000L, 1000L), files.stream().map(DataFile::recordCount).sorted().toList());
    assertEquals(java.util.stream.IntStream.range(0, 2001).boxed().toList(), ACCEPTED);
    assertEquals(
        List.of(999, 1999),
        SIZE_CHECKS,
        "the host must check only accepted prefixes at its normal 1000-row cadence");
  }

  private static ArrowBatch batch(int count) {
    IntVector vector = new IntVector("id", NativeAllocator.SHARED);
    vector.allocateNew(count);
    for (int i = 0; i < count; i++) vector.set(i, i);
    vector.setValueCount(count);
    return new ArrowBatch(VectorSchemaRoot.of(vector));
  }

  private static final class FakeFactory implements FileWriterFactory<RowData> {
    private final ArrowWriteContext context;

    FakeFactory(ArrowWriteContext context) {
      this.context = context;
    }

    @Override
    public DataWriter<RowData> newDataWriter(
        EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
      return new DataWriter<>(
          new FakeAppender(file.encryptingOutputFile().create(), context),
          FileFormat.PARQUET,
          file.encryptingOutputFile().location(),
          spec,
          partition,
          file.keyMetadata());
    }

    @Override
    public EqualityDeleteWriter<RowData> newEqualityDeleteWriter(
        EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
      throw new AssertionError("append-only sink must not create equality deletes");
    }

    @Override
    public PositionDeleteWriter<RowData> newPositionDeleteWriter(
        EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
      throw new AssertionError("append-only sink must not create position deletes");
    }
  }

  private static final class FakeAppender implements FileAppender<RowData> {
    private final PositionOutputStream out;
    private final ArrowWriteContext context;
    private long rows;
    private boolean closed;

    FakeAppender(PositionOutputStream out, ArrowWriteContext context) {
      this.out = out;
      this.context = context;
    }

    @Override
    public void add(RowData row) {
      lastRow = row;
      int position = context.position(row);
      assertEquals(position, row.getInt(0));
      assertEquals(position, ((IntVector) context.root().getVector(0)).get(position));
      ACCEPTED.add(position);
      rows++;
      try {
        out.write(1);
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }

    @Override
    public long length() {
      if (!closed) SIZE_CHECKS.add(context.position(lastRow));
      return rows;
    }

    private RowData lastRow;

    @Override
    public Metrics metrics() {
      return new Metrics(rows, null, null, null, null);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      out.close();
    }
  }
}
