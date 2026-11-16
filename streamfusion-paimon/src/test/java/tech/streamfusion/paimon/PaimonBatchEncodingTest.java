package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.NativeAllocator;

class PaimonBatchEncodingTest {
  @Test
  void copiesReusedRowsAndFlushesBeforeRollingAndClose() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var codec = new RecordingCodec();
    var output = new Output();
    var writer = writer(codec, output);
    var row = GenericRow.of(0);
    for (int i = 0; i < 10_001; i++) {
      row.setField(0, i);
      writer.addElement(row);
      if (i == 2999) assertTrue(writer.reachTargetSize(true, 3000));
    }
    writer.close();
    writer.close();
    assertEquals(10_001, codec.values.size());
    for (int i = 0; i < codec.values.size(); i++) assertEquals(i, codec.values.get(i));
    assertTrue(codec.maxBatch <= 4096);
    assertEquals(1, codec.finishes);
    assertEquals(1, codec.closes);
    assertTrue(writer.wroteNatively());
    assertFalse(output.closed, "Paimon owns the output stream");
    assertThrows(IllegalStateException.class, () -> writer.addElement(row));
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void releasesArrowAndEncoderWhenFinalWriteFails() throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var codec = new RecordingCodec();
    codec.failWrite = true;
    var writer = writer(codec, new Output());
    writer.addElement(GenericRow.of(1));
    assertEquals("write failed", assertThrows(IOException.class, writer::close).getMessage());
    writer.close();
    assertEquals(0, codec.finishes);
    assertEquals(1, codec.closes);
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"open", "finish"})
  void releasesRowsWhenEncoderOpenOrFinishFails(String phase) throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var codec = new RecordingCodec();
    codec.failOpen = phase.equals("open");
    codec.failFinish = phase.equals("finish");
    var writer = writer(codec, new Output());
    writer.addElement(GenericRow.of(1));
    assertEquals(phase + " failed", assertThrows(IOException.class, writer::close).getMessage());
    assertEquals(phase.equals("open") ? 0 : 1, codec.closes);
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void wideRowsFlushBeforeTheRowLimitAndReleaseOversizedVectors() {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    String wide = "x".repeat(1 << 20);
    try (var batch = new PaimonRowBatch(RowType.of(DataTypes.INT(), DataTypes.STRING()))) {
      while (!batch.isFull() && batch.size() < 4096)
        batch.add(
            GenericRow.of(batch.size(), org.apache.paimon.data.BinaryString.fromString(wide)));
      assertTrue(batch.size() < 4096, "Byte pressure must flush a wide batch before the row limit");
      var root = batch.finish();
      assertEquals(wide, root.getVector(1).getObject(batch.size() - 1).toString());
      batch.reset();
      assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
      batch.add(GenericRow.of(7, org.apache.paimon.data.BinaryString.fromString("small")));
      root = batch.finish();
      assertEquals(1, root.getRowCount());
      assertEquals("small", root.getVector(1).getObject(0).toString());
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @Test
  void stringsAreCopiedFromReusedAndSegmentedPaimonStorage() {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    try (var batch = new PaimonRowBatch(RowType.of(DataTypes.STRING()))) {
      byte[] bytes = "--héllo😀--".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      var value = org.apache.paimon.data.BinaryString.fromBytes(bytes, 2, bytes.length - 4);
      batch.add(GenericRow.of(value));
      java.util.Arrays.fill(bytes, (byte) 'x');
      var segments =
          new org.apache.paimon.memory.MemorySegment[] {
            org.apache.paimon.memory.MemorySegment.wrap(
                "_abc".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
            org.apache.paimon.memory.MemorySegment.wrap(
                "def_".getBytes(java.nio.charset.StandardCharsets.UTF_8))
          };
      batch.add(GenericRow.of(org.apache.paimon.data.BinaryString.fromAddress(segments, 1, 6)));
      batch.add(GenericRow.of((Object) null));
      batch.add(GenericRow.of(org.apache.paimon.data.BinaryString.EMPTY_UTF8));
      var root = batch.finish();
      assertEquals(4, root.getRowCount());
      assertEquals("héllo😀", root.getVector(0).getObject(0).toString());
      assertEquals("abcdef", root.getVector(0).getObject(1).toString());
      assertNull(root.getVector(0).getObject(2));
      assertEquals("", root.getVector(0).getObject(3).toString());
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private NativePaimonFileWriter writer(RecordingCodec codec, Output output) {
    return new NativePaimonFileWriter(
            RowType.of(DataTypes.INT()),
            codec,
            new String[0],
            new String[0],
            output,
            "snappy",
            (out, compression) -> {
              throw new AssertionError("stock writer selected");
            })
        .withRowEncoding(true);
  }

  private static final class Output extends PositionOutputStream {
    boolean closed;

    public long getPos() {
      return 0;
    }

    public void write(int value) {}

    public void write(byte[] bytes) {}

    public void write(byte[] bytes, int offset, int length) {}

    public void flush() {}

    public void close() {
      closed = true;
    }
  }

  private static final class RecordingCodec implements ColumnarFileCodec {
    final List<Integer> values = new ArrayList<>();
    int maxBatch, closes, finishes;
    boolean failWrite, failOpen, failFinish;

    public Encoder createEncoder(
        org.apache.arrow.vector.types.pojo.Schema schema,
        int[] partitions,
        String[] keys,
        String[] values,
        boolean changelog,
        java.io.OutputStream output)
        throws IOException {
      if (failOpen) throw new IOException("open failed");
      return new Encoder() {
        public void write(long array, int[] rows, int offset, int count) {
          throw new AssertionError("Expected the Arrow root overload");
        }

        public void write(VectorSchemaRoot root, int[] rows, int offset, int count)
            throws IOException {
          if (failWrite) throw new IOException("write failed");
          maxBatch = Math.max(maxBatch, count);
          for (int i = offset; i < offset + count; i++)
            RecordingCodec.this.values.add((Integer) root.getVector(0).getObject(i));
        }

        public long estimatedBytes() {
          return RecordingCodec.this.values.size();
        }

        public void finish() throws IOException {
          finishes++;
          if (failFinish) throw new IOException("finish failed");
        }

        public void close() {
          closes++;
        }
      };
    }

    public long createDecoder(Object in, long length, long schema, String[] names, int rows) {
      throw new UnsupportedOperationException();
    }

    public boolean next(long handle, long array, long schema) {
      throw new UnsupportedOperationException();
    }

    public long readerMemory(long handle) {
      throw new UnsupportedOperationException();
    }

    public void closeDecoder(long handle) {
      throw new UnsupportedOperationException();
    }
  }
}
