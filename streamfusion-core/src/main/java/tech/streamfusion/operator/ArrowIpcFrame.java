package tech.streamfusion.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * The length-prefixed Arrow IPC framing every batch-carrying record type uses to cross a network
 * edge. Writing is the batch's terminal use on the sending side, so it releases the root; reading
 * transfers the buffers out of the IPC reader into a fresh root the receiver owns.
 */
final class ArrowIpcFrame {

  private ArrowIpcFrame() {}

  static void write(VectorSchemaRoot root, DataOutputView target) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } finally {
      root.close();
    }
    byte[] encoded = bytes.toByteArray();
    target.writeInt(encoded.length);
    target.write(encoded);
  }

  static VectorSchemaRoot read(DataInputView source, BufferAllocator allocator)
      throws IOException {
    int length = source.readInt();
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(encoded), allocator)) {
      reader.loadNextBatch();
      VectorSchemaRoot read = reader.getVectorSchemaRoot();
      List<FieldVector> transferred = new ArrayList<>();
      for (FieldVector vector : read.getFieldVectors()) {
        TransferPair pair = vector.getTransferPair(allocator);
        pair.transfer();
        transferred.add((FieldVector) pair.getTo());
      }
      VectorSchemaRoot root = new VectorSchemaRoot(transferred);
      root.setRowCount(read.getRowCount());
      return root;
    }
  }

  static void copy(DataInputView source, DataOutputView target) throws IOException {
    int length = source.readInt();
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    target.writeInt(length);
    target.write(encoded);
  }
}
