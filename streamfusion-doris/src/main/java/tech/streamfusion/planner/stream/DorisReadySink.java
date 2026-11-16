package tech.streamfusion.planner.stream;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.batch.DorisBatchStreamLoad;
import org.apache.doris.flink.sink.batch.DorisBatchWriter;
import org.apache.doris.flink.sink.batch.DorisBatchWriterAdapter;
import org.apache.doris.flink.sink.writer.DorisAbstractWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import tech.streamfusion.operator.ArrowBatch;

/** Waits for the released batch loader before a short job can flush its first batch. */
final class DorisReadySink extends DorisSink<ArrowBatch> {
  private static final long serialVersionUID = 1L;

  DorisReadySink(DorisOptions options, DorisReadOptions reads, DorisExecutionOptions execution,
      DorisRecordSerializer<ArrowBatch> serializer) {
    super(options, reads, execution, serializer);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public DorisAbstractWriter createWriter(WriterInitContext context) throws IOException {
    return ready(super.createWriter(context));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public DorisAbstractWriter restoreWriter(WriterInitContext context,
      Collection<DorisWriterState> states) throws IOException {
    return ready(super.restoreWriter(context, states));
  }

  @SuppressWarnings("rawtypes")
  private static DorisAbstractWriter ready(DorisAbstractWriter writer) throws IOException {
    try {
      if (writer instanceof DorisBatchWriterAdapter) {
        var delegate = DorisBatchWriterAdapter.class.getDeclaredField("delegate");
        delegate.setAccessible(true);
        var load = DorisBatchWriter.class.getDeclaredField("batchStreamLoad");
        load.setAccessible(true);
        awaitStarted((DorisBatchStreamLoad) load.get(delegate.get(writer)));
      }
      return writer;
    } catch (ReflectiveOperationException | IOException failure) {
      try {
        writer.close();
      } catch (Exception closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw new IOException("Cannot start the released Doris batch loader", failure);
    }
  }

  static void awaitStarted(DorisBatchStreamLoad loader) throws IOException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!loader.isLoadThreadAlive()) {
      if (loader.getException().get() != null)
        throw new IOException("Doris batch loader failed during startup", loader.getException().get());
      if (System.nanoTime() >= deadline) throw new IOException("Doris batch loader did not start");
      try {
        Thread.sleep(1);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while starting the Doris batch loader", interrupted);
      }
    }
  }
}
