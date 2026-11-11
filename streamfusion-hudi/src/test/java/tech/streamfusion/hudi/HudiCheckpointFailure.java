package tech.streamfusion.hudi;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.ArrowBatch;

/** Shares the same completed-checkpoint failure boundary between reader and writer tests. */
final class HudiCheckpointFailure extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>, CheckpointListener {
  private final String marker;
  private transient boolean committed;

  HudiCheckpointFailure(String marker) {
    this.marker = marker;
  }

  @Override
  public void notifyCheckpointComplete(long id) {
    committed = true;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> record) throws Exception {
    var root = record.getValue().root();
    if (committed && !Files.exists(Path.of(marker))) {
      root.close();
      Files.createFile(Path.of(marker));
      throw new IOException("Expected failure after a completed Hudi checkpoint");
    }
    try {
      Thread.sleep(10);
    } catch (Throwable failure) {
      root.close();
      throw failure;
    }
    output.collect(new StreamRecord<>(new ArrowBatch(root)));
  }
}
