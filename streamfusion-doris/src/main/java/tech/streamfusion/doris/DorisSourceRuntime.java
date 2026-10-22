package tech.streamfusion.doris;

import java.io.Serializable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.streamfusion.operator.ArrowBatch;

public interface DorisSourceRuntime extends Serializable {
  Transformation<ArrowBatch> addTo(StreamExecutionEnvironment environment);
}
