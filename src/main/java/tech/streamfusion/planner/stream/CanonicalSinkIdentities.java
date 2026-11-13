package tech.streamfusion.planner.stream;

interface CanonicalSinkIdentities {
  org.apache.flink.streaming.api.transformations.OneInputTransformation<
      tech.streamfusion.operator.ArrowBatch, tech.streamfusion.operator.ArrowBatch>
      materializerTransformation();

  tech.streamfusion.state.StockStateContract materializerContract();

  default int materializerId() {
    return materializerTransformation().getId();
  }

  int originalMaterializerId();

  java.util.Collection<Integer> helperIds();
}
