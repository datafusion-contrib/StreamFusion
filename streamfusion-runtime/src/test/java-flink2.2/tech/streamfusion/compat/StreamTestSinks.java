package tech.streamfusion.compat;

public final class StreamTestSinks {
  private StreamTestSinks() {}

  public static <T> void discard(org.apache.flink.streaming.api.datastream.DataStream<T> stream) {
    stream.sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>());
  }
}
