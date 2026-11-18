package tech.streamfusion.compat;

public final class StreamTestSinks {
  private StreamTestSinks() {}

  public static <T> void discard(org.apache.flink.streaming.api.datastream.DataStream<T> stream) {
    stream.addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());
  }
}
