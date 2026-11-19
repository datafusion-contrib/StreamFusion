package tech.streamfusion.compat;

import java.util.Collection;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Uses the selected host's collection-source API with the same elements and type information. */
public final class FlinkTestSources {
  private FlinkTestSources() {}

  public static <T> DataStreamSource<T> fromData(StreamExecutionEnvironment env, T... data) {
    return env.fromData(data);
  }

  public static <T> DataStreamSource<T> fromData(
      StreamExecutionEnvironment env, TypeInformation<T> type, T... data) {
    return env.fromData(type, data);
  }

  public static <T> DataStreamSource<T> fromData(
      StreamExecutionEnvironment env, Collection<T> data, TypeInformation<T> type) {
    return env.fromData(data, type);
  }

  public static <T> DataStreamSource<T> fromData(
      StreamExecutionEnvironment env, Class<T> type, T... data) {
    return env.fromData(type, data);
  }

  public static <T> DataStreamSource<T> fromData(
      StreamExecutionEnvironment env, Collection<T> data) {
    return env.fromData(data);
  }
}
