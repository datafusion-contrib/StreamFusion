package tech.streamfusion.state;

import java.lang.reflect.Proxy;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.table.data.RowData;

final class StockSnapshotProbe {
  int releases;
  int closes;
  int disposals;
  final CheckpointableKeyedStateBackend<RowData> backend;

  @SuppressWarnings("unchecked")
  StockSnapshotProbe(List<StateMetaInfoSnapshot> metadata) {
    FullSnapshotResources<RowData> resources =
        new FullSnapshotResources<RowData>() {
          @Override
          public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
            return metadata;
          }

          @Override
          public void release() {
            releases++;
          }

          @Override
          public KeyValueStateIterator createKVStateIterator() {
            throw new AssertionError("unexpected serialization during inventory");
          }

          @Override
          public KeyGroupRange getKeyGroupRange() {
            throw new AssertionError("unexpected key-group access");
          }

          @Override
          public TypeSerializer<RowData> getKeySerializer() {
            throw new AssertionError("unexpected serializer access");
          }

          @Override
          public StreamCompressionDecorator getStreamCompressionDecorator() {
            throw new AssertionError("unexpected compression access");
          }
        };
    backend =
        (CheckpointableKeyedStateBackend<RowData>)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {CheckpointableKeyedStateBackend.class},
                (proxy, method, arguments) -> {
                  switch (method.getName()) {
                    case "savepoint":
                      return new SavepointResources<>(
                          resources, SnapshotExecutionType.ASYNCHRONOUS);
                    case "close":
                      closes++;
                      return null;
                    case "dispose":
                      disposals++;
                      return null;
                    default:
                      throw new AssertionError("unexpected backend use: " + method.getName());
                  }
                });
  }
}
