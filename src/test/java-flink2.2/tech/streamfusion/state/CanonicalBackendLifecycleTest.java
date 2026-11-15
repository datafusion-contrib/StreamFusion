package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class CanonicalBackendLifecycleTest {
  @Test
  void namespacedContractIsValidatedBeforeNativeAllocationAndKeepsStockCleanup() throws Exception {
    String id = "0123456789abcdef0123456789abcdef";
    var type =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType());
    var keys = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type);
    var namespace = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
    var events = "_timer_state/event_window-timers";
    StockStateContract contract =
        new StockStateContract() {
          @Override
          public org.apache.flink.api.common.typeutils.TypeSerializer<RowData> keySerializer() {
            return keys;
          }

          @Override
          public java.util.Map<String, org.apache.flink.api.common.state.StateDescriptor.Type>
              stateDescriptors() {
            return java.util.Map.of(
                "window-aggs", org.apache.flink.api.common.state.StateDescriptor.Type.VALUE);
          }

          @Override
          public org.apache.flink.api.common.typeutils.TypeSerializer<?> namespaceSerializer(
              String name) {
            return namespace;
          }

          @Override
          public java.util.Map<String, StockStateInventory.Timer> timerSchema() {
            return java.util.Map.of(events, StockStateInventory.Timer.consumed(namespace));
          }

          @Override
          public void importState(
              CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent) {}

          @Override
          public void exportState(
              CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent) {}

          @Override
          public void clear(CheckpointableKeyedStateBackend<RowData> backend) {}
        };
    var probe =
        new StockSnapshotProbe(
            java.util.List.of(
                new org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot(
                    "window-aggs",
                    org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType
                        .KEY_VALUE,
                    java.util.Map.of("KEYED_STATE_TYPE", "VALUE"),
                    java.util.Map.of("NAMESPACE_SERIALIZER", namespace.snapshotConfiguration())),
                new org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot(
                    events,
                    org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType
                        .PRIORITY_QUEUE,
                    java.util.Map.of(),
                    java.util.Map.of(
                        "VALUE_SERIALIZER",
                        new org.apache.flink.streaming.api.operators.TimerSerializer<>(
                                keys, namespace)
                            .snapshotConfiguration()))));
    var configured =
        new CanonicalStateBackend(
            new org.apache.flink.runtime.state.hashmap.HashMapStateBackend(),
            java.util.Map.of(
                id, new CanonicalStateBackend.BoundContract("NativeWindow", contract)));
    var handles =
        java.util.List.<org.apache.flink.runtime.state.KeyedStateHandle>of(
            new org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle(
                new org.apache.flink.runtime.state.KeyGroupRangeOffsets(0, 0, new long[] {0}),
                new org.apache.flink.runtime.state.memory.ByteStreamStateHandle(
                    "stock", new byte[] {1})));
    AtomicInteger allocated = new AtomicInteger();
    var context =
        new tech.streamfusion.compat.KeyedBackendContext<Integer>(
            null,
            new org.apache.flink.api.common.JobID(),
            "NativeWindow_" + id + "_(1/1)",
            org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
            128,
            new org.apache.flink.runtime.state.KeyGroupRange(0, 127),
            handles,
            0,
            restored -> {
              assertEquals(
                  1, probe.releases, "Stock validation must finish before native allocation");
              assertTrue(restored.isEmpty());
              allocated.incrementAndGet();
              return new BackendProbe<Integer>().backend;
            },
            restored -> {
              throw new AssertionError("unused native projection");
            });
    context.withStockBackendFactory((serializer, restored) -> probe.backend);
    var backend = configured.createNativeKeyedBackend(context);
    assertEquals(1, allocated.get());
    backend.close();
    backend.dispose();
    assertEquals(1, probe.closes);
    assertEquals(1, probe.disposals);
  }

  @Test
  void noClaimCapabilityFollowsTheConfiguredBackendForTheFirstIndependentCheckpoint() {
    assertTrue(
        new CanonicalStateBackend(
                new org.apache.flink.runtime.state.hashmap.HashMapStateBackend(),
                java.util.Map.of())
            .supportsNoClaimRestoreMode());
    assertTrue(
        new CanonicalStateBackend(
                new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(),
                java.util.Map.of())
            .supportsNoClaimRestoreMode());
    var unsupported =
        new org.apache.flink.runtime.state.hashmap.HashMapStateBackend() {
          @Override
          public boolean supportsNoClaimRestoreMode() {
            return false;
          }
        };
    assertFalse(
        new CanonicalStateBackend(unsupported, java.util.Map.of()).supportsNoClaimRestoreMode());
  }

  @Test
  void stockOperatorWithTheSameIdKeepsItsOwnRestoreHandles() throws Exception {
    verifyUnboundOperatorKeepsItsOwnRestoreHandles("KeyedProcessOperator");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "NativeColumnarKeepLastDeduplicateOperator_Unsupported",
        "NativeColumnarKeepLastDeduplicateOperator_0123456789abcdef0123456789abcdef"
      })
  void anUnboundOperatorClassCannotReuseAContractBySharingItsNamePrefix(String operatorClass)
      throws Exception {
    verifyUnboundOperatorKeepsItsOwnRestoreHandles(operatorClass);
  }

  private void verifyUnboundOperatorKeepsItsOwnRestoreHandles(String operatorClass)
      throws Exception {
    String id = "0123456789abcdef0123456789abcdef";
    var rowType =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType());
    var contract = valueContract(rowType);
    var backend =
        new CanonicalStateBackend(
            new org.apache.flink.runtime.state.hashmap.HashMapStateBackend(),
            java.util.Map.of(
                id,
                new CanonicalStateBackend.BoundContract(
                    "NativeColumnarKeepLastDeduplicateOperator", contract)));
    var handles =
        java.util.List.<org.apache.flink.runtime.state.KeyedStateHandle>of(
            new org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle(
                new org.apache.flink.runtime.state.KeyGroupRangeOffsets(0, 0, new long[] {0}),
                new org.apache.flink.runtime.state.memory.ByteStreamStateHandle(
                    "stock", new byte[] {1})));
    BackendProbe<Integer> probe = new BackendProbe<>();
    var context =
        new tech.streamfusion.compat.KeyedBackendContext<Integer>(
            null,
            new org.apache.flink.api.common.JobID(),
            operatorClass + "_" + id + "_(1/1)",
            org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
            128,
            new org.apache.flink.runtime.state.KeyGroupRange(0, 127),
            handles,
            0,
            restored -> {
              assertSame(handles, restored);
              return probe.backend;
            },
            restored -> {
              throw new AssertionError("unused native projection");
            });
    context.withStockBackendFactory(
        (serializer, restored) -> {
          throw new AssertionError("stock operator state was intercepted");
        });
    assertSame(probe.backend, backend.createNativeKeyedBackend(context));
  }

  @Test
  void disablingNativeExecutionDoesNotDecodeStaleContracts() throws Exception {
    var config = new org.apache.flink.configuration.Configuration();
    config.setString("streamfusion.native.enabled", "false");
    config.setString(CanonicalStateBackend.DELEGATE, "hashmap");
    config.setString(CanonicalStateBackend.CONTRACTS, "invalid base64");
    var factory = new CanonicalStateBackendFactory();
    assertInstanceOf(
        org.apache.flink.runtime.state.hashmap.HashMapStateBackend.class,
        factory.createFromConfig(config, getClass().getClassLoader()));
    config.setString("streamfusion.native.enabled", "true");
    assertThrows(
        IOException.class, () -> factory.createFromConfig(config, getClass().getClassLoader()));
  }

  @Test
  void ordinaryProcessingAndCancellationDoNotMaterializeTheCanonicalView() throws Exception {
    AtomicInteger created = new AtomicInteger();
    CanonicalProjection view =
        new CanonicalProjection(
            () -> {
              created.incrementAndGet();
              return new BackendProbe<RowData>().backend;
            },
            false);
    BackendProbe<Integer> nativeProbe = new BackendProbe<>();
    CheckpointableKeyedStateBackend<Integer> nativeState = nativeProbe.backend;
    var backend = new CanonicalKeyedStateBackend<>(nativeState, view);
    backend.setCurrentKey(3);
    backend.close();
    backend.dispose();
    assertEquals(0, created.get());
    assertEquals(3, nativeProbe.key);
    assertEquals(1, nativeProbe.closes);
    assertEquals(1, nativeProbe.disposals);
    assertThrows(IllegalStateException.class, view::backend);
  }

  @Test
  void failedInitializationDisposesImportedStateWithoutOperatorInitialization() throws Exception {
    BackendProbe<RowData> stockProbe = new BackendProbe<>();
    CheckpointableKeyedStateBackend<RowData> stockState = stockProbe.backend;
    CanonicalProjection view = new CanonicalProjection(() -> stockState, true);
    var backend = new CanonicalKeyedStateBackend<>(new BackendProbe<Integer>().backend, view);
    assertSame(stockState, view.backend());
    backend.dispose();
    backend.close();
    view.close();
    assertEquals(1, stockProbe.closes);
    assertEquals(1, stockProbe.disposals);
  }

  @Test
  void nativeCloseFailureStillClosesAndDisposesTheStockView() throws Exception {
    BackendProbe<RowData> stockProbe = new BackendProbe<>();
    CheckpointableKeyedStateBackend<RowData> stockState = stockProbe.backend;
    CanonicalProjection view = new CanonicalProjection(() -> stockState, true);
    BackendProbe<Integer> nativeProbe = new BackendProbe<>();
    CheckpointableKeyedStateBackend<Integer> nativeState = nativeProbe.backend;
    IOException nativeFailure = new IOException("native close failure");
    nativeProbe.closeFailure = nativeFailure;
    stockProbe.closeFailure = new IOException("stock close failure");
    var backend = new CanonicalKeyedStateBackend<>(nativeState, view);
    assertSame(nativeFailure, assertThrows(IOException.class, backend::close));
    assertEquals(1, nativeFailure.getSuppressed().length);
    assertEquals(1, stockProbe.disposals);
    backend.dispose();
    assertEquals(1, stockProbe.closes);
  }

  @Test
  @SuppressWarnings("unchecked")
  void unknownStockStateFailsBeforeCreatingNativeStateAndReleasesTheView() throws Exception {
    String id = "0123456789abcdef0123456789abcdef";
    var type =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType());
    var contract = valueContract(type);
    var configured =
        new CanonicalStateBackend(
            new org.apache.flink.runtime.state.hashmap.HashMapStateBackend(),
            java.util.Map.of(
                id,
                new CanonicalStateBackend.BoundContract(
                    "NativeColumnarKeepLastDeduplicateOperator", contract)));
    var probe =
        new StockSnapshotProbe(
            java.util.List.of(
                new org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot(
                    "unexpected-stock-state",
                    org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType
                        .KEY_VALUE,
                    java.util.Map.of(),
                    java.util.Map.of())));
    var stock = probe.backend;
    var handles =
        java.util.List.<org.apache.flink.runtime.state.KeyedStateHandle>of(
            new org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle(
                new org.apache.flink.runtime.state.KeyGroupRangeOffsets(0, 0, new long[] {0}),
                new org.apache.flink.runtime.state.memory.ByteStreamStateHandle(
                    "stock", new byte[] {1})));
    var context =
        new tech.streamfusion.compat.KeyedBackendContext<Integer>(
            null,
            new org.apache.flink.api.common.JobID(),
            "NativeColumnarKeepLastDeduplicateOperator_" + id + "_(1/1)",
            org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
            128,
            new org.apache.flink.runtime.state.KeyGroupRange(0, 127),
            handles,
            0,
            restored -> {
              throw new AssertionError("native state was created before inventory validation");
            },
            restored -> {
              throw new AssertionError("unused native projection");
            });
    context.withStockBackendFactory((serializer, restored) -> stock);
    var error =
        assertThrows(
            IllegalStateException.class, () -> configured.createNativeKeyedBackend(context));
    assertTrue(error.getMessage().contains("unexpected-stock-state"), error.getMessage());
    assertEquals(1, probe.releases);
    assertEquals(1, probe.closes);
    assertEquals(1, probe.disposals);
  }

  private static StockStateContract valueContract(
      org.apache.flink.table.types.logical.RowType type) {
    return new StockStateContract() {
      @Override
      public org.apache.flink.api.common.typeutils.TypeSerializer<RowData> keySerializer() {
        return new org.apache.flink.table.runtime.typeutils.RowDataSerializer(type);
      }

      @Override
      public java.util.Map<String, org.apache.flink.api.common.state.StateDescriptor.Type>
          stateDescriptors() {
        return java.util.Map.of(
            "deduplicate-state", org.apache.flink.api.common.state.StateDescriptor.Type.VALUE);
      }

      @Override
      public void exportState(
          CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent) {
        throw new AssertionError("Lifecycle checks must not transfer operator data");
      }

      @Override
      public void importState(
          CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent) {
        throw new AssertionError("Lifecycle checks must not transfer operator data");
      }

      @Override
      public void clear(CheckpointableKeyedStateBackend<RowData> backend) {
        throw new AssertionError("Lifecycle checks must not transfer operator data");
      }
    };
  }

  private static final class BackendProbe<K> {
    K key;
    int closes;
    int disposals;
    Throwable closeFailure;

    @SuppressWarnings("unchecked")
    final CheckpointableKeyedStateBackend<K> backend =
        (CheckpointableKeyedStateBackend<K>)
            java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {CheckpointableKeyedStateBackend.class},
                (proxy, method, arguments) -> {
                  switch (method.getName()) {
                    case "setCurrentKey":
                      key = (K) arguments[0];
                      return null;
                    case "close":
                      closes++;
                      if (closeFailure != null) throw closeFailure;
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
