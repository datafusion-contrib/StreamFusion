package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CanonicalCheckpointStorageTest {
  private static final OperatorID NATIVE = new OperatorID(1, 2);
  private static final OperatorID STOCK = new OperatorID(3, 4);
  @TempDir Path root;

  private FileSystemCheckpointStorage provider() {
    return new FileSystemCheckpointStorage(root.resolve("checkpoints").toUri().toString());
  }

  private CanonicalCheckpointStorage projected(FileSystemCheckpointStorage provider) {
    return new CanonicalCheckpointStorage(
        provider, new OperatorIdentityProjection(Map.of(NATIVE.toString(), STOCK.toString())));
  }

  @Test
  void failedContextMarkerWriteOrCloseRemovesItsFileAndPreservesTheOriginalError()
      throws Exception {
    for (boolean failOnClose : List.of(false, true)) {
      var marker = new org.apache.flink.core.fs.Path(root.resolve("context").toUri());
      var original = new IOException(failOnClose ? "context close failed" : "context write failed");
      var closed = new java.util.concurrent.atomic.AtomicBoolean();
      var fileSystem =
          new org.apache.flink.core.fs.local.LocalFileSystem() {
            @Override
            public org.apache.flink.core.fs.FSDataOutputStream create(
                org.apache.flink.core.fs.Path path,
                org.apache.flink.core.fs.FileSystem.WriteMode mode)
                throws IOException {
              var output = super.create(path, mode);
              return new org.apache.flink.core.fs.FSDataOutputStream() {
                public long getPos() throws IOException {
                  return output.getPos();
                }

                public void flush() throws IOException {
                  output.flush();
                }

                public void sync() throws IOException {
                  output.sync();
                }

                public void write(int value) throws IOException {
                  if (!failOnClose) throw original;
                  output.write(value);
                }

                public void close() throws IOException {
                  output.close();
                  closed.set(true);
                  if (failOnClose) throw original;
                }
              };
            }
          };
      assertSame(
          original,
          assertThrows(
              IOException.class,
              () -> CanonicalCheckpointStorage.createContextMarker(marker, fileSystem)));
      assertTrue(closed.get());
      assertFalse(Files.exists(root.resolve("context")));
    }
  }

  @Test
  void exportsStockMetadataAndImportsNativeIdentitiesAfterRelocation() throws Exception {
    var provider = provider();
    var storage = projected(provider);
    var location =
        storage
            .createCheckpointStorage(new JobID())
            .initializeLocationForSavepoint(11, root.resolve("savepoints").toUri().toString());
    Path directory = directory(location);
    Files.write(directory.resolve("state-data"), new byte[] {5, 7, 9});
    var relative =
        new RelativeFileStateHandle(
            new org.apache.flink.core.fs.Path(directory.resolve("state-data").toUri()),
            "state-data",
            3);
    var operator = operator(NATIVE);
    operator.putState(
        0,
        OperatorSubtaskState.builder()
            .setManagedKeyedState(
                new StateObjectCollection<>(
                    List.of(
                        new KeyGroupsSavepointStateHandle(
                            new KeyGroupRangeOffsets(0, 0, new long[] {0}), relative))))
            .build());
    operator.setCoordinatorState(new ByteStreamStateHandle("enumerator", new byte[] {3, 8}));
    var metadata = metadata(operator, SavepointFormatType.CANONICAL);
    var completed = write(location, metadata);
    assertNoContextFiles(directory);
    Path relocated = root.resolve("relocated");
    Files.move(directory, relocated);
    String pointer = relocated.toUri().toString();
    var stock = read(provider.resolveCheckpoint(pointer));
    var actual = stock.getOperatorStates().iterator().next();
    assertEquals(STOCK, actual.getOperatorID());
    assertEquals(operator.getCoordinatorState(), actual.getCoordinatorState());
    assertArrayEquals(new byte[] {9, 6}, stock.getMasterStates().iterator().next().bytes());
    var keyed =
        (KeyGroupsSavepointStateHandle) actual.getState(0).getManagedKeyedState().iterator().next();
    var payload = (RelativeFileStateHandle) keyed.getDelegateStateHandle();
    assertEquals("state-data", payload.getRelativePath());
    try (var input = payload.openInputStream()) {
      assertArrayEquals(new byte[] {5, 7, 9}, input.readAllBytes());
    }
    var imported = read(storage.resolveCheckpoint(pointer));
    assertEquals(NATIVE, imported.getOperatorStates().iterator().next().getOperatorID());
    assertTrue(Files.exists(relocated.resolve("state-data")));
    assertFalse(
        Files.readString(
                relocated.resolve("_metadata"), java.nio.charset.StandardCharsets.ISO_8859_1)
            .contains("tech.streamfusion"));
    assertEquals(11, imported.getCheckpointId());
    assertNotNull(completed.getExternalPointer());
  }

  @Test
  void ordinaryCheckpointsAndNativeSavepointsKeepTheirBytesAndIdentities() throws Exception {
    var provider = provider();
    var storage = projected(provider);
    var access = storage.createCheckpointStorage(new JobID());
    access.initializeBaseLocationsForCheckpoint();
    var checkpoint =
        new CheckpointMetadata(
            12,
            List.of(operator(NATIVE)),
            List.of(),
            CheckpointProperties.forCheckpoint(
                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
    var completed = write(access.initializeLocationForCheckpoint(12), checkpoint);
    assertArrayEquals(
        bytes(checkpoint),
        completed.getMetadataHandle().asBytesIfInMemory().orElseGet(() -> readBytes(completed)));
    assertEquals(
        NATIVE,
        read(storage.resolveCheckpoint(completed.getExternalPointer()))
            .getOperatorStates()
            .iterator()
            .next()
            .getOperatorID());
    var nativeSavepoint = metadata(operator(NATIVE), SavepointFormatType.NATIVE);
    var location =
        access.initializeLocationForSavepoint(13, root.resolve("savepoints").toUri().toString());
    var saved = write(location, nativeSavepoint);
    assertArrayEquals(bytes(nativeSavepoint), readBytes(saved));
    assertEquals(
        NATIVE,
        read(storage.resolveCheckpoint(saved.getExternalPointer()))
            .getOperatorStates()
            .iterator()
            .next()
            .getOperatorID());
    assertNoContextFiles(directory(location));
  }

  @Test
  void unknownStateFailsBeforePublishingMetadataAndKeepsPayloadOwnership() throws Exception {
    var location =
        projected(provider())
            .createCheckpointStorage(new JobID())
            .initializeLocationForSavepoint(11, root.resolve("savepoints").toUri().toString());
    Path directory = directory(location);
    Files.write(directory.resolve("state-data"), new byte[] {1});
    var unknown = operator(new OperatorID());
    unknown.setCoordinatorState(new ByteStreamStateHandle("owned", new byte[0]));
    var output = location.createMetadataOutputStream();
    output.write(bytes(metadata(unknown, SavepointFormatType.CANONICAL)));
    var failure = assertThrows(IOException.class, output::closeAndFinalizeCheckpoint);
    assertTrue(failure.getMessage().contains("identity mapping"), failure.toString());
    assertFalse(Files.exists(directory.resolve("_metadata")));
    assertTrue(Files.exists(directory.resolve("state-data")));
    assertNoContextFiles(directory);
    output.close();
    assertThrows(IOException.class, () -> output.write(7));
    assertThrows(IOException.class, output::closeAndFinalizeCheckpoint);
  }

  @Test
  void cancelledOutputNeverPublishes() throws Exception {
    var location =
        projected(provider())
            .createCheckpointStorage(new JobID())
            .initializeLocationForSavepoint(11, root.resolve("savepoints").toUri().toString());
    var output = location.createMetadataOutputStream();
    byte[] bytes = bytes(metadata(operator(NATIVE), SavepointFormatType.CANONICAL));
    output.write(bytes);
    assertEquals(bytes.length, output.getPos());
    output.flush();
    output.sync();
    output.close();
    output.close();
    assertThrows(IOException.class, output::closeAndFinalizeCheckpoint);
    assertFalse(Files.exists(directory(location).resolve("_metadata")));
    assertNoContextFiles(directory(location));
  }

  @Test
  void malformedMetadataIsAbortedWithoutPublication() throws Exception {
    var location =
        projected(provider())
            .createCheckpointStorage(new JobID())
            .initializeLocationForSavepoint(11, root.resolve("savepoints").toUri().toString());
    var output = location.createMetadataOutputStream();
    output.write(new byte[] {1, 2, 3});
    assertThrows(IOException.class, output::closeAndFinalizeCheckpoint);
    assertFalse(Files.exists(directory(location).resolve("_metadata")));
    assertNoContextFiles(directory(location));
  }

  @Test
  @org.junit.jupiter.api.Timeout(10)
  void cancellationInterruptsMetadataPublicationWithoutWaitingForTheRemoteWrite() throws Exception {
    var writing = new java.util.concurrent.CountDownLatch(1);
    var cancelled = new java.util.concurrent.CountDownLatch(1);
    var published = new java.util.concurrent.atomic.AtomicBoolean();
    var remote =
        new org.apache.flink.runtime.state.CheckpointMetadataOutputStream() {
          @Override
          public void write(int value) throws IOException {
            writing.countDown();
            try {
              if (!cancelled.await(5, java.util.concurrent.TimeUnit.SECONDS))
                throw new IOException("remote write timed out");
            } catch (InterruptedException failure) {
              Thread.currentThread().interrupt();
              throw new IOException(failure);
            }
            throw new IOException("remote write cancelled");
          }

          @Override
          public long getPos() {
            return 0;
          }

          @Override
          public void flush() {}

          @Override
          public void sync() {}

          @Override
          public void close() {
            cancelled.countDown();
          }

          @Override
          public CompletedCheckpointStorageLocation closeAndFinalizeCheckpoint() {
            published.set(true);
            throw new AssertionError("cancelled metadata was published");
          }
        };
    var location =
        (CheckpointStorageLocation)
            java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {CheckpointStorageLocation.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("createMetadataOutputStream")) return remote;
                  if (method.getName().equals("getLocationReference"))
                    return org.apache.flink.runtime.state.CheckpointStorageLocationReference
                        .getDefault();
                  throw new AssertionError("unexpected location use: " + method.getName());
                });
    var access =
        (org.apache.flink.runtime.state.CheckpointStorageAccess)
            java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {org.apache.flink.runtime.state.CheckpointStorageAccess.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("initializeLocationForSavepoint")) return location;
                  throw new AssertionError("unexpected access use: " + method.getName());
                });
    var delegate =
        new org.apache.flink.runtime.state.CheckpointStorage() {
          @Override
          public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) {
            throw new UnsupportedOperationException();
          }

          @Override
          public org.apache.flink.runtime.state.CheckpointStorageAccess createCheckpointStorage(
              JobID id) {
            return access;
          }
        };
    var storage =
        new CanonicalCheckpointStorage(
            delegate, new OperatorIdentityProjection(Map.of(NATIVE.toString(), STOCK.toString())));
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try (var output =
        storage
            .createCheckpointStorage(new JobID())
            .initializeLocationForSavepoint(11, null)
            .createMetadataOutputStream()) {
      output.write(bytes(metadata(operator(NATIVE), SavepointFormatType.CANONICAL)));
      var result = executor.submit(output::closeAndFinalizeCheckpoint);
      assertTrue(writing.await(5, java.util.concurrent.TimeUnit.SECONDS));
      output.close();
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> result.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertInstanceOf(IOException.class, failure.getCause());
      assertFalse(published.get());
    } finally {
      cancelled.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  @org.junit.jupiter.api.Timeout(10)
  void metadataInputCanBeCancelledWhileTheStorageReadIsBlocked() throws Exception {
    var reading = new java.util.concurrent.CountDownLatch(1);
    var cancelled = new java.util.concurrent.CountDownLatch(1);
    var handle =
        new ByteStreamStateHandle("remote-metadata", new byte[0]) {
          @Override
          public org.apache.flink.core.fs.FSDataInputStream openInputStream() {
            return new org.apache.flink.core.fs.FSDataInputStream() {
              @Override
              public void seek(long position) {
                throw new UnsupportedOperationException();
              }

              @Override
              public long getPos() {
                return 0;
              }

              @Override
              public int read() throws IOException {
                reading.countDown();
                try {
                  if (!cancelled.await(5, java.util.concurrent.TimeUnit.SECONDS))
                    throw new IOException("storage read timed out");
                } catch (InterruptedException failure) {
                  Thread.currentThread().interrupt();
                  throw new IOException(failure);
                }
                throw new IOException("storage read cancelled");
              }

              @Override
              public void close() {
                cancelled.countDown();
              }
            };
          }
        };
    var location =
        new CompletedCheckpointStorageLocation() {
          @Override
          public String getExternalPointer() {
            return "remote-pointer";
          }

          @Override
          public org.apache.flink.runtime.state.StreamStateHandle getMetadataHandle() {
            return handle;
          }

          @Override
          public void disposeStorageLocation() {
            throw new AssertionError("read transferred ownership");
          }
        };
    var delegate =
        new org.apache.flink.runtime.state.CheckpointStorage() {
          @Override
          public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) {
            return location;
          }

          @Override
          public org.apache.flink.runtime.state.CheckpointStorageAccess createCheckpointStorage(
              JobID id) {
            throw new UnsupportedOperationException();
          }
        };
    var storage =
        new CanonicalCheckpointStorage(
            delegate, new OperatorIdentityProjection(Map.of(NATIVE.toString(), STOCK.toString())));
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try (var input =
        storage.resolveCheckpoint("remote-pointer").getMetadataHandle().openInputStream()) {
      assertEquals(
          1, reading.getCount(), "open must allow registration with Flink's cancellation registry");
      var result = executor.submit(() -> input.read());
      assertTrue(reading.await(5, java.util.concurrent.TimeUnit.SECONDS));
      input.close();
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> result.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertInstanceOf(IOException.class, failure.getCause());
      assertTrue(failure.getCause().getMessage().contains("cancelled"));
    } finally {
      cancelled.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void disabledFactoryUsesTheConfiguredProviderWithoutReadingStaleMappings() {
    var config = new org.apache.flink.configuration.Configuration();
    config.setString("streamfusion.native.enabled", "false");
    config.setString(CanonicalCheckpointStorageFactory.DELEGATE, "filesystem");
    config.setString("execution.checkpointing.dir", root.toUri().toString());
    config.setString(CanonicalCheckpointStorageFactory.IDENTITIES, "invalid base64");
    config.setString(CanonicalCheckpointStorageFactory.ACTIVE, "true");
    var factory = new CanonicalCheckpointStorageFactory();
    assertInstanceOf(
        FileSystemCheckpointStorage.class,
        factory.createFromConfig(config, getClass().getClassLoader()));
    config.setString("streamfusion.native.enabled", "true");
    assertThrows(
        org.apache.flink.configuration.IllegalConfigurationException.class,
        () -> factory.createFromConfig(config, getClass().getClassLoader()));
    CanonicalCheckpointStorageFactory.reset(config);
    assertInstanceOf(
        FileSystemCheckpointStorage.class,
        factory.createFromConfig(config, getClass().getClassLoader()));
  }

  private static OperatorState operator(OperatorID id) {
    return new OperatorState("user-operator", "user-uid", id, 1, 128);
  }

  private static CheckpointMetadata metadata(OperatorState operator, SavepointFormatType format) {
    return new CheckpointMetadata(
        11,
        List.of(operator),
        List.of(new MasterState("user-hook", new byte[] {9, 6}, 1)),
        CheckpointProperties.forSavepoint(false, format));
  }

  private static byte[] bytes(CheckpointMetadata metadata) throws IOException {
    var output = new ByteArrayOutputStream();
    Checkpoints.storeCheckpointMetadata(metadata, output);
    return output.toByteArray();
  }

  private static CompletedCheckpointStorageLocation write(
      CheckpointStorageLocation location, CheckpointMetadata metadata) throws IOException {
    try (var output = location.createMetadataOutputStream()) {
      output.write(bytes(metadata));
      return output.closeAndFinalizeCheckpoint();
    }
  }

  private static CheckpointMetadata read(CompletedCheckpointStorageLocation location)
      throws IOException {
    try (var input = new DataInputStream(location.getMetadataHandle().openInputStream())) {
      return Checkpoints.loadCheckpointMetadata(
          input,
          CanonicalCheckpointStorageTest.class.getClassLoader(),
          location.getExternalPointer());
    }
  }

  private static byte[] readBytes(CompletedCheckpointStorageLocation location) {
    try (var input = location.getMetadataHandle().openInputStream()) {
      return input.readAllBytes();
    } catch (IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  private static Path directory(CheckpointStorageLocation location) {
    return Path.of(
        AbstractFsCheckpointStorageAccess.decodePathFromReference(location.getLocationReference())
            .toUri());
  }

  private static void assertNoContextFiles(Path directory) throws IOException {
    try (var files = Files.list(directory)) {
      assertTrue(
          files.noneMatch(path -> path.getFileName().toString().startsWith(".identity-context-")));
    }
  }
}
