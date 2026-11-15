package tech.streamfusion.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

/** Projects canonical metadata inline; payload streams and ordinary checkpoints are delegated. */
public final class CanonicalCheckpointStorage implements CheckpointStorage {
  private static final long serialVersionUID = 1L;
  private final CheckpointStorage delegate;
  private final OperatorIdentityProjection identities;

  public CanonicalCheckpointStorage(
      CheckpointStorage delegate, OperatorIdentityProjection identities) {
    this.delegate = delegate;
    this.identities = identities;
  }

  public static CheckpointStorage unwrap(CheckpointStorage storage) {
    return storage instanceof CanonicalCheckpointStorage
        ? ((CanonicalCheckpointStorage) storage).delegate
        : storage;
  }

  @Override
  public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
    return imported(delegate.resolveCheckpoint(pointer));
  }

  @Override
  public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
    return new Access(delegate.createCheckpointStorage(jobId));
  }

  private CompletedCheckpointStorageLocation imported(CompletedCheckpointStorageLocation location) {
    return new CompletedCheckpointStorageLocation() {
      private static final long serialVersionUID = 1L;

      @Override
      public String getExternalPointer() {
        return location.getExternalPointer();
      }

      @Override
      public StreamStateHandle getMetadataHandle() {
        return new ImportedMetadata(location.getMetadataHandle(), location.getExternalPointer());
      }

      @Override
      public void disposeStorageLocation() throws IOException {
        location.disposeStorageLocation();
      }
    };
  }

  private byte[] project(byte[] bytes, String pointer, boolean exporting) throws IOException {
    var metadata =
        Checkpoints.loadCheckpointMetadata(
            new DataInputStream(new ByteArrayInputStream(bytes)),
            Thread.currentThread().getContextClassLoader(),
            pointer);
    var projected =
        exporting ? identities.exportMetadata(metadata) : identities.importMetadata(metadata);
    if (projected == metadata) return bytes;
    var output = new ByteArrayOutputStream(bytes.length);
    // Parsed handles still belong to the original savepoint. Never dispose temporary metadata.
    Checkpoints.storeCheckpointMetadata(projected, output);
    return output.toByteArray();
  }

  private final class Access implements tech.streamfusion.compat.CheckpointStorageAccessCompat {
    private final CheckpointStorageAccess access;

    Access(CheckpointStorageAccess access) {
      this.access = access;
    }

    @Override
    public boolean supportsHighlyAvailableStorage() {
      return access.supportsHighlyAvailableStorage();
    }

    @Override
    public boolean hasDefaultSavepointLocation() {
      return access.hasDefaultSavepointLocation();
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
      return imported(access.resolveCheckpoint(pointer));
    }

    @Override
    public void initializeBaseLocationsForCheckpoint() throws IOException {
      access.initializeBaseLocationsForCheckpoint();
    }

    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long id) throws IOException {
      return access.initializeLocationForCheckpoint(id);
    }

    @Override
    public CheckpointStorageLocation initializeLocationForSavepoint(long id, String pointer)
        throws IOException {
      return new SavepointLocation(access.initializeLocationForSavepoint(id, pointer));
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
        long id, CheckpointStorageLocationReference reference) throws IOException {
      return access.resolveCheckpointStorageLocation(id, reference);
    }

    @Override
    public CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
      return access.createTaskOwnedStateStream();
    }

    @Override
    public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
      return access.createTaskOwnedCheckpointStateToolset();
    }

    @Override
    public CheckpointStorageAccess storageDelegate() {
      return access;
    }
  }

  private final class SavepointLocation
      implements tech.streamfusion.compat.CheckpointStorageLocationCompat {
    private final CheckpointStorageLocation location;

    SavepointLocation(CheckpointStorageLocation location) {
      this.location = location;
    }

    @Override
    public CheckpointMetadataOutputStream createMetadataOutputStream() throws IOException {
      return new ExportedMetadata(location.createMetadataOutputStream(), location);
    }

    @Override
    public void disposeOnFailure() throws IOException {
      location.disposeOnFailure();
    }

    @Override
    public CheckpointStorageLocationReference getLocationReference() {
      return location.getLocationReference();
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(
        CheckpointedStateScope scope) throws IOException {
      return location.createCheckpointStateOutputStream(scope);
    }

    @Override
    public boolean canFastDuplicate(StreamStateHandle handle, CheckpointedStateScope scope)
        throws IOException {
      return location.canFastDuplicate(handle, scope);
    }

    @Override
    public List<StreamStateHandle> duplicate(
        List<StreamStateHandle> handles, CheckpointedStateScope scope) throws IOException {
      return location.duplicate(handles, scope);
    }

    @Override
    public CheckpointStorageLocation storageDelegate() {
      return location;
    }
  }

  private final class ExportedMetadata extends CheckpointMetadataOutputStream {
    private final CheckpointMetadataOutputStream output;
    private final CheckpointStorageLocation location;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private boolean finalizing;
    private boolean closed;

    ExportedMetadata(CheckpointMetadataOutputStream output, CheckpointStorageLocation location) {
      this.output = output;
      this.location = location;
    }

    private void checkWritable() throws IOException {
      if (closed || finalizing) throw new IOException("Savepoint metadata stream is closed");
    }

    @Override
    public synchronized void write(int value) throws IOException {
      checkWritable();
      buffer.write(value);
    }

    @Override
    public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
      checkWritable();
      buffer.write(bytes, offset, length);
    }

    @Override
    public synchronized long getPos() {
      return buffer.size();
    }

    @Override
    public synchronized void flush() throws IOException {
      checkWritable();
    }

    @Override
    public void sync() throws IOException {
      flush();
    }

    @Override
    public CompletedCheckpointStorageLocation closeAndFinalizeCheckpoint() throws IOException {
      byte[] bytes;
      synchronized (this) {
        checkWritable();
        finalizing = true;
        bytes = buffer.toByteArray();
        buffer.reset();
      }
      try {
        byte[] projected;
        try (var context = new MetadataContext(location)) {
          projected = project(bytes, context.pointer, true);
        }
        synchronized (this) {
          if (closed) throw new IOException("Savepoint metadata publication was cancelled");
        }
        output.write(projected);
        var completed = output.closeAndFinalizeCheckpoint();
        synchronized (this) {
          closed = true;
        }
        return completed;
      } catch (Throwable failure) {
        try {
          close();
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
        if (failure instanceof IOException) throw (IOException) failure;
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw new IOException(failure);
      }
    }

    @Override
    public void close() throws IOException {
      synchronized (this) {
        if (closed) return;
        closed = true;
        buffer.reset();
      }
      // Do not hold the buffer monitor while cancelling a remote write.
      output.close();
    }
  }

  private static final class MetadataContext implements AutoCloseable {
    final Path marker;
    final String pointer;

    MetadataContext(CheckpointStorageLocation location) throws IOException {
      Path directory = null;
      if (location instanceof FsCheckpointStorageLocation) {
        directory = ((FsCheckpointStorageLocation) location).getCheckpointDirectory();
      } else if (!location.getLocationReference().isDefaultReference()) {
        try {
          directory =
              AbstractFsCheckpointStorageAccess.decodePathFromReference(
                  location.getLocationReference());
        } catch (IllegalArgumentException opaqueReference) {
          // Custom storage may use absolute handles and an opaque location reference.
        }
      }
      marker =
          directory == null ? null : new Path(directory, ".identity-context-" + UUID.randomUUID());
      pointer = marker == null ? null : marker.toString();
      if (marker != null) {
        // The released metadata reader needs an existing pointer to resolve relative handles.
        // The final _metadata may not be visible until its recoverable writer commits.
        createContextMarker(marker, marker.getFileSystem());
      }
    }

    @Override
    public void close() throws IOException {
      if (marker != null && !marker.getFileSystem().delete(marker, false)) {
        throw new IOException("Cannot remove savepoint metadata context " + marker);
      }
    }
  }

  static void createContextMarker(Path marker, FileSystem fileSystem) throws IOException {
    var output = fileSystem.create(marker, FileSystem.WriteMode.NO_OVERWRITE);
    try (output) {
      output.write(0);
    } catch (IOException | RuntimeException | Error failure) {
      try {
        if (!fileSystem.delete(marker, false))
          throw new IOException("Cannot remove failed savepoint metadata context " + marker);
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private final class ImportedMetadata implements tech.streamfusion.compat.StreamStateHandleCompat {
    private static final long serialVersionUID = 1L;
    private final StreamStateHandle handle;
    private final String pointer;

    ImportedMetadata(StreamStateHandle handle, String pointer) {
      this.handle = handle;
      this.pointer = pointer;
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
      FSDataInputStream source = handle.openInputStream();
      return new FSDataInputStream() {
        private FSDataInputStream projected;
        private volatile boolean closed;

        private FSDataInputStream input() throws IOException {
          if (closed) throw new IOException("Savepoint metadata stream is closed");
          if (projected == null) {
            byte[] bytes = project(source.readAllBytes(), pointer, false);
            source.close();
            if (closed) throw new IOException("Savepoint metadata read was cancelled");
            projected = new ByteStreamStateHandle("projected-metadata", bytes).openInputStream();
          }
          return projected;
        }

        @Override
        public void seek(long position) throws IOException {
          input().seek(position);
        }

        @Override
        public long getPos() throws IOException {
          return projected == null ? 0 : projected.getPos();
        }

        @Override
        public int read() throws IOException {
          return input().read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
          return input().read(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
          closed = true;
          try {
            source.close();
          } finally {
            if (projected != null) projected.close();
          }
        }
      };
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
      return Optional.empty();
    }

    @Override
    public StreamStateHandle handleDelegate() {
      return handle;
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
      return handle.getStreamStateHandleID();
    }

    @Override
    public void discardState() throws Exception {
      handle.discardState();
    }

    @Override
    public long getStateSize() {
      return handle.getStateSize();
    }
  }
}
