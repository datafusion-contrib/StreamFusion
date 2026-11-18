package tech.streamfusion.fluss;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.Errors;

/**
 * Shared transport lease and table identity; allocators and returned Arrow batches remain
 * caller-owned.
 */
final class FlussArrowContext implements AutoCloseable {
  final Configuration config;
  final TablePath path;
  final FlussConnection connection;
  final MetadataUpdater metadata;
  final TableInfo table;
  final BufferAllocator allocator;
  final Schema wireSchema;
  final boolean profile = Boolean.getBoolean("streamfusion.fluss.profile");
  private final AutoCloseable connectionLease;
  private final AtomicBoolean closed = new AtomicBoolean();

  static FlussArrowContext open(Configuration config, TablePath path, BufferAllocator allocator)
      throws IOException {
    var lease = FlussConnections.acquire(config);
    try {
      var connection = lease.connection();
      var metadata = connection.getMetadataUpdater();
      var table = await(connection.getAdmin().getTableInfo(path));
      return new FlussArrowContext(config, path, table, allocator, connection, metadata, lease);
    } catch (Throwable failure) {
      try {
        lease.close();
      } catch (Exception close) {
        failure.addSuppressed(close);
      }
      throw failure;
    }
  }

  FlussArrowContext(
      Configuration config,
      TablePath path,
      TableInfo table,
      BufferAllocator allocator,
      FlussConnection connection,
      MetadataUpdater metadata,
      AutoCloseable connectionLease)
      throws IOException {
    this.config = config;
    this.path = path;
    this.table = table;
    this.allocator = allocator;
    this.connection = connection;
    this.metadata = metadata;
    this.connectionLease = connectionLease;
    wireSchema = FlussArrowSchema.wireSchema(table.getRowType());
  }

  @FunctionalInterface
  interface ResponseValidator<T> {
    void validate(T response) throws IOException;
  }

  <T extends ApiMessage> T retry(
      TableBucket bucket,
      java.util.function.Function<TabletServerGateway, CompletableFuture<T>> call,
      ResponseValidator<T> validator,
      int maxRetries)
      throws IOException {
    for (long attempt = 0; ; attempt++) {
      try {
        metadata.checkAndUpdateMetadata(path, bucket);
        Integer leader = metadata.getCluster().leaderFor(bucket);
        if (leader == null)
          throw new org.apache.fluss.exception.LeaderNotAvailableException(
              "Fluss bucket leader is unavailable");
        var gateway = metadata.newTabletServerClientForNode(leader);
        if (gateway == null)
          throw new org.apache.fluss.exception.LeaderNotAvailableException(
              "Fluss bucket leader is unavailable");
        T response = await(call.apply(gateway));
        try {
          validator.validate(response);
          return response;
        } catch (IOException | RuntimeException | Error failure) {
          if (response.isLazilyParsed() && response.getParsedByteBuf() != null)
            response.getParsedByteBuf().release();
          throw failure;
        }
      } catch (RetriableException failure) {
        if (attempt >= maxRetries) throw failure;
        metadata.updateTableOrPartitionMetadata(path, bucket.getPartitionId());
        try {
          Thread.sleep(100);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IOException("Fluss retry interrupted", interrupted);
        }
      }
    }
  }

  static void checkError(org.apache.fluss.rpc.messages.ErrorMessage response) {
    if (response.hasErrorCode()) {
      RuntimeException error =
          Errors.forCode(response.getErrorCode())
              .exception(response.hasErrorMessage() ? response.getErrorMessage() : null);
      if (error != null) throw error;
    }
  }

  static <T> T await(CompletableFuture<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException failure) {
      // The shared transport can outlive this reader; release a reply arriving after cancellation.
      future.thenAccept(
          reply -> {
            if (reply instanceof ApiMessage
                && ((ApiMessage) reply).isLazilyParsed()
                && ((ApiMessage) reply).getParsedByteBuf() != null) {
              ApiMessage message = ((ApiMessage) reply);
              message.getParsedByteBuf().release();
            }
          });
      Thread.currentThread().interrupt();
      throw new IOException(failure);
    } catch (ExecutionException failure) {
      Object runtimeCandidate;
      if ((runtimeCandidate = failure.getCause()) instanceof RuntimeException) {
        RuntimeException runtime = ((RuntimeException) runtimeCandidate);
        throw runtime;
      }
      throw new IOException(failure.getCause());
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) connectionLease.close();
  }
}
