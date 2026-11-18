package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;

class FlussArrowOwnershipTest {
  private static FlussArrowContext context(RootAllocator allocator, AutoCloseable lease)
      throws IOException {
    var path = TablePath.of("ownership", "test");
    var descriptor =
        TableDescriptor.builder()
            .schema(
                org.apache.fluss.metadata.Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .build())
            .distributedBy(1)
            .build();
    var table = TableInfo.of(path, 42, 1, descriptor, "ownership", 0, 0);
    return new FlussArrowContext(new Configuration(), path, table, allocator, null, null, lease);
  }

  @Test
  void readAndWriteOwnersDoNotCloseTheSharedLeaseOrCallerBuffers() throws Exception {
    var releases = new AtomicInteger();
    try (var allocator = new RootAllocator();
        var context = context(allocator, releases::incrementAndGet);
        var root = VectorSchemaRoot.create(context.wireSchema, allocator)) {
      root.allocateNew();
      ((BigIntVector) root.getVector(0)).setSafe(0, 7);
      root.setRowCount(1);
      try (var reader = new FlussArrowReader(context, context.wireSchema, true, null);
          var writer = new FlussArrowWriter(context, false)) {
        writer.close();
        reader.close();
        assertEquals(0, releases.get());
        assertEquals(7, ((BigIntVector) root.getVector(0)).get(0));
      }
      try (var nextReader = new FlussArrowReader(context, context.wireSchema, true, null)) {
        assertEquals(0, releases.get(), "The context is still available to a different read owner");
      }
      context.close();
      context.close();
      assertEquals(1, releases.get());
      assertEquals(7, ((BigIntVector) root.getVector(0)).get(0));
    }
  }

  @Test
  void failedReadInitializationReleasesItsLeaseAndRetainsTheOriginalFailure() throws Exception {
    var releases = new AtomicInteger();
    var closeFailure = new IOException("transport close failed");
    try (var allocator = new RootAllocator()) {
      var context =
          context(
              allocator,
              () -> {
                releases.incrementAndGet();
                throw closeFailure;
              });
      var unknown = new Schema(List.of(Field.nullable("unknown", new ArrowType.Int(64, true))));
      var failure =
          assertThrows(IOException.class, () -> new FlussArrowClient(context, unknown, true, null));
      assertTrue(failure.getMessage().contains("projection contains no physical table columns"));
      assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
      context.close();
      assertEquals(1, releases.get());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void failedTransportShutdownStillStopsBothOwnersAndLeavesCallerBuffersAlive() throws Exception {
    var releases = new AtomicInteger();
    var closeFailure = new IOException("transport close failed");
    try (var allocator = new RootAllocator()) {
      var context =
          context(
              allocator,
              () -> {
                releases.incrementAndGet();
                throw closeFailure;
              });
      try (var root = VectorSchemaRoot.create(context.wireSchema, allocator)) {
        root.allocateNew();
        ((BigIntVector) root.getVector(0)).setSafe(0, 42);
        root.setRowCount(1);
        var client = new FlussArrowClient(context, context.wireSchema, true, null);
        assertSame(closeFailure, assertThrows(IOException.class, client::close));
        assertTrue(
            assertThrows(IOException.class, () -> client.appendAsync(root))
                .getMessage()
                .contains("closed"));
        assertTrue(
            assertThrows(
                    IOException.class,
                    () -> client.fetch(new LogSplit(new TableBucket(42, 0), null, 0, 1)))
                .getMessage()
                .contains("closed"));
        client.close();
        assertEquals(1, releases.get());
        assertEquals(42, ((BigIntVector) root.getVector(0)).get(0));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
