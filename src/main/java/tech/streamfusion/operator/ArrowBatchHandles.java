package tech.streamfusion.operator;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-global handoff table for the zero-copy local exchange. When producer and consumer of a
 * columnar shuffle edge share one JVM, the serializer parks the batch here and ships only a handle
 * through Flink's network buffers; the receiving deserializer claims the batch back by handle,
 * ownership intact, without ever touching the Arrow buffers. The frame carries this JVM's random
 * token so a handle that leaks across a process boundary (a misconfigured multi-TaskManager
 * deployment) fails loudly on the reader instead of dereferencing foreign memory.
 *
 * <p>Ownership: {@link #register} transfers the batch to the table; {@link #claim} transfers it
 * out, exactly once. Each producing split subtask has an owner token, so its failure/cancellation
 * cleanup can close only its own records dropped between those two operations.
 */
public final class ArrowBatchHandles {

  public static final long TOKEN_HI;
  public static final long TOKEN_LO;

  static {
    UUID token = UUID.randomUUID();
    TOKEN_HI = token.getMostSignificantBits();
    TOKEN_LO = token.getLeastSignificantBits();
  }

  private static final AtomicLong NEXT = new AtomicLong();
  private static final AtomicLong NEXT_OWNER = new AtomicLong();
  private static final Map<Long, OwnedBatch> IN_FLIGHT = new ConcurrentHashMap<>();

  private ArrowBatchHandles() {}

  public static long newOwner() {
    return NEXT_OWNER.incrementAndGet();
  }

  public static long register(ArrowBatch batch) {
    long handle = NEXT.incrementAndGet();
    IN_FLIGHT.put(handle, new OwnedBatch(batch.handleOwner(), batch));
    return handle;
  }

  public static ArrowBatch claim(long tokenHi, long tokenLo, long handle) {
    if (tokenHi != TOKEN_HI || tokenLo != TOKEN_LO) {
      throw new IllegalStateException(
          "zero-copy exchange handle crossed a process boundary: the columnar shuffle was planned"
              + " for a single-process deployment but producer and consumer run in different JVMs."
              + " Set streamfusion.exchange.zeroCopyLocal=false for multi-TaskManager deployments.");
    }
    OwnedBatch owned = IN_FLIGHT.remove(handle);
    if (owned == null) {
      throw new IllegalStateException(
          "zero-copy exchange handle " + handle + " was already claimed or never registered");
    }
    return owned.batch;
  }

  /** Closes every still-unclaimed batch emitted by one canceled or failed split subtask. */
  public static int releaseOwner(long owner) {
    if (owner == ArrowBatch.NO_HANDLE_OWNER) {
      return 0;
    }
    int released = 0;
    for (Map.Entry<Long, OwnedBatch> entry : IN_FLIGHT.entrySet()) {
      OwnedBatch owned = entry.getValue();
      if (owned.owner == owner && IN_FLIGHT.remove(entry.getKey(), owned)) {
        owned.batch.closeUnclaimed();
        released++;
      }
    }
    return released;
  }

  /** In-flight handle count, for leak assertions in tests. */
  public static int inFlight() {
    return IN_FLIGHT.size();
  }

  /** Total handles ever registered, for engagement assertions in tests. */
  public static long registered() {
    return NEXT.get();
  }

  private record OwnedBatch(long owner, ArrowBatch batch) {}
}
