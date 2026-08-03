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
 * out, exactly once. A record dropped between the two (job cancellation with records still in
 * network buffers) strands its batch until process exit — bounded by the in-flight buffer volume,
 * and only on failure paths, since aligned checkpoints and bounded jobs drain in-flight records.
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
  private static final Map<Long, ArrowBatch> IN_FLIGHT = new ConcurrentHashMap<>();

  private ArrowBatchHandles() {}

  public static long register(ArrowBatch batch) {
    long handle = NEXT.incrementAndGet();
    IN_FLIGHT.put(handle, batch);
    return handle;
  }

  public static ArrowBatch claim(long tokenHi, long tokenLo, long handle) {
    if (tokenHi != TOKEN_HI || tokenLo != TOKEN_LO) {
      throw new IllegalStateException(
          "zero-copy exchange handle crossed a process boundary: the columnar shuffle was planned"
              + " for a single-process deployment but producer and consumer run in different JVMs."
              + " Set streamfusion.exchange.zeroCopyLocal=false for multi-TaskManager deployments.");
    }
    ArrowBatch batch = IN_FLIGHT.remove(handle);
    if (batch == null) {
      throw new IllegalStateException(
          "zero-copy exchange handle " + handle + " was already claimed or never registered");
    }
    return batch;
  }

  /** In-flight handle count, for leak assertions in tests. */
  public static int inFlight() {
    return IN_FLIGHT.size();
  }

  /** Total handles ever registered, for engagement assertions in tests. */
  public static long registered() {
    return NEXT.get();
  }
}
