package tech.streamfusion.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discretionary state-table shaping on a background thread — the RocksDB model: deep compactions
 * never run on the write path. The barrier's synchronous round is only the minimal level-0
 * up-leveling; this thread runs the ordinary universal picks that bound run counts and space
 * amplification, kicked after each barrier and coalescing while a round is in flight. The
 * strategy's round callback serializes shaping against the barrier rounds on the compaction
 * mutex — Paimon supports exactly one compactor per table at a time (concurrent picks can select
 * the same input files and the loser's commit fails); a round usually fits between barriers, and
 * a barrier arriving mid-round waits for it.
 *
 * <p>Shaping is best-effort by design: on a deletion-vector table every level-1+ file reads
 * correct standalone however far shaping lags, so a failed or slow round costs scan work, never
 * results. A shaping commit racing the barrier's commits is resolved by Paimon's optimistic
 * retry; the store's local GC only ever deletes files it previously listed as live, so an
 * in-flight round's fresh outputs cannot be swept.
 */
final class PaimonTableShaping implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonTableShaping.class);

  /** One shaping round over every table the strategy maintains. */
  interface ShapeRound {
    void run(long round) throws Exception;
  }

  private final ShapeRound rounds;
  private final Thread thread;
  private final Object lock = new Object();
  private boolean kicked;
  private boolean closed;
  /** Monotonic across restarts (Paimon dedupes re-committed identifiers per commit user). */
  private long round = System.currentTimeMillis();

  PaimonTableShaping(ShapeRound rounds) {
    this.rounds = rounds;
    this.thread = new Thread(this::run, "paimon-state-shaping");
    this.thread.setDaemon(true);
    this.thread.start();
  }

  /** Signals that a barrier committed and up-leveled new runs; coalesces while a round runs. */
  void kick() {
    synchronized (lock) {
      kicked = true;
      lock.notifyAll();
    }
  }

  private void run() {
    while (true) {
      synchronized (lock) {
        while (!kicked && !closed) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            return;
          }
        }
        if (closed) {
          return;
        }
        kicked = false;
      }
      try {
        rounds.run(++round);
      } catch (Exception e) {
        LOG.warn("state-table shaping round failed; the next barrier retries it", e);
      }
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      closed = true;
      lock.notifyAll();
    }
    // Let an in-flight round finish (its commit is safe either way) before resorting to an
    // interrupt; the table directory is deleted right after this returns.
    try {
      thread.join(10_000);
      if (thread.isAlive()) {
        thread.interrupt();
        thread.join(1_000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
