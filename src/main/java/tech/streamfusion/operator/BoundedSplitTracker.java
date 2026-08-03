package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Bounded-read bookkeeping shared by the native split readers: the concrete stopping offset per
 * tracked split, the last position seen, and the splits already reported finished. A split finishes
 * once its position reaches its stopping offset; a split known to be complete before any fetch is
 * queued as pending and reported on the next fetch.
 *
 * @param <T> the reader's per-split handle (whatever its native unassign call needs)
 */
public final class BoundedSplitTracker<T> {

  private final Map<String, Long> stoppingOffsets = new HashMap<>();
  private final Map<String, Long> positions = new HashMap<>();
  private final Map<String, T> splitsById = new HashMap<>();
  private final Set<String> finished = new HashSet<>();
  private final Set<String> pendingFinished = new HashSet<>();

  public void track(String splitId, T split, long startingOffset, OptionalLong stoppingOffset) {
    splitsById.put(splitId, split);
    positions.put(splitId, startingOffset);
    stoppingOffset.ifPresent(stop -> stoppingOffsets.put(splitId, stop));
  }

  public void markPendingFinished(String splitId) {
    pendingFinished.add(splitId);
  }

  /** Drains the splits queued as finished before any fetch, marking them reported. */
  public Set<String> drainPendingFinished() {
    Set<String> drained = Set.copyOf(pendingFinished);
    finished.addAll(pendingFinished);
    pendingFinished.clear();
    return drained;
  }

  public void recordPosition(String splitId, long position) {
    positions.put(splitId, position);
  }

  public T tracked(String splitId) {
    return splitsById.get(splitId);
  }

  public List<T> trackedSplits() {
    return List.copyOf(splitsById.values());
  }

  /** Reports each split whose position has reached its stopping offset, once, and returns them. */
  public List<T> finishReached(Consumer<String> reportFinished) {
    List<T> justFinished = new ArrayList<>();
    for (Map.Entry<String, Long> stop : stoppingOffsets.entrySet()) {
      String splitId = stop.getKey();
      if (!finished.contains(splitId)
          && positions.getOrDefault(splitId, Long.MIN_VALUE) >= stop.getValue()) {
        reportFinished.accept(splitId);
        finished.add(splitId);
        justFinished.add(splitsById.get(splitId));
      }
    }
    return justFinished;
  }

  /** Forgets the split; if it was tracked but never reported finished, queues it as finished. */
  public T retire(String splitId) {
    T split = splitsById.remove(splitId);
    stoppingOffsets.remove(splitId);
    positions.remove(splitId);
    if (split != null && !finished.remove(splitId)) {
      pendingFinished.add(splitId);
    }
    return split;
  }
}
