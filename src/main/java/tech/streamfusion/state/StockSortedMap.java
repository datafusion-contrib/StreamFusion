package tech.streamfusion.state;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import org.apache.flink.api.java.tuple.Tuple2;

/** Read-only view of ordered spool entries for the released sorted-map serializer. */
final class StockSortedMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V> {
  private final List<Tuple2<K, V>> values;
  private final Comparator<? super K> comparator;

  StockSortedMap(List<Tuple2<K, V>> values, Comparator<? super K> comparator) {
    this.values = values;
    this.comparator = comparator;
  }

  @Override
  public Comparator<? super K> comparator() {
    return comparator;
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public K firstKey() {
    if (values.isEmpty()) throw new NoSuchElementException();
    return values.get(0).f0;
  }

  @Override
  public K lastKey() {
    if (values.isEmpty()) throw new NoSuchElementException();
    return values.get(values.size() - 1).f0;
  }

  private int lower(K key) {
    int low = 0, high = values.size();
    while (low < high) {
      int middle = low + (high - low) / 2;
      if (comparator.compare(values.get(middle).f0, key) < 0) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  @Override
  public SortedMap<K, V> subMap(K from, K to) {
    if (comparator.compare(from, to) > 0) throw new IllegalArgumentException("Reversed map range");
    return new StockSortedMap<>(values.subList(lower(from), lower(to)), comparator);
  }

  @Override
  public SortedMap<K, V> headMap(K to) {
    return new StockSortedMap<>(values.subList(0, lower(to)), comparator);
  }

  @Override
  public SortedMap<K, V> tailMap(K from) {
    return new StockSortedMap<>(values.subList(lower(from), values.size()), comparator);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return new AbstractSet<>() {
      @Override
      public int size() {
        return values.size();
      }

      @Override
      public Iterator<Entry<K, V>> iterator() {
        var source = values.iterator();
        return new Iterator<>() {
          private K previous;
          private boolean started;

          @Override
          public boolean hasNext() {
            return source.hasNext();
          }

          @Override
          public Entry<K, V> next() {
            var entry = source.next();
            if (started && comparator.compare(previous, entry.f0) >= 0)
              throw new IllegalStateException("Duplicate or unordered stock counter");
            previous = entry.f0;
            started = true;
            return new SimpleImmutableEntry<>(entry.f0, entry.f1);
          }
        };
      }
    };
  }
}
