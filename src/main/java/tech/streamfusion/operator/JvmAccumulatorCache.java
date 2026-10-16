package tech.streamfusion.operator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.netty4.io.netty.util.collection.LongObjectHashMap;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** Bounded accumulator views, flushed into managed state before checkpoint snapshots. */
final class JvmAccumulatorCache {
  private final int capacity;
  private final boolean intKey;
  private final boolean stringKey;
  private final Supplier<Object> currentKey;
  private final Counter hits;
  private final Counter bypasses;
  private final Consumer<Object> selectKey;
  private final List<CachedState> states = new ArrayList<>();

  JvmAccumulatorCache(
      int capacity,
      LogicalTypeRoot keyType,
      Supplier<Object> currentKey,
      Consumer<Object> selectKey,
      Counter hits,
      Counter bypasses) {
    this.capacity = capacity;
    this.intKey = keyType == LogicalTypeRoot.INTEGER;
    this.stringKey = keyType == LogicalTypeRoot.VARCHAR;
    this.currentKey = currentKey;
    this.hits = hits;
    this.bypasses = bypasses;
    this.selectKey = selectKey;
  }

  RuntimeContext wrap(RuntimeContext delegate) {
    return (RuntimeContext)
        Proxy.newProxyInstance(
            RuntimeContext.class.getClassLoader(),
            new Class<?>[] {RuntimeContext.class},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                  case "equals":
                    return proxy == args[0];
                  case "hashCode":
                    return System.identityHashCode(proxy);
                  default:
                    return "AccumulatorContext[" + delegate + "]";
                }
              }
              Object value;
              try {
                value = method.invoke(delegate, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
              // This is the released grouped runtime's accumulator state. DataViews remain direct.
              if (method.getName().equals("getState")
                  && args[0] instanceof ValueStateDescriptor
                  && ((ValueStateDescriptor<?>) args[0]).getName().equals("accState")) {
                @SuppressWarnings("unchecked")
                var state = new CachedState((ValueState<RowData>) value);
                states.add(state);
                return state;
              }
              return value;
            });
  }

  RowData keyFor(RowData input, int field, RowDataKeySelector selector) throws Exception {
    if (states.size() == 1) {
      RowData key = states.get(0).keyFor(input, field);
      if (key != null) return key;
    }
    return selector.getKey(input);
  }

  void flush() throws IOException {
    for (CachedState state : states) state.flush();
  }

  void invalidate() {
    states.forEach(CachedState::invalidate);
  }

  private static final class Entry {
    private final RowData key;
    private RowData value;
    private boolean dirty;
    private StringData text;
    private Entry next;

    private Entry(RowData key, RowData value) {
      this.key = key;
      this.value = value;
    }
  }

  private final class CachedState implements ValueState<RowData> {
    private final ValueState<RowData> delegate;
    private final LongObjectHashMap<Entry> values = new LongObjectHashMap<>();
    private int stringCount;
    private Entry probedEntry;
    private Entry nullEntry;
    private RowData lastKey;
    private Entry lastEntry;
    private boolean enabled = true;
    private boolean hitSinceClear;

    private CachedState(ValueState<RowData> delegate) {
      this.delegate = delegate;
    }

    private RowData keyFor(RowData input, int field) {
      if (!enabled) return null;
      Entry entry =
          input.isNullAt(field)
              ? nullEntry
              : stringKey
                  ? stringEntry(input.getString(field))
                  : values.get(intKey ? input.getInt(field) : input.getLong(field));
      if (stringKey) probedEntry = entry;
      return entry == null ? null : entry.key;
    }

    private Entry stringEntry(StringData text) {
      for (Entry entry = values.get(text.hashCode()); entry != null; entry = entry.next)
        if (entry.text.equals(text)) return entry;
      return null;
    }

    private void removeString(StringData text) {
      long hash = text.hashCode();
      Entry previous = null;
      for (Entry entry = values.get(hash); entry != null; entry = entry.next) {
        if (entry.text.equals(text)) {
          if (previous != null) previous.next = entry.next;
          else if (entry.next == null) values.remove(hash);
          else values.put(hash, entry.next);
          stringCount--;
          return;
        }
        previous = entry;
      }
    }

    private long keyValue(RowData key) {
      return intKey ? key.getInt(0) : key.getLong(0);
    }

    private int size() {
      return stringKey ? stringCount : values.size();
    }

    private boolean makeRoom() throws IOException {
      if (size() + (nullEntry == null ? 0 : 1) >= capacity) {
        boolean unused = !hitSinceClear;
        flush();
        invalidate();
        if (unused) {
          enabled = false;
          bypasses.inc();
        }
      }
      return enabled;
    }

    private void flush() throws IOException {
      if (size() == 0 && nullEntry == null) return;
      Object original = currentKey.get();
      try {
        for (Entry entry : values.values()) {
          if (stringKey) {
            for (Entry collision = entry; collision != null; collision = collision.next)
              flush(collision);
          } else flush(entry);
        }
        if (nullEntry != null) flush(nullEntry);
      } finally {
        if (original != null) selectKey.accept(original);
      }
    }

    private void flush(Entry entry) throws IOException {
      if (!entry.dirty) return;
      selectKey.accept(entry.key);
      delegate.update(entry.value);
      entry.dirty = false;
    }

    private void put(RowData key, Entry entry) {
      if (key.isNullAt(0)) nullEntry = entry;
      else if (stringKey) {
        entry.text = key.getString(0);
        removeString(entry.text);
        entry.next = values.put(entry.text.hashCode(), entry);
        stringCount++;
      } else values.put(keyValue(key), entry);
      probedEntry = null;
    }

    @Override
    public RowData value() throws IOException {
      if (!enabled) return delegate.value();
      RowData key = (RowData) currentKey.get();
      Entry entry =
          probedEntry != null && probedEntry.key == key
              ? probedEntry
              : key.isNullAt(0)
                  ? nullEntry
                  : stringKey ? stringEntry(key.getString(0)) : values.get(keyValue(key));
      if (entry == null) {
        if (!makeRoom()) return delegate.value();
        entry = new Entry(key, delegate.value());
        put(key, entry);
      } else {
        hitSinceClear = true;
        hits.inc();
      }
      lastKey = key;
      lastEntry = entry;
      return entry.value;
    }

    @Override
    public void update(RowData value) throws IOException {
      if (!enabled) {
        delegate.update(value);
        return;
      }
      RowData key = (RowData) currentKey.get();
      if (key == lastKey && lastEntry != null) lastEntry.value = value;
      else {
        if (!makeRoom()) {
          delegate.update(value);
          return;
        }
        lastKey = key;
        lastEntry = new Entry(key, value);
        put(key, lastEntry);
      }
      lastEntry.dirty = true;
    }

    @Override
    public void clear() {
      delegate.clear();
      if (!enabled) return;
      RowData key = (RowData) currentKey.get();
      if (key.isNullAt(0)) nullEntry = null;
      else if (stringKey) removeString(key.getString(0));
      else values.remove(keyValue(key));
      lastKey = null;
      lastEntry = null;
      probedEntry = null;
      if (size() == 0 && nullEntry == null) hitSinceClear = false;
    }

    private void invalidate() {
      values.clear();
      stringCount = 0;
      probedEntry = null;
      hitSinceClear = false;
      nullEntry = null;
      lastKey = null;
      lastEntry = null;
    }
  }
}
