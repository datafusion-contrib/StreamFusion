package tech.streamfusion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;

/** Observes host completion acknowledgements; it never marks a continuous source bounded. */
final class IcebergCorpusCompletion {
  static final long WATERMARK_DELAY_MILLIS = 4000;
  private static final Map<String, Progress> PROGRESS = new ConcurrentHashMap<>();
  private final int sources;
  private final int nativeSources;
  private final int nativeWriters;

  private IcebergCorpusCompletion(int sources, int nativeSources, int nativeWriters) {
    this.sources = sources;
    this.nativeSources = nativeSources;
    this.nativeWriters = nativeWriters;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static IcebergCorpusCompletion install(StreamGraph graph, Set<Long> snapshots) throws Exception {
    return install(graph, snapshots, false);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static IcebergCorpusCompletion install(StreamGraph graph, Set<Long> snapshots, boolean kafka)
      throws Exception {
    PROGRESS.clear();
    int count = 0, nativeSources = 0, nativeWriters = 0;
    var field = SourceOperatorFactory.class.getDeclaredField("source");
    field.setAccessible(true);
    for (var node : graph.getStreamNodes()) {
      if (node.getOperatorFactory()
              instanceof org.apache.flink.streaming.api.operators.SimpleOperatorFactory<?> simple
          && simple
              .getOperator()
              .getClass()
              .getName()
              .equals("org.apache.iceberg.flink.sink.IcebergStreamWriter")) {
        var writerField = simple.getOperator().getClass().getDeclaredField("taskWriterFactory");
        writerField.setAccessible(true);
        if (writerField
            .get(simple.getOperator())
            .getClass()
            .getName()
            .equals("tech.streamfusion.iceberg.IcebergTaskWriterFactory")) nativeWriters++;
      }
      if (!(node.getOperatorFactory() instanceof SourceOperatorFactory factory)) continue;
      var source = (Source<?, ?, ?>) field.get(factory);
      String type = source.getClass().getName();
      if (kafka) {
        if (type.equals("tech.streamfusion.kafka.NativeKafkaSource")) {
          count++;
          nativeSources++;
        } else if (type.equals("org.apache.flink.connector.kafka.source.KafkaSource")) count++;
        continue;
      }
      if (!type.equals("org.apache.iceberg.flink.source.IcebergSource")
          && !type.equals("tech.streamfusion.iceberg.IcebergArrowSource")) continue;
      if (type.equals("tech.streamfusion.iceberg.IcebergArrowSource")) nativeSources++;
      delayWatermarks(source);
      String id = Integer.toString(count++);
      PROGRESS.put(id, new Progress());
      field.set(factory, new ObservedSource(source, id, Set.copyOf(snapshots)));
    }
    if (count == 0) throw new IllegalStateException("No expected sources in benchmark graph");
    return new IcebergCorpusCompletion(count, nativeSources, nativeWriters);
  }

  private static void delayWatermarks(Source<?, ?, ?> source) throws Exception {
    Object host = source;
    if (source.getClass().getName().equals("tech.streamfusion.iceberg.IcebergArrowSource")) {
      var delegate = source.getClass().getDeclaredField("delegate");
      delegate.setAccessible(true);
      host = delegate.get(source);
    }
    var emitterField = host.getClass().getDeclaredField("emitter");
    emitterField.setAccessible(true);
    Object emitter = emitterField.get(host);
    if (!emitter
        .getClass()
        .getName()
        .equals("org.apache.iceberg.flink.source.reader.WatermarkExtractorRecordEmitter"))
      throw new IllegalStateException("Benchmark requires Iceberg's split watermark emitter");
    var extractorField = emitter.getClass().getDeclaredField("timeExtractor");
    extractorField.setAccessible(true);
    var extractor =
        (org.apache.iceberg.flink.source.reader.SplitWatermarkExtractor)
            extractorField.get(emitter);
    extractorField.set(
        emitter,
        (org.apache.iceberg.flink.source.reader.SplitWatermarkExtractor)
            split -> {
              long lowerBound = extractor.extractWatermark(split);
              return lowerBound < Long.MIN_VALUE + WATERMARK_DELAY_MILLIS
                  ? Long.MIN_VALUE
                  : lowerBound - WATERMARK_DELAY_MILLIS;
            });
  }

  int nativeSourceCount() {
    return nativeSources;
  }

  int nativeWriterCount() {
    return nativeWriters;
  }

  boolean complete() {
    return PROGRESS.size() == sources && PROGRESS.values().stream().allMatch(p -> p.done.get());
  }

  long nativeTasks() {
    return PROGRESS.values().stream()
        .flatMap(p -> p.nativeTasks.values().stream())
        .mapToLong(Counter::getCount)
        .sum();
  }

  long fallbackTasks() {
    return PROGRESS.values().stream()
        .flatMap(p -> p.fallbackTasks.values().stream())
        .mapToLong(Counter::getCount)
        .sum();
  }

  int sourceCount() {
    return sources;
  }

  private static final class Progress {
    final AtomicBoolean done = new AtomicBoolean();
    final Map<Integer, Counter> nativeTasks = new ConcurrentHashMap<>();
    final Map<Integer, Counter> fallbackTasks = new ConcurrentHashMap<>();
  }

  private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException failure) {
      throw failure.getCause();
    }
  }

  private static final class ObservedSource<T>
      implements Source<T, IcebergSourceSplit, IcebergEnumeratorState> {
    private final Source<T, IcebergSourceSplit, IcebergEnumeratorState> delegate;
    private final String id;
    private final Set<Long> snapshots;

    private ObservedSource(
        Source<T, IcebergSourceSplit, IcebergEnumeratorState> delegate,
        String id,
        Set<Long> snapshots) {
      this.delegate = delegate;
      this.id = id;
      this.snapshots = snapshots;
    }

    public Boundedness getBoundedness() {
      return delegate.getBoundedness();
    }

    public SourceReader<T, IcebergSourceSplit> createReader(SourceReaderContext context)
        throws Exception {
      SourceReaderMetricGroup group = context.metricGroup();
      var observed =
          (SourceReaderMetricGroup)
              Proxy.newProxyInstance(
                  SourceReaderMetricGroup.class.getClassLoader(),
                  new Class<?>[] {SourceReaderMetricGroup.class},
                  (proxy, method, args) -> {
                    Object value = invoke(group, method, args);
                    if (method.getName().equals("counter")
                        && args != null
                        && args[0] instanceof String name
                        && value instanceof Counter counter) {
                      var progress = PROGRESS.get(id);
                      if (name.equals("nativeParquetTasks"))
                        progress.nativeTasks.put(context.getIndexOfSubtask(), counter);
                      if (name.equals("stockFallbackTasks"))
                        progress.fallbackTasks.put(context.getIndexOfSubtask(), counter);
                    }
                    return value;
                  });
      var readerContext =
          (SourceReaderContext)
              Proxy.newProxyInstance(
                  SourceReaderContext.class.getClassLoader(),
                  new Class<?>[] {SourceReaderContext.class},
                  (proxy, method, args) ->
                      method.getName().equals("metricGroup")
                          ? observed
                          : invoke(context, method, args));
      return delegate.createReader(readerContext);
    }

    public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
        SplitEnumeratorContext<IcebergSourceSplit> context) throws Exception {
      return enumerator(context, null);
    }

    public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> restoreEnumerator(
        SplitEnumeratorContext<IcebergSourceSplit> context, IcebergEnumeratorState state)
        throws Exception {
      return enumerator(context, state);
    }

    @SuppressWarnings("unchecked")
    private SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> enumerator(
        SplitEnumeratorContext<IcebergSourceSplit> context, IcebergEnumeratorState restored)
        throws Exception {
      Set<String> assigned = new HashSet<>();
      var observed =
          (SplitEnumeratorContext<IcebergSourceSplit>)
              Proxy.newProxyInstance(
                  SplitEnumeratorContext.class.getClassLoader(),
                  new Class<?>[] {SplitEnumeratorContext.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("assignSplit"))
                      assigned.add(((IcebergSourceSplit) args[0]).splitId());
                    if (method.getName().equals("assignSplits"))
                      ((SplitsAssignment<IcebergSourceSplit>) args[0])
                          .assignment()
                          .values()
                          .forEach(
                              splits -> splits.forEach(split -> assigned.add(split.splitId())));
                    return invoke(context, method, args);
                  });
      var host =
          restored == null
              ? delegate.createEnumerator(observed)
              : delegate.restoreEnumerator(observed, restored);
      return new SplitEnumerator<>() {
        private void probe() {
          try {
            var state = host.snapshotState(-1);
            Object position = state.lastEnumeratedPosition();
            Long snapshot = null;
            if (position != null) {
              var method = position.getClass().getDeclaredMethod("snapshotId");
              method.setAccessible(true);
              snapshot = (Long) method.invoke(position);
            }
            boolean discovered = snapshot != null && snapshots.contains(snapshot);
            PROGRESS
                .get(id)
                .done
                .set(discovered && state.pendingSplits().isEmpty() && assigned.isEmpty());
          } catch (Exception failure) {
            throw new IllegalStateException("Cannot observe Iceberg corpus completion", failure);
          }
        }

        public void start() {
          host.start();
          context.callAsync(
              () -> Boolean.TRUE,
              (ignored, failure) -> {
                if (failure != null) throw new IllegalStateException(failure);
                probe();
              },
              0,
              50);
        }

        public void handleSplitRequest(int subtask, String hostname) {
          host.handleSplitRequest(subtask, hostname);
        }

        public void addSplitsBack(java.util.List<IcebergSourceSplit> splits, int subtask) {
          splits.forEach(split -> assigned.remove(split.splitId()));
          host.addSplitsBack(splits, subtask);
        }

        public void addReader(int subtask) {
          host.addReader(subtask);
        }

        public void handleSourceEvent(int subtask, SourceEvent event) {
          if (event instanceof SplitRequestEvent request)
            assigned.removeAll(request.finishedSplitIds());
          host.handleSourceEvent(subtask, event);
          probe();
        }

        public IcebergEnumeratorState snapshotState(long checkpoint) throws Exception {
          return host.snapshotState(checkpoint);
        }

        public void notifyCheckpointComplete(long checkpoint) throws Exception {
          host.notifyCheckpointComplete(checkpoint);
        }

        public void notifyCheckpointAborted(long checkpoint) throws Exception {
          host.notifyCheckpointAborted(checkpoint);
        }

        public void close() throws java.io.IOException {
          host.close();
        }
      };
    }

    public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
      return delegate.getSplitSerializer();
    }

    public SimpleVersionedSerializer<IcebergEnumeratorState> getEnumeratorCheckpointSerializer() {
      return delegate.getEnumeratorCheckpointSerializer();
    }
  }
}
