package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.NativeAllocator;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyContextImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionDescription;

/**
 * Exactly-once Kafka sink with a native record data plane and Flink's stock Java commit plane. Rust
 * serializes and produces complete Arrow batches; real {@link KafkaCommittable}s preserve the
 * connector's checkpoint-completion and recovery behavior.
 */
public final class NativeKafkaExactlyOnceSink
    implements TwoPhaseCommittingStatefulSink<ArrowBatch, KafkaWriterState, KafkaCommittable>,
        Serializable {

  private final String topic;
  private final String transactionalIdPrefix;
  private final Properties javaProducerProperties;
  private final Map<String, String> nativeProducerConfig;
  private final long maxBlockMs;
  private final int maxRequestSize;
  private final boolean ignoreNullFields;
  private final String timestampFormat;
  private final String[] logicalTypes;
  private final String[] fieldNames;
  private final int[] keyFields;
  private final int[] valueFields;
  private final boolean upsert;
  private final KafkaSink<PreSerializedKafkaRecord> commitDelegate;

  /** A pre-warmed native producer and its coordinator-authoritative transaction identity. */
  private record WarmedProducer(long handle, long producerId, short epoch) {}

  public NativeKafkaExactlyOnceSink(
      String topic,
      String transactionalIdPrefix,
      Properties javaProducerProperties,
      Map<String, String> nativeProducerConfig,
      long maxBlockMs,
      int maxRequestSize,
      boolean ignoreNullFields,
      String timestampFormat,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert) {
    this.topic = topic;
    this.transactionalIdPrefix = transactionalIdPrefix;
    this.javaProducerProperties = copy(javaProducerProperties);
    this.nativeProducerConfig = Map.copyOf(nativeProducerConfig);
    this.maxBlockMs = maxBlockMs;
    this.maxRequestSize = maxRequestSize;
    this.ignoreNullFields = ignoreNullFields;
    this.timestampFormat = timestampFormat;
    this.logicalTypes = logicalTypes.clone();
    this.fieldNames = fieldNames.clone();
    this.keyFields = keyFields.clone();
    this.valueFields = valueFields.clone();
    this.upsert = upsert;
    this.commitDelegate =
        KafkaSink.<PreSerializedKafkaRecord>builder()
            .setKafkaProducerConfig(copy(javaProducerProperties))
            .setRecordSerializer(new PreSerializedKafkaRecordSchema(topic))
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix(transactionalIdPrefix)
            .setTransactionNamingStrategy(TransactionNamingStrategy.INCREMENTING)
            .build();
  }

  @Override
  public PrecommittingStatefulSinkWriter<ArrowBatch, KafkaWriterState, KafkaCommittable>
      createWriter(WriterInitContext context) throws IOException {
    return restoreWriter(context, List.of());
  }

  @Override
  public PrecommittingStatefulSinkWriter<ArrowBatch, KafkaWriterState, KafkaCommittable>
      restoreWriter(WriterInitContext context, Collection<KafkaWriterState> recoveredState)
          throws IOException {
    return new Writer(context, recoveredState);
  }

  @Override
  public Committer<KafkaCommittable> createCommitter(CommitterInitContext context)
      throws IOException {
    return commitDelegate.createCommitter(context);
  }

  @Override
  public SimpleVersionedSerializer<KafkaCommittable> getCommittableSerializer() {
    return commitDelegate.getCommittableSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KafkaWriterState> getWriterStateSerializer() {
    return commitDelegate.getWriterStateSerializer();
  }

  private final class Writer
      implements PrecommittingStatefulSinkWriter<ArrowBatch, KafkaWriterState, KafkaCommittable> {

    private final TaskInfo taskInfo;
    private final int[] ownedSubtaskIds;
    private final int totalOwnedSubtasks;
    private final ReadableBackchannel<TransactionFinished> backchannel;
    private final java.util.ArrayList<CheckpointTransaction> pendingTransactions;
    private final Counter batchesOut;
    private final Counter recordsOut;
    private final Counter bytesOut;
    private final Counter flushNanos;
    private final String[] configKeys;
    private final String[] configValues;
    private final ExecutorService producerWarmer;
    private final Admin adminClient;
    private CompletableFuture<WarmedProducer> warmingProducer;
    private boolean inputEnded;
    private long handle;
    private long warmedProducerId;
    private short warmedEpoch;
    private long currentCheckpointId;
    private String currentTransactionalId;
    private long currentRecords;
    private long[] flushedIdentity;

    private Writer(WriterInitContext context, Collection<KafkaWriterState> recoveredState)
        throws IOException {
      this.taskInfo = context.getTaskInfo();
      TransactionOwnership ownership = TransactionOwnership.IMPLICIT_BY_SUBTASK_ID;
      this.ownedSubtaskIds =
          ownership.getOwnedSubtaskIds(
              taskInfo.getIndexOfThisSubtask(),
              taskInfo.getNumberOfParallelSubtasks(),
              recoveredState);
      this.totalOwnedSubtasks =
          ownership.getTotalNumberOfOwnedSubtasks(
              taskInfo.getIndexOfThisSubtask(),
              taskInfo.getNumberOfParallelSubtasks(),
              recoveredState);
      this.pendingTransactions = new java.util.ArrayList<>();
      recoveredState.stream()
          .flatMap(state -> state.getPrecommittedTransactionalIds().stream())
          .forEach(pendingTransactions::add);
      this.backchannel =
          BackchannelFactory.getInstance()
              .getReadableBackchannel(
                  taskInfo.getIndexOfThisSubtask(),
                  taskInfo.getAttemptNumber(),
                  transactionalIdPrefix);
      this.batchesOut = context.metricGroup().counter("nativeKafkaProducerBatches");
      this.recordsOut = context.metricGroup().counter("nativeKafkaProducerRecords");
      this.bytesOut = context.metricGroup().counter("nativeKafkaProducerBytes");
      this.flushNanos = context.metricGroup().counter("nativeKafkaProducerFlushNanos");
      this.configKeys = nativeProducerConfig.keySet().toArray(new String[0]);
      this.configValues = nativeProducerConfig.values().toArray(new String[0]);
      this.producerWarmer =
          Executors.newSingleThreadExecutor(
              runnable -> {
                Thread thread =
                    new Thread(
                        runnable,
                        "native-kafka-producer-warmer-" + taskInfo.getIndexOfThisSubtask());
                thread.setDaemon(true);
                return thread;
              });

      this.adminClient = Admin.create(copy(javaProducerProperties));

      long restoredCheckpointId = context.getRestoredCheckpointId().orElse(0);
      try {
        abortLingeringTransactions(recoveredState, restoredCheckpointId + 1);
        prepareTransaction(restoredCheckpointId + 1);
      } catch (Throwable failure) {
        closeQuietly();
        throw new IOException("Failed to initialize native Kafka transaction", failure);
      }
    }

    @Override
    public void write(ArrowBatch element, Context context) throws IOException {
      ensureProducerStarted();
      try (VectorSchemaRoot root = element.root()) {
        int rows = root.getRowCount();
        BufferAllocator allocator =
            root.getFieldVectors().isEmpty()
                ? NativeAllocator.SHARED
                : root.getFieldVectors().get(0).getAllocator();
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
          long bytes =
              NativeKafka.produceKafkaJsonBatch(
                  handle,
                  topic,
                  array.memoryAddress(),
                  schema.memoryAddress(),
                  ignoreNullFields,
                  timestampFormat,
                  logicalTypes,
                  fieldNames,
                  keyFields,
                  valueFields,
                  upsert);
          currentRecords += rows;
          batchesOut.inc();
          recordsOut.inc(rows);
          bytesOut.inc(bytes);
        }
      } catch (RuntimeException failure) {
        throw new IOException("Native Kafka batch production failed", failure);
      }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
      inputEnded |= endOfInput;
      if (currentRecords == 0 || flushedIdentity != null) {
        return;
      }
      long started = System.nanoTime();
      try {
        long[] advisory = new long[2];
        NativeKafka.flushKafkaProducer(handle, transactionTimeoutMs(), advisory);
        if (advisory[0] >= 0 && (advisory[0] != warmedProducerId || advisory[1] != warmedEpoch)) {
          throw new IOException(
              "Kafka producer identity moved during transaction "
                  + currentTransactionalId
                  + ": expected "
                  + warmedProducerId
                  + ":"
                  + warmedEpoch
                  + ", observed "
                  + advisory[0]
                  + ":"
                  + advisory[1]);
        }
        flushedIdentity = new long[] {warmedProducerId, warmedEpoch};
      } catch (RuntimeException failure) {
        throw new IOException("Native Kafka transaction flush failed", failure);
      } finally {
        flushNanos.inc(System.nanoTime() - started);
      }
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() throws IOException {
      if (currentRecords == 0) {
        // INCREMENTING recovery probing relies on every transaction offset being initialized,
        // including empty checkpoints. The warmed producer's init_transactions already did that,
        // and a producer destroyed before its first record leaves nothing on the broker, so
        // consuming the warm-up is enough; the Java probe stays as the fallback when it failed.
        try {
          NativeKafka.closeKafkaProducer(takeWarmedProducer().handle());
        } catch (RuntimeException warmFailure) {
          try {
            probeTransaction(currentTransactionalId);
          } catch (RuntimeException failure) {
            failure.addSuppressed(warmFailure);
            throw new IOException(
                "Failed to initialize empty Kafka transaction " + currentTransactionalId, failure);
          }
        }
        closeCurrentProducer();
        return Collections.emptyList();
      }
      if (flushedIdentity == null) {
        throw new IOException("prepareCommit called before native Kafka flush");
      }
      KafkaCommittable committable =
          new KafkaCommittable(
              flushedIdentity[0], checkedEpoch(flushedIdentity[1]), currentTransactionalId, null);
      pendingTransactions.add(
          new CheckpointTransaction(currentTransactionalId, currentCheckpointId));
      closeCurrentProducer();
      return Collections.singletonList(committable);
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
      TransactionFinished finished;
      while ((finished = backchannel.poll()) != null) {
        String transactionId = finished.getTransactionId();
        pendingTransactions.removeIf(
            transaction -> transaction.getTransactionalId().equals(transactionId));
      }
      if (checkpointId < currentCheckpointId) {
        throw new IOException(
            "Kafka checkpoint moved backwards: transaction="
                + currentCheckpointId
                + ", checkpoint="
                + checkpointId);
      }
      for (long skipped = currentCheckpointId + 1; skipped <= checkpointId; skipped++) {
        String skippedTransactionalId =
            TransactionalIdFactory.buildTransactionalId(
                transactionalIdPrefix, ownedSubtaskIds[0], skipped);
        try {
          probeTransaction(skippedTransactionalId);
        } catch (RuntimeException failure) {
          throw new IOException(
              "Failed to initialize skipped Kafka transaction " + skippedTransactionalId, failure);
        }
      }
      prepareTransaction(checkpointId + 1);
      java.util.ArrayList<KafkaWriterState> states = new java.util.ArrayList<>();
      for (int index = 0; index < ownedSubtaskIds.length; index++) {
        states.add(
            new KafkaWriterState(
                transactionalIdPrefix,
                ownedSubtaskIds[index],
                totalOwnedSubtasks,
                TransactionOwnership.IMPLICIT_BY_SUBTASK_ID,
                index == 0 ? pendingTransactions : List.of()));
      }
      return states;
    }

    @Override
    public void close() {
      if (handle != 0) {
        try {
          if (currentRecords > 0 && flushedIdentity == null) {
            NativeKafka.abortKafkaTransaction(handle, transactionTimeoutMs());
          }
        } catch (RuntimeException ignored) {
          // Recovery probing is authoritative; close-time abort is best effort like stock Flink.
        } finally {
          closeCurrentProducer();
        }
      }
      CompletableFuture<WarmedProducer> warmed = warmingProducer;
      warmingProducer = null;
      if (warmed != null) {
        try {
          // Drain the in-flight warm-up synchronously so no native producer outlives the writer;
          // a never-begun transaction leaves nothing behind on the broker.
          NativeKafka.closeKafkaProducer(warmed.get(maxBlockMs, TimeUnit.MILLISECONDS).handle());
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          releaseWhenWarmed(warmed);
        } catch (java.util.concurrent.ExecutionException failed) {
          // The warm-up already released its producer on failure.
        } catch (java.util.concurrent.TimeoutException unreachableBroker) {
          // Cancellation must not hang on an unreachable broker; release whenever it finishes.
          releaseWhenWarmed(warmed);
        }
      }
      producerWarmer.shutdown();
      adminClient.close(java.time.Duration.ofMillis(maxBlockMs));
      backchannel.close();
    }

    private void prepareTransaction(long checkpointId) {
      currentCheckpointId = checkpointId;
      currentTransactionalId =
          TransactionalIdFactory.buildTransactionalId(
              transactionalIdPrefix, ownedSubtaskIds[0], checkpointId);
      handle = 0;
      currentRecords = 0;
      flushedIdentity = null;
      // Checkpoints after end of input snapshot state without prepareCommit, so an unconsumed
      // warm-up must be released here — and there is no point warming another.
      if (warmingProducer != null) {
        releaseWhenWarmed(warmingProducer);
        warmingProducer = null;
      }
      if (inputEnded) {
        return;
      }
      // Warm the epoch's producer off the record path: init_transactions and the identity
      // statistics tick run on the warmer thread while the previous epoch commits. Warming a
      // FRESH transactional id can never fence a pending transaction — only ids with a pending
      // committable must not be re-initialized, and INCREMENTING never reuses one. The
      // single-threaded warmer runs FIFO, so a released stale warm-up always completes (and
      // frees its producer) before the drain of a newer one observes completion.
      String transactionalId = currentTransactionalId;
      warmingProducer =
          CompletableFuture.supplyAsync(() -> openProducer(transactionalId), producerWarmer);
    }

    private WarmedProducer openProducer(String transactionalId) {
      long opened =
          NativeKafka.openTransactionalKafkaProducer(
              KafkaProducerConfigTranslator.ABI_VERSION,
              configKeys,
              configValues,
              transactionalId,
              maxBlockMs,
              maxRequestSize);
      try {
        // kafka-clients bounds initTransactions with max.block.ms, not the transaction timeout.
        NativeKafka.initKafkaTransactions(opened, maxBlockMs, new long[2]);
        // The transaction coordinator is the authoritative identity source (KIP-664); the
        // statistics tick is only an advisory cross-check, so nothing waits on the timer.
        TransactionDescription description =
            adminClient
                .describeTransactions(Collections.singleton(transactionalId))
                .description(transactionalId)
                .get(maxBlockMs, TimeUnit.MILLISECONDS);
        return new WarmedProducer(
            opened, description.producerId(), (short) description.producerEpoch());
      } catch (Exception failure) {
        NativeKafka.closeKafkaProducer(opened);
        if (failure instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw failure instanceof RuntimeException runtime
            ? runtime
            : new IllegalStateException(
                "Failed to resolve the producer identity of " + transactionalId, failure);
      }
    }

    private WarmedProducer takeWarmedProducer() {
      CompletableFuture<WarmedProducer> warmed = warmingProducer;
      warmingProducer = null;
      if (warmed == null) {
        return openProducer(currentTransactionalId);
      }
      try {
        return warmed.join();
      } catch (CompletionException failure) {
        Throwable cause = failure.getCause();
        throw cause instanceof RuntimeException runtime
            ? runtime
            : new IllegalStateException("Native Kafka producer warm-up failed", cause);
      }
    }

    private void ensureProducerStarted() throws IOException {
      if (handle != 0) {
        return;
      }
      try {
        WarmedProducer warmed = takeWarmedProducer();
        handle = warmed.handle();
        warmedProducerId = warmed.producerId();
        warmedEpoch = warmed.epoch();
        NativeKafka.beginKafkaTransaction(handle);
      } catch (RuntimeException failure) {
        closeCurrentProducer();
        throw new IOException(
            "Failed to start native Kafka transaction " + currentTransactionalId, failure);
      }
    }

    private void abortLingeringTransactions(
        Collection<KafkaWriterState> recoveredState, long startCheckpointId) {
      java.util.ArrayList<String> prefixes = new java.util.ArrayList<>();
      prefixes.add(transactionalIdPrefix);
      recoveredState.stream()
          .map(KafkaWriterState::getTransactionalIdPrefix)
          .filter(prefix -> !prefix.equals(transactionalIdPrefix))
          .distinct()
          .forEach(prefixes::add);
      java.util.Set<String> precommitted =
          recoveredState.stream()
              .flatMap(state -> state.getPrecommittedTransactionalIds().stream())
              .map(CheckpointTransaction::getTransactionalId)
              .collect(java.util.stream.Collectors.toSet());
      TransactionAbortStrategyContextImpl abortContext =
          new TransactionAbortStrategyContextImpl(
              () -> List.of(topic),
              taskInfo.getIndexOfThisSubtask(),
              taskInfo.getNumberOfParallelSubtasks(),
              ownedSubtaskIds,
              totalOwnedSubtasks,
              prefixes,
              startCheckpointId,
              this::probeTransaction,
              () -> org.apache.kafka.clients.admin.Admin.create(copy(javaProducerProperties)),
              precommitted);
      TransactionAbortStrategyImpl.PROBING.abortTransactions(abortContext);
    }

    private int probeTransaction(String transactionalId) {
      try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
          new FlinkKafkaInternalProducer<>(copy(javaProducerProperties), transactionalId)) {
        producer.initTransactions();
        producer.flush();
        return producer.getEpoch();
      }
    }

    private long transactionTimeoutMs() {
      return Long.parseLong(
          javaProducerProperties.getProperty(
              "transaction.timeout.ms",
              String.valueOf(KafkaProducerConfigTranslator.FLINK_TRANSACTION_TIMEOUT_MS)));
    }

    private short checkedEpoch(long epoch) throws IOException {
      if (epoch < 0 || epoch > Short.MAX_VALUE) {
        throw new IOException("Kafka producer epoch is outside the protocol range: " + epoch);
      }
      return (short) epoch;
    }

    private void closeCurrentProducer() {
      if (handle != 0) {
        NativeKafka.closeKafkaProducer(handle);
        handle = 0;
      }
    }

    private static void releaseWhenWarmed(CompletableFuture<WarmedProducer> warmed) {
      warmed.whenComplete(
          (opened, failure) -> {
            if (opened != null) {
              NativeKafka.closeKafkaProducer(opened.handle());
            }
          });
    }

    private void closeQuietly() {
      try {
        close();
      } catch (Throwable ignored) {
        // Preserve the initialization failure.
      }
    }
  }

  private static Properties copy(Properties properties) {
    Properties copy = new Properties();
    copy.putAll(properties);
    return copy;
  }
}
