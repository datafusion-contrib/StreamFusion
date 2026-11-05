package tech.streamfusion.paimon;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.paimon.flink.sink.AppendBypassCompactWorkerOperator;
import org.apache.paimon.flink.sink.AppendOnlySingleTableCompactionWorkerOperator;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;

/** Adapts the file worker in an already-built stock Paimon compaction graph before execution. */
public final class NativePaimonMaintenance {
  private NativePaimonMaintenance() {}

  /** Returns adapted worker count; actual native rewrites are reported by task metrics. */
  public static int accelerate(StreamExecutionEnvironment env) {
    return accelerate(env.getTransformations(), FlinkSink.isStreaming(env));
  }

  static int accelerate(Transformation<?> root, boolean streaming) {
    return accelerate(List.of(root), streaming);
  }

  private static int accelerate(Iterable<Transformation<?>> roots, boolean streaming) {
    var seen = Collections.newSetFromMap(new IdentityHashMap<Transformation<?>, Boolean>());
    var replacements = new LinkedHashMap<OneInputTransformation<?, ?>, StreamOperatorFactory<?>>();
    for (var root : roots) {
      for (var transformation : root.getTransitivePredecessors()) {
        if (!seen.add(transformation) || !(transformation instanceof OneInputTransformation<?, ?>))
          continue;
        var input = (OneInputTransformation<?, ?>) transformation;
        var factory = input.getOperatorFactory();
        if (factory.getClass() == AppendBypassCompactWorkerOperator.Factory.class
            || factory.getClass() == AppendOnlySingleTableCompactionWorkerOperator.Factory.class) {
          try {
            var table = (FileStoreTable) field(factory.getClass(), "table").get(factory);
            if (!table.primaryKeys().isEmpty() || !NativePaimonAppendRewriter.available()) continue;
            var user = (String) field(factory.getClass(), "commitUser").get(factory);
            boolean workerStreaming = PaimonVersion.appendWorkerStreaming(factory, streaming);
            var replacement =
                new NativePaimonAppendCompactWorker.Factory(
                    table,
                    user,
                    workerStreaming,
                    factory.getClass() == AppendBypassCompactWorkerOperator.Factory.class);
            replacement.setChainingStrategy(factory.getChainingStrategy());
            replacements.put(input, replacement);
          } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException(
                "Cannot adapt the released Paimon append worker", failure);
          }
          continue;
        }
        if (!PaimonVersion.isCompactionFactory(factory)) continue;
        try {
          var type = factory.getClass();
          var table = (FileStoreTable) field(type, "table").get(factory);
          var stock = (StoreSinkWrite.Provider) field(type, "storeSinkWriteProvider").get(factory);
          var provider = PaimonCompactionProvider.wrap(table, stock, streaming, false, false);
          if (provider == stock) continue;
          String user = (String) field(type, "initialCommitUser").get(factory);
          boolean full = field(type, "fullCompaction").getBoolean(factory);
          StreamOperatorFactory<?> replacement =
              PaimonVersion.compactionFactory(factory, table, provider, user, full);
          replacement.setChainingStrategy(factory.getChainingStrategy());
          replacements.put(input, replacement);
        } catch (ReflectiveOperationException failure) {
          throw new IllegalStateException(
              "Cannot adapt the released Paimon compaction factory", failure);
        }
      }
    }
    try {
      var operatorFactory = field(OneInputTransformation.class, "operatorFactory");
      for (var replacement : replacements.entrySet())
        operatorFactory.set(replacement.getKey(), replacement.getValue());
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot install the Paimon compaction worker", failure);
    }
    return replacements.size();
  }

  static Field field(Class<?> type, String name) throws NoSuchFieldException {
    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
      try {
        var field = current.getDeclaredField(name);
        if (!field.trySetAccessible()) throw new IllegalStateException("Cannot access " + field);
        return field;
      } catch (NoSuchFieldException absent) {
        // The released append factories inherit their immutable configuration fields.
      }
    }
    throw new NoSuchFieldException(type.getName() + "." + name);
  }
}
