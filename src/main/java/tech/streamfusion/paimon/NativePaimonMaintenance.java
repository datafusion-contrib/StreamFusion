package tech.streamfusion.paimon;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.StoreCompactOperator;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.FileStoreTable;

/** Adapts the file worker in an already-built stock Paimon compaction graph before execution. */
public final class NativePaimonMaintenance {
  private NativePaimonMaintenance() {}

  /** Returns adapted worker count; actual native rewrites are reported by task metrics. */
  public static int accelerate(StreamExecutionEnvironment env) {
    var seen = Collections.newSetFromMap(new IdentityHashMap<Transformation<?>, Boolean>());
    var replacements = new LinkedHashMap<OneInputTransformation<?, ?>, StreamOperatorFactory<?>>();
    for (var root : env.getTransformations()) {
      for (var transformation : root.getTransitivePredecessors()) {
        if (!seen.add(transformation) || !(transformation instanceof OneInputTransformation<?, ?>))
          continue;
        var input = (OneInputTransformation<?, ?>) transformation;
        var factory = input.getOperatorFactory();
        boolean coordinated = factory.getClass() == StoreCompactOperator.CoordinatedFactory.class;
        if (!coordinated && factory.getClass() != StoreCompactOperator.Factory.class) continue;
        try {
          var type = factory.getClass();
          var table = (FileStoreTable) field(type, "table").get(factory);
          var stock = (StoreSinkWrite.Provider) field(type, "storeSinkWriteProvider").get(factory);
          var provider =
              PaimonCompactionProvider.wrap(table, stock, FlinkSink.isStreaming(env), false, false);
          if (provider == stock) continue;
          String user = (String) field(type, "initialCommitUser").get(factory);
          boolean full = field(type, "fullCompaction").getBoolean(factory);
          StreamOperatorFactory<?> replacement =
              coordinated
                  ? new StoreCompactOperator.CoordinatedFactory(table, provider, user, full)
                  : new StoreCompactOperator.Factory(table, provider, user, full);
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

  private static Field field(Class<?> type, String name) throws NoSuchFieldException {
    var field = type.getDeclaredField(name);
    if (!field.trySetAccessible()) throw new IllegalStateException("Cannot access " + field);
    return field;
  }
}
