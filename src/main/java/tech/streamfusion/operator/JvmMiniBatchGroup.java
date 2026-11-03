package tech.streamfusion.operator;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Collector;

/** Released bundle evaluation with independently owned keys and copied input rows. */
final class JvmMiniBatchGroup<V> {
  private final MapBundleFunction<RowData, V, RowData, RowData> function;
  private final BundleTrigger<RowData> trigger;
  private final RowDataSerializer keys;
  private final Collector<RowData> output;
  private final Counter rows;
  private final Counter bundles;
  private final Map<RowData, V> buffer = new HashMap<>();

  JvmMiniBatchGroup(
      MapBundleFunction<RowData, V, RowData, RowData> function,
      BundleTrigger<RowData> trigger,
      RowDataSerializer keys,
      Collector<RowData> output,
      Counter rows,
      Counter bundles) {
    this.function = function;
    this.trigger = trigger;
    this.keys = keys;
    this.output = output;
    this.rows = rows;
    this.bundles = bundles;
  }

  void open(ExecutionContext context) throws Exception {
    function.open(context);
    trigger.registerCallback(this::finishBundle);
    trigger.reset();
  }

  void add(RowData key, RowData input) throws Exception {
    var existing = buffer.get(key);
    var value = function.addInput(existing, input);
    buffer.put(existing == null ? keys.copy(key) : key, value);
    rows.inc();
    trigger.onElement(input);
  }

  void finishBundle() throws Exception {
    if (!buffer.isEmpty()) {
      function.finishBundle(buffer, output);
      buffer.clear();
      bundles.inc();
    }
    trigger.reset();
  }

  void close() {
    try {
      // Match AbstractMapBundleOperator's released rich-function closing contract.
      FunctionUtils.closeFunction(function);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      org.slf4j.LoggerFactory.getLogger(JvmMiniBatchGroup.class)
          .warn("Closing JVM aggregate bundle", interrupted);
    } catch (Exception failure) {
      org.slf4j.LoggerFactory.getLogger(JvmMiniBatchGroup.class)
          .warn("Closing JVM aggregate bundle", failure);
    } finally {
      buffer.clear();
    }
  }
}
