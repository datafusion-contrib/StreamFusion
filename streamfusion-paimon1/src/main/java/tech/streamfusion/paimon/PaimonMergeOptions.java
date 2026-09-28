package tech.streamfusion.paimon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.FirstRowMergeFunction;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.UserDefinedSeqComparator;

/** Released Paimon option parsing and validation for the native level-0 merge. */
public final class PaimonMergeOptions {
  private PaimonMergeOptions() {}

  private static final Set<String> HOST_AGGREGATES =
      Set.of(
          "collect",
          "merge_map",
          "merge_map_with_keytime",
          "nested_update",
          "nested_partial_update",
          "hll_sketch",
          "theta_sketch",
          "rbm32",
          "rbm64");

  private record MergePlan(Map<String, Object> settings, List<Map<String, Object>> fields) {}

  static Object[] aggregators(FileStoreTable table) {
    MergePlan plan = plan(table);
    Object[] kernels = new Object[plan.fields().size()];
    boolean any = false;
    for (int i = 0; i < kernels.length; i++) {
      var field = plan.fields().get(i);
      if (Boolean.TRUE.equals(field.get("host"))) {
        kernels[i] =
            new PaimonFieldAggregator(
                table.rowType().getFields().get(i),
                (String) field.get("function"),
                table.coreOptions());
        any = true;
      }
    }
    return any ? kernels : null;
  }

  public static String unsupportedReason(FileStoreTable table) {
    try {
      encode(table);
      return null;
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      return e.getMessage();
    }
  }

  public static String encode(FileStoreTable table) {
    return JsonSerdeUtil.toJson(plan(table).settings());
  }

  private static MergePlan plan(FileStoreTable table) {
    CoreOptions options = table.coreOptions();
    var rowType = table.rowType();
    List<String> names = rowType.getFieldNames();
    List<String> keys = table.primaryKeys();
    var engine = options.mergeEngine();
    switch (engine) {
      case DEDUPLICATE:
        break;
      case FIRST_ROW:
        FirstRowMergeFunction.factory(
                options.toConfiguration(),
                new org.apache.paimon.types.RowType(table.schema().trimmedPrimaryKeysFields()),
                rowType)
            .create(null);
        break;
      case PARTIAL_UPDATE:
        PartialUpdateMergeFunction.factory(options.toConfiguration(), rowType, keys).create(null);
        break;
      case AGGREGATE:
        AggregateMergeFunction.factory(
                options.toConfiguration(), names, rowType.getFieldTypes(), keys)
            .create(null);
        break;
      default:
        throw new UnsupportedOperationException("merge-engine " + engine + " is not supported");
    }
    UserDefinedSeqComparator.create(rowType, options);
    RowKindGenerator.create(table.schema(), options);
    List<Integer> sequence = ordinals(names, options.sequenceField());
    if (!sequence.isEmpty() && engine != CoreOptions.MergeEngine.DEDUPLICATE) {
      throw new UnsupportedOperationException(
          "sequence.field with " + engine + " requires the stock writer's merge grouping");
    }
    for (int column : sequence) {
      requireComparable(rowType.getFields().get(column), "sequence.field");
    }
    Map<Integer, List<Integer>> groups = new HashMap<>();
    List<String> sequenceFields = new ArrayList<>();
    List<String> protectedFields = new ArrayList<>();
    if (engine == CoreOptions.MergeEngine.PARTIAL_UPDATE) {
      for (var entry : options.toMap().entrySet()) {
        String key = entry.getKey();
        if (!key.startsWith("fields.") || !key.endsWith(".sequence-group")) {
          continue;
        }
        List<String> fields = Arrays.asList(key.substring(7, key.length() - 15).split(","));
        List<Integer> columns = ordinals(names, fields);
        for (int column : columns) {
          requireComparable(rowType.getFields().get(column), key);
        }
        List<String> protectedNames = Arrays.asList(entry.getValue().split(","));
        for (int column : ordinals(names, protectedNames)) {
          groups.put(column, columns);
        }
        for (int column : columns) {
          groups.put(column, columns);
        }
        sequenceFields.addAll(fields);
        protectedFields.addAll(protectedNames);
      }
    }
    List<Map<String, Object>> fields = new ArrayList<>();
    for (int column = 0; column < names.size(); column++) {
      String name = names.get(column);
      String function =
          engine == CoreOptions.MergeEngine.AGGREGATE
              ? aggregateFunction(name, options, keys, true)
              : engine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                  ? aggregateFunction(name, options, keys, false)
                  : null;
      if (function != null) {
        requireAggregator(rowType.getFields().get(column), function, options);
      }
      Map<String, Object> field = new HashMap<>();
      field.put("function", function);
      field.put("legacy_numeric", true);
      field.put("host", function != null && HOST_AGGREGATES.contains(function));
      field.put("ignore_retract", options.fieldAggIgnoreRetract(name));
      field.put("sequence_group", groups.getOrDefault(column, List.of()));
      field.put("delimiter", options.fieldListAggDelimiter(name));
      field.put("distinct", options.fieldCollectAggDistinct(name));
      fields.add(field);
    }
    Map<String, Object> plan = new HashMap<>();
    plan.put("engine", engine.toString());
    plan.put("sequence_columns", sequence);
    plan.put("sequence_ascending", options.sequenceFieldSortOrderIsAscending());
    plan.put("rowkind_column", options.rowkindField().map(names::indexOf).orElse(null));
    plan.put("ignore_update_before", false);
    plan.put("fields", fields);
    plan.put(
        "remove_on_delete",
        engine == CoreOptions.MergeEngine.PARTIAL_UPDATE
            ? options.toConfiguration().get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)
            : false);
    String removeGroup =
        options.toConfiguration().get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP);
    plan.put(
        "remove_on_sequence_group",
        removeGroup == null ? List.of() : ordinals(names, Arrays.asList(removeGroup.split(","))));
    return new MergePlan(plan, fields);
  }

  private static String aggregateFunction(
      String name, CoreOptions options, List<String> keys, boolean aggregate) {
    String function = options.fieldAggFunc(name);
    if (function == null) function = options.fieldsDefaultFunc();
    if (keys.contains(name) && (aggregate || function != null)) return "primary-key";
    return aggregate && function == null ? "last_non_null_value" : function;
  }

  private static List<Integer> ordinals(List<String> names, List<String> fields) {
    return fields.stream().map(names::indexOf).collect(Collectors.toList());
  }

  static VectorSchemaRoot defaults(FileStoreTable table, BufferAllocator allocator) {
    // Paimon 1.0 assigns defaults after merging and schema evolution, on reads.
    return null;
  }

  private static void requireComparable(DataField field, String option) {
    if (!PaimonKeyValueLayout.comparableNatively(field.type())
        && field.type().getTypeRoot() != DataTypeRoot.FLOAT
        && field.type().getTypeRoot() != DataTypeRoot.DOUBLE) {
      throw new UnsupportedOperationException(
          option + " cannot order " + field.name() + " of type " + field.type());
    }
  }

  private static void requireAggregator(DataField field, String function, CoreOptions options) {
    DataType type = field.type();
    if (HOST_AGGREGATES.contains(function)) {
      org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory.create(
          type,
          function,
          options.fieldAggIgnoreRetract(field.name()),
          false,
          options,
          field.name());
      return;
    }
    DataTypeRoot root = type.getTypeRoot();
    switch (function) {
      case "primary-key":
      case "first_value":
      case "last_value":
      case "first_non_null_value":
      case "first_not_null_value":
      case "last_non_null_value":
        return;
      case "min":
      case "max":
        requireComparable(field, function);
        return;
      case "sum":
      case "product":
        if (Set.of(
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE)
                .contains(root)
            || root == DataTypeRoot.DECIMAL) {
          return;
        }
        break;
      case "bool_and":
      case "bool_or":
        if (root == DataTypeRoot.BOOLEAN) {
          return;
        }
        break;
      case "listagg":
        if (root == DataTypeRoot.VARCHAR) {
          return;
        }
        break;
      default:
        break;
    }
    throw new UnsupportedOperationException(
        "aggregate function "
            + function
            + " for "
            + field.name()
            + " of type "
            + type
            + " (including its field options) is not supported natively");
  }
}
