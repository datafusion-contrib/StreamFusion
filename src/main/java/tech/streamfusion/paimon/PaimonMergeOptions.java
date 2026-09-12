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
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
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
import tech.streamfusion.operator.RowDataArrowConverter;

/** Released Paimon option parsing and validation for the native level-0 merge. */
public final class PaimonMergeOptions {
  private PaimonMergeOptions() {}

  public static String unsupportedReason(FileStoreTable table) {
    try {
      encode(table);
      return null;
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      return e.getMessage();
    }
  }

  public static String encode(FileStoreTable table) {
    CoreOptions options = table.coreOptions();
    var rowType = table.rowType();
    List<String> names = rowType.getFieldNames();
    List<String> keys = table.primaryKeys();
    var engine = options.mergeEngine();
    switch (engine) {
      case DEDUPLICATE:
        break;
      case FIRST_ROW:
        FirstRowMergeFunction.factory(options.toConfiguration()).create(null);
        break;
      case PARTIAL_UPDATE:
        PartialUpdateMergeFunction.factory(options.toConfiguration(), rowType, keys).create(null);
        break;
      case AGGREGATE:
        AggregateMergeFunction.factory(options.toConfiguration(), rowType, keys).create(null);
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
    for (DataField field : rowType.getFields()) {
      if (field.defaultValue() != null
          && (table.partitionKeys().contains(field.name())
              || table.schema().bucketKeys().contains(field.name())
              || keys.contains(field.name()))) {
        throw new UnsupportedOperationException(
            "default values on routing columns require the stock Paimon writer");
      }
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
              ? AggregateMergeFunction.getAggFuncName(name, options, keys, options.sequenceField())
              : engine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                  ? PartialUpdateMergeFunction.getAggFuncName(
                      name, options, keys, sequenceFields, protectedFields)
                  : null;
      if (function != null) {
        requireAggregator(rowType.getFields().get(column), function, options);
      }
      Map<String, Object> field = new HashMap<>();
      field.put("function", function);
      field.put("ignore_retract", options.fieldAggIgnoreRetract(name));
      field.put("sequence_group", groups.getOrDefault(column, List.of()));
      field.put("delimiter", options.fieldListAggDelimiter(name));
      fields.add(field);
    }
    Map<String, Object> plan = new HashMap<>();
    plan.put("engine", engine.toString());
    plan.put("sequence_columns", sequence);
    plan.put("sequence_ascending", options.sequenceFieldSortOrderIsAscending());
    plan.put("rowkind_column", options.rowkindField().map(names::indexOf).orElse(null));
    plan.put("ignore_update_before", options.ignoreUpdateBefore());
    plan.put("fields", fields);
    plan.put(
        "remove_on_delete",
        engine == CoreOptions.MergeEngine.PARTIAL_UPDATE
            ? options.toConfiguration().get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)
            : engine == CoreOptions.MergeEngine.AGGREGATE
                && options.aggregationRemoveRecordOnDelete());
    String removeGroup =
        options.toConfiguration().get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP);
    plan.put(
        "remove_on_sequence_group",
        removeGroup == null ? List.of() : ordinals(names, Arrays.asList(removeGroup.split(","))));
    return JsonSerdeUtil.toJson(plan);
  }

  private static List<Integer> ordinals(List<String> names, List<String> fields) {
    return fields.stream().map(names::indexOf).collect(Collectors.toList());
  }

  static VectorSchemaRoot defaults(FileStoreTable table, BufferAllocator allocator) {
    DefaultValueRow defaults = DefaultValueRow.create(table.rowType());
    return defaults == null
        ? null
        : RowDataArrowConverter.write(
            List.of(new FlinkRowData(defaults.defaultValueRow())),
            LogicalTypeConversion.toLogicalType(
                new org.apache.paimon.types.RowType(
                    table.rowType().getFields().stream()
                        .map(field -> field.copy(true))
                        .collect(Collectors.toList()))),
            allocator);
  }

  private static void requireComparable(DataField field, String option) {
    if (!PaimonKeyValueLayout.comparableNatively(field.type())) {
      throw new UnsupportedOperationException(
          option + " cannot order " + field.name() + " of type " + field.type());
    }
  }

  private static void requireAggregator(DataField field, String function, CoreOptions options) {
    DataType type = field.type();
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
            || function.equals("sum") && root == DataTypeRoot.DECIMAL) {
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
        if (root == DataTypeRoot.VARCHAR && !options.fieldCollectAggDistinct(field.name())) {
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
