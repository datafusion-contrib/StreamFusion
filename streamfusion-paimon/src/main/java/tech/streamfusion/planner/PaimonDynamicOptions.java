package tech.streamfusion.planner;

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.OptionsUtils;

/**
 * Paimon lets a job override table options from the Flink configuration with keys of the form
 * {@code paimon.<catalog>.<database>.<table>.<option>}, each segment optionally {@code *}. The
 * native sink resolves them from the planner's table config the same way so its table copy carries
 * exactly the options the stock sink would have seen.
 */
final class PaimonDynamicOptions {
  private PaimonDynamicOptions() {}

  static Map<String, String> forTable(RelNode rel, ObjectIdentifier identifier) {
    Map<String, String> all = ShortcutUtils.unwrapTableConfig(rel).getConfiguration().toMap();
    String tableName =
        new Identifier(identifier.getDatabaseName(), identifier.getObjectName()).getTableName();
    Pattern pattern =
        Pattern.compile(
            String.format(
                "(%s)(%s|\\*)\\.(%s|\\*)\\.(%s|\\*)\\.(.+)",
                FlinkConnectorOptions.TABLE_DYNAMIC_OPTION_PREFIX,
                Pattern.quote(identifier.getCatalogName()),
                Pattern.quote(identifier.getDatabaseName()),
                Pattern.quote(tableName)));
    return OptionsUtils.convertToDynamicTableProperties(all, "", pattern, 5);
  }
}
