package tech.streamfusion.doris;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

/** Reproduces the stock reader's zoned-timestamp interpretation inside the Doris query. */
final class DorisArrowProjection {
  private DorisArrowProjection() {}

  static String normalize(String projection, Schema schema, ZoneId zone) throws Exception {
    return normalize(projection, schema, schema, zone);
  }

  static String normalize(String projection, Schema schema, Schema expected, ZoneId zone)
      throws Exception {
    boolean change = false;
    for (int i = 0; i < schema.getFields().size(); i++)
      change |= changes(schema.getFields().get(i), expected.getFields().get(i), zone);
    if (!change) return projection;
    var select =
        (SqlSelect)
            SqlParser.create("SELECT " + projection, SqlParser.config().withLex(Lex.MYSQL))
                .parseQuery();
    var expressions = select.getSelectList();
    if (expressions.size() != schema.getFields().size())
      throw new IllegalArgumentException("Arrow projection and schema have different arities");
    List<String> result = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
      var expression = expressions.get(i);
      if (expression.getKind() == SqlKind.AS) expression = ((SqlCall) expression).operand(0);
      String sql = expression.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
      var field = schema.getFields().get(i);
      result.add(
          normalize(sql, field, expected.getFields().get(i), zone, 0)
              + " AS "
              + quote(field.getName()));
    }
    return String.join(", ", result);
  }

  private static boolean changes(Field actual, Field expected, ZoneId zone) {
    if (actual.getType() instanceof ArrowType.Timestamp) {
      String timezone = ((ArrowType.Timestamp) actual.getType()).getTimezone();
      return timezone != null
          && !(zone.getRules().isFixedOffset()
              && zone.getRules().getOffset(java.time.Instant.EPOCH).getTotalSeconds() == 0);
    }
    if (actual.getType() instanceof ArrowType.Decimal
        && expected.getType() instanceof ArrowType.Decimal
        && !actual.getType().equals(expected.getType())) return true;
    var targets = DorisArrowBuffers.logicalFields(expected.getChildren());
    if (actual.getChildren().size() != targets.size()) return false;
    for (int i = 0; i < actual.getChildren().size(); i++)
      if (changes(actual.getChildren().get(i), targets.get(i), zone)) return true;
    return false;
  }

  private static String normalize(String sql, Field field, Field expected, ZoneId zone, int depth) {
    if (!changes(field, expected, zone)) return sql;
    if (field.getType() instanceof ArrowType.Timestamp) {
      if (((ArrowType.Timestamp) field.getType()).getTimezone() == null) return sql;
      String from = ((ArrowType.Timestamp) field.getType()).getTimezone();
      String shifted =
          "CONVERT_TZ("
              + sql
              + ", '"
              + from.replace("'", "''")
              + "', '"
              + zone.getId().replace("'", "''")
              + "')";
      if (ZoneId.of(from).getRules().equals(ZoneId.of("UTC").getRules())) return shifted;
      // The scanner encodes its result in this zone again. Undo that second interpretation.
      return "CONVERT_TZ(" + shifted + ", 'UTC', '" + from.replace("'", "''") + "')";
    }
    if (field.getType() instanceof ArrowType.Decimal
        && expected.getType() instanceof ArrowType.Decimal) {
      var decimal = (ArrowType.Decimal) expected.getType();
      return "CAST("
          + sql
          + " AS DECIMAL("
          + decimal.getPrecision()
          + ","
          + decimal.getScale()
          + "))";
    }
    var targets = DorisArrowBuffers.logicalFields(expected.getChildren());
    if (field.getType() instanceof ArrowType.List) {
      String element = "_sf_doris_" + depth;
      return "ARRAY_MAP("
          + element
          + " -> "
          + normalize(element, field.getChildren().get(0), targets.get(0), zone, depth + 1)
          + ", "
          + sql
          + ")";
    }
    if (field.getType() instanceof ArrowType.Map) {
      var children = field.getChildren().get(0).getChildren();
      var target = targets.get(0).getChildren();
      String key = "_sf_doris_k" + depth;
      String value = "_sf_doris_v" + depth;
      return "MAP_FROM_ARRAYS(ARRAY_MAP("
          + key
          + " -> "
          + normalize(key, children.get(0), target.get(0), zone, depth + 1)
          + ", MAP_KEYS("
          + sql
          + ")), "
          + "ARRAY_MAP("
          + value
          + " -> "
          + normalize(value, children.get(1), target.get(1), zone, depth + 1)
          + ", MAP_VALUES("
          + sql
          + ")))";
    }
    List<String> children = new ArrayList<>();
    for (int i = 0; i < field.getChildren().size(); i++) {
      var child = field.getChildren().get(i);
      children.add("'" + child.getName().replace("'", "''") + "'");
      children.add(
          normalize(
              "STRUCT_ELEMENT(" + sql + ", '" + child.getName().replace("'", "''") + "')",
              child,
              targets.get(i),
              zone,
              depth + 1));
    }
    return "IF(" + sql + " IS NULL, NULL, NAMED_STRUCT(" + String.join(", ", children) + "))";
  }

  private static String quote(String name) {
    return "`" + name.replace("`", "``") + "`";
  }
}
