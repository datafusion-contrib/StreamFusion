package tech.streamfusion.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

/** Expands search predicates without trimming their stored character endpoints. */
final class SearchArguments {
  private SearchArguments() {}

  static RexNode expandSearch(RexBuilder builder, RexProgram program, RexNode expression) {
    boolean[] character = {false};
    expression.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (call.getKind() == SqlKind.SEARCH
                && call.getOperands().size() == 2
                && SqlTypeFamily.CHARACTER.contains(call.getOperands().get(1).getType()))
              character[0] = true;
            return super.visitCall(call);
          }
        });
    if (!character[0]) return RexUtil.expandSearch(builder, program, expression);
    RexBuilder endpoints =
        new RexBuilder(builder.getTypeFactory()) {
          private RexLiteral endpoint(Object value, RelDataType type) {
            if (!SqlTypeFamily.CHARACTER.contains(type)
                || !(value instanceof NlsString || value instanceof String)) return null;
            NlsString text =
                value instanceof NlsString
                    ? (NlsString) value
                    : new NlsString((String) value, type.getCharset().name(), type.getCollation());
            return super.makeLiteral(
                text, getTypeFactory().createTypeWithNullability(type, false), SqlTypeName.CHAR);
          }

          @Override
          public RexNode makeLiteral(Object value, RelDataType type, boolean allowCast) {
            RexLiteral literal = endpoint(value, type);
            return literal != null ? literal : builder.makeLiteral(value, type, allowCast);
          }

          @Override
          public RexNode makeLiteral(
              Object value, RelDataType type, boolean allowCast, boolean trim) {
            RexLiteral literal = endpoint(value, type);
            return literal != null ? literal : builder.makeLiteral(value, type, allowCast, trim);
          }
        };
    return RexUtil.expandSearch(endpoints, program, expression);
  }
}
