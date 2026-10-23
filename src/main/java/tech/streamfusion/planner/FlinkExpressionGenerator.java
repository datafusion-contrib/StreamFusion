package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.planner.FlinkExpressionFunction.Evaluator;
import tech.streamfusion.planner.FlinkExpressionFunction.ProcessingTimeContext;

/** Planner-only generation; workers receive only the generated function and runtime metadata. */
final class FlinkExpressionGenerator {
  private FlinkExpressionGenerator() {}

  static FlinkExpressionFunction generate(
      RexNode expression,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader) {
    return generate(expression, argumentTypes, config, classLoader, false);
  }

  static FlinkExpressionFunction generate(
      RexNode expression,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader,
      boolean binaryStringResult) {
    return generate(expression, argumentTypes, config, classLoader, binaryStringResult, true);
  }

  static FlinkExpressionFunction generate(
      RexNode expression,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader,
      boolean binaryStringResult,
      boolean exactDoubleTruncate) {
    return generate(
        scalarBody(
            expression,
            argumentTypes,
            config,
            classLoader,
            binaryStringResult,
            exactDoubleTruncate),
        argumentTypes,
        config,
        classLoader);
  }

  static FlinkExpressionFunction generate(
      List<RexNode> projections,
      RexNode condition,
      LogicalType[] argumentTypes,
      RowType resultType,
      ReadableConfig config,
      ClassLoader classLoader) {
    return generate(
        projections, condition, argumentTypes, resultType, config, classLoader, false);
  }

  static FlinkExpressionFunction generate(
      List<RexNode> projections,
      RexNode condition,
      LogicalType[] argumentTypes,
      RowType resultType,
      ReadableConfig config,
      ClassLoader classLoader,
      boolean preserveCollections) {
    return generate(
        rowBody(
            projections,
            condition,
            argumentTypes,
            resultType,
            config,
            classLoader),
        argumentTypes,
        config,
        classLoader,
        preserveCollections,
        condition != null);
  }

  private static final class Body {
    private final Context context;
    private final String code;
    private final RowType rowType;

    private Body(Context context, String code, RowType rowType) {
      this.context = context;
      this.code = code;
      this.rowType = rowType;
    }

    public Context context() {
      return context;
    }

    public String code() {
      return code;
    }

    public RowType rowType() {
      return rowType;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Body that = (Body) other;
      return java.util.Objects.equals(context, that.context)
          && java.util.Objects.equals(code, that.code)
          && java.util.Objects.equals(rowType, that.rowType);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(context);
      result = 31 * result + java.util.Objects.hashCode(code);
      result = 31 * result + java.util.Objects.hashCode(rowType);
      return result;
    }

    @Override
    public String toString() {
      return "Body[context=" + context + ", code=" + code + ", rowType=" + rowType + "]";
    }
  }

  static FlinkExpressionFunction doubleTruncate(
      RexCall call, LogicalType[] argumentTypes, ReadableConfig config, ClassLoader classLoader) {
    if (!RexExpression.isExactDoubleTruncate(call)) {
      throw new IllegalArgumentException("unverified DOUBLE TRUNCATE signature");
    }
    var context = new Context(config, classLoader);
    var generator = new ExactExpressionGenerator(context);
    generator.bindInput(RowType.of(argumentTypes), "input", scala.Option.empty());
    var value = generator.generateExpression(call.getOperands().get(0));
    var scale =
        call.getOperands().size() == 2
            ? generator.generateExpression(call.getOperands().get(1))
            : null;
    String code =
        context.reuseInputUnboxingCode()
            + value.code()
            + (scale == null ? "" : scale.code())
            + "\nif ("
            + value.nullTerm()
            + (scale == null ? "" : " || " + scale.nullTerm())
            + ") return null;\nreturn "
            + ExactDoubleTruncateFunction.class.getCanonicalName()
            + ".truncate("
            + value.resultTerm()
            + ", "
            + (scale == null ? "0" : scale.resultTerm())
            + ");\n";
    return generate(new Body(context, code, null), argumentTypes, config, classLoader);
  }

  private static Body scalarBody(
      RexNode expression,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader,
      boolean binaryStringResult,
      boolean exactDoubleTruncate) {
    var context = new Context(config, classLoader);
    String decimalCode = decimalRoundingText(expression);
    if (decimalCode != null) return new Body(context, decimalCode, null);
    var generator =
        exactDoubleTruncate
            ? new ExactExpressionGenerator(context)
            : new ExprCodeGenerator(context, false);
    generator.bindInput(RowType.of(argumentTypes), "input", scala.Option.empty());
    String temporal = temporalBody(expression, generator, context, config);
    if (temporal != null) return new Body(context, temporal, null);
    var result = generator.generateExpression(expression);
    return new Body(
        context,
        canonicalTemporalPrefix(expression, context, config)
            + context.reuseInputUnboxingCode()
            + result.code()
            + "\nif ("
            + result.nullTerm()
            + ") { return null; }\nreturn "
            + result.resultTerm()
            + (binaryStringResult ? ".toBytes()" : "")
            + ";\n",
        null);
  }

  private static String temporalBody(
      RexNode expression, ExprCodeGenerator generator, Context context, ReadableConfig config) {
    if (!(expression instanceof RexCall)
        || ((RexCall) expression).getOperator() != FlinkSqlOperatorTable.TRY_CAST
        || ((RexCall) expression).getOperands().size() != 1
        || !SqlTypeFamily.CHARACTER.contains(((RexCall) expression).getOperands().get(0).getType())
        || config
            .get(
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
            .isEnabled()) return null;
    RexCall call = ((RexCall) expression);

    SqlTypeName type = call.getType().getSqlTypeName();
    if (call.getOperands().get(0) instanceof RexInputRef) return null;
    String method;
    String suffix = "";
    if (type == SqlTypeName.DATE) method = "tryDate";
    else if (type == SqlTypeName.TIME) {
      method = "tryTime";
      var logical =
          (org.apache.flink.table.types.logical.TimeType)
              org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(call.getType());
      suffix = ", " + logical.getPrecision();
    } else if (type == SqlTypeName.TIMESTAMP
        || type == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      method = "tryTimestamp";
      suffix =
          ", "
              + call.getType().getPrecision()
              + ", "
              + (type == SqlTypeName.TIMESTAMP ? "null" : context.addReusableSessionTimeZone());
    } else return null;
    var input = generator.generateExpression(call.getOperands().get(0));
    // Child evaluation stays outside TRY_CAST's parser failure handler and executes exactly once.
    return context.reuseInputUnboxingCode()
        + input.code()
        + "\nif ("
        + input.nullTerm()
        + ") return null;\nreturn "
        + CanonicalTemporalParser.class.getCanonicalName()
        + "."
        + method
        + "("
        + input.resultTerm()
        + suffix
        + ");\n";
  }

  private static String canonicalTemporalPrefix(
      RexNode expression, Context context, ReadableConfig config) {
    Object inputCandidate;
    if (!(expression instanceof RexCall)
        || ((RexCall) expression).getOperator() != FlinkSqlOperatorTable.TRY_CAST
        || ((RexCall) expression).getOperands().size() != 1
        || !((inputCandidate = ((RexCall) expression).getOperands().get(0)) instanceof RexInputRef)
        || !SqlTypeFamily.CHARACTER.contains(((RexInputRef) inputCandidate).getType())) return "";
    RexInputRef input = ((RexInputRef) inputCandidate);

    RexCall call = ((RexCall) expression);

    SqlTypeName target = call.getType().getSqlTypeName();
    String parser = CanonicalTemporalParser.class.getCanonicalName();
    String inputTerm = "input.getString(" + input.getIndex() + ")";
    String resultType;
    String value;
    String result = "sfCanonicalTemporal";
    if (target == SqlTypeName.DATE || target == SqlTypeName.TIME) {
      if (config
          .get(
              org.apache.flink.table.api.config.ExecutionConfigOptions
                  .TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
          .isEnabled()) return "";
      resultType = "java.lang.Integer";
      value =
          parser
              + (target == SqlTypeName.DATE ? ".parseDate(" : ".parseTimeMillis(")
              + inputTerm
              + ")";
      if (target == SqlTypeName.TIME) {
        var logical =
            (org.apache.flink.table.types.logical.TimeType)
                org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(
                    call.getType());
        result =
            "tech.streamfusion.compat.RuntimeCompat.applyParsedTimePrecision(sfCanonicalTemporal, "
                + logical.getPrecision()
                + ")";
      }
    } else if (target == SqlTypeName.TIMESTAMP
        || target == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      resultType = "org.apache.flink.table.data.TimestampData";
      String zone = target == SqlTypeName.TIMESTAMP ? "null" : context.addReusableSessionTimeZone();
      value =
          parser + ".parse(" + inputTerm + ", " + call.getType().getPrecision() + ", " + zone + ")";
    } else return "";
    // Direct references can be inspected before the released fallback without repeating child
    // effects.
    return "if (!input.isNullAt("
        + input.getIndex()
        + ")) {\n"
        + resultType
        + " sfCanonicalTemporal = "
        + value
        + ";\n"
        + "if (sfCanonicalTemporal != null) return "
        + result
        + ";\n}\n";
  }

  static org.apache.calcite.rex.RexCall decimalRoundingTextCall(RexNode expression) {
    Object scaleCandidate;
    Object valueCandidate;
    Object roundCandidate;
    if (!(expression instanceof org.apache.calcite.rex.RexCall)
        || ((org.apache.calcite.rex.RexCall) expression).getKind()
            != org.apache.calcite.sql.SqlKind.CAST
        || ((org.apache.calcite.rex.RexCall) expression).getType().getSqlTypeName()
            != org.apache.calcite.sql.type.SqlTypeName.VARCHAR
        || ((org.apache.calcite.rex.RexCall) expression).getType().getPrecision()
            != Integer.MAX_VALUE
        || !((roundCandidate = ((org.apache.calcite.rex.RexCall) expression).getOperands().get(0))
            instanceof org.apache.calcite.rex.RexCall)
        || (((org.apache.calcite.rex.RexCall) roundCandidate).getOperator()
                != org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
            && ((org.apache.calcite.rex.RexCall) roundCandidate).getOperator()
                != org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE)
        || ((org.apache.calcite.rex.RexCall) roundCandidate).getOperands().size() != 2
        || !((valueCandidate =
                ((org.apache.calcite.rex.RexCall) roundCandidate).getOperands().get(0))
            instanceof org.apache.calcite.rex.RexInputRef)
        || !((scaleCandidate =
                ((org.apache.calcite.rex.RexCall) roundCandidate).getOperands().get(1))
            instanceof org.apache.calcite.rex.RexInputRef)
        || ((org.apache.calcite.rex.RexInputRef) valueCandidate).getType().getSqlTypeName()
            != org.apache.calcite.sql.type.SqlTypeName.DECIMAL
        || ((org.apache.calcite.rex.RexInputRef) scaleCandidate).getType().getSqlTypeName()
            != org.apache.calcite.sql.type.SqlTypeName.INTEGER) return null;
    org.apache.calcite.rex.RexCall round = ((org.apache.calcite.rex.RexCall) roundCandidate);

    return round;
  }

  private static String decimalRoundingText(RexNode expression) {
    var round = decimalRoundingTextCall(expression);
    if (round == null) return null;
    var value = (org.apache.calcite.rex.RexInputRef) round.getOperands().get(0);
    var scale = (org.apache.calcite.rex.RexInputRef) round.getOperands().get(1);
    return "return "
        + DecimalRounding.class.getCanonicalName()
        + ".text(input, "
        + value.getIndex()
        + ", "
        + scale.getIndex()
        + ", "
        + value.getType().getPrecision()
        + ", "
        + value.getType().getScale()
        + ", "
        + (round.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE)
        + ");\n";
  }

  private static Body rowBody(
      List<RexNode> projections,
      RexNode condition,
      LogicalType[] argumentTypes,
      RowType resultType,
      ReadableConfig config,
      ClassLoader classLoader) {
    var context = new Context(config, classLoader);
    String process =
        org.apache.flink.table.planner.codegen.CalcCodeGenerator$.MODULE$.generateProcessCode(
            context,
            RowType.of(argumentTypes),
            resultType,
            org.apache.flink.table.data.BoxedWrapperRowData.class,
            scala.collection.JavaConverters.asScalaBuffer(projections).toSeq(),
            scala.Option.apply(condition),
            "input",
            "this",
            true,
            false,
            true);
    return new Body(context, "rowResult = null;\n" + process + "\nreturn rowResult;\n", resultType);
  }

  private static FlinkExpressionFunction generate(
      Body body, LogicalType[] argumentTypes, ReadableConfig config, ClassLoader classLoader) {
    return generate(body, argumentTypes, config, classLoader, false, false);
  }

  private static FlinkExpressionFunction generate(
      Body body,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader,
      boolean preserveCollections,
      boolean rowFilter) {
    // Logical NOT NULL is enforced by Flink's sink; generated expressions can still overflow to
    // NULL.
    RowType rowResultType =
        body.rowType() == null
            ? null
            : new RowType(
                body.rowType().isNullable(),
                body.rowType().getFields().stream()
                    .map(field -> new RowType.RowField(field.getName(), field.getType().copy(true)))
                    .collect(tech.streamfusion.compat.ListCollectors.toList()));
    var context = body.context();
    byte[] arrowResultSchema =
        rowResultType == null
            ? null
            : tech.streamfusion.arrow.ArrowConversion.toArrowRowResultSchema(
                    rowResultType, preserveCollections)
                .serializeAsMessage();
    List<ScalarFunction> functions = List.copyOf(context.functionInstances.values());
    String className = tech.streamfusion.compat.FlinkCompat.expressionClassName(context);
    boolean numericBatch =
        rowResultType != null
            && rowResultType.getFieldCount() > 0
            && java.util.Arrays.stream(argumentTypes)
                .allMatch(FlinkExpressionGenerator::numericOrBoolean)
            && rowResultType.getChildren().stream()
                .allMatch(FlinkExpressionGenerator::numericOrBoolean);
    String code =
        "public final class "
            + className
            + " implements "
            + Evaluator.class.getCanonicalName()
            + " {\n"
            + "private "
            + ProcessingTimeContext.class.getCanonicalName()
            + " ctx;\n"
            + "public void bindProcessingTime("
            + ProcessingTimeService.class.getCanonicalName()
            + " service) { ctx = new "
            + ProcessingTimeContext.class.getCanonicalName()
            + "(service); }\n"
            + context.reuseMemberCode()
            + (rowResultType == null
                ? ""
                : "private "
                    + RowData.class.getCanonicalName()
                    + " rowResult;\npublic void collect("
                    + RowData.class.getCanonicalName()
                    + " row) { rowResult = row; }\n")
            + "public "
            + className
            + "(Object[] references) throws Exception {\n"
            + context.reuseInitCode()
            + "}\n"
            + context.reuseConstructorCode(className)
            + "public void open("
            + FunctionContext.class.getCanonicalName()
            + " context) throws Exception {\n"
            + context.reuseOpenCode()
            + "}\n"
            + "public Object eval("
            + RowData.class.getCanonicalName()
            + " input) throws Exception {\n"
            + context.reusePerRecordCode()
            + context.reuseLocalVariableCode(context.reuseLocalVariableCode$default$1())
            + body.code()
            + "}\n"
            + (!numericBatch
                ? ""
                : "public boolean tryEvalBatch("
                    + "tech.streamfusion.arrow.ArrowReader reader, "
                    + "tech.streamfusion.arrow.ArrowWriter writer, int rows) throws Exception {\n"
                    + "for (int row = 0; row < rows; row++) {\n"
                    + RowData.class.getCanonicalName()
                    + " value = ("
                    + RowData.class.getCanonicalName()
                    + ") eval(reader.read(row));\n"
                    + "if (value != null) writer.write(value);\n}\nreturn true;\n}\n")
            + "public void close() throws Exception {\n"
            + context.reuseCloseCode()
            + "}\n"
            + context.reuseInnerClassDefinitionCode()
            + "}\n";
    Object[] references =
        scala.collection.JavaConverters.seqAsJavaList(context.references()).toArray();
    GeneratedFunction<Evaluator> generated =
        new GeneratedFunction<>(className, code, references, config);
    generated.compile(classLoader);
    return new FlinkExpressionFunction(
        generated, argumentTypes, functions, rowResultType, arrowResultSchema, rowFilter);
  }

  private static boolean numericOrBoolean(LogicalType type) {
    return type.is(LogicalTypeFamily.NUMERIC) || type.getTypeRoot() == LogicalTypeRoot.BOOLEAN;
  }

  private static final class ExactExpressionGenerator extends ExprCodeGenerator {
    ExactExpressionGenerator(Context context) {
      super(context, false);
    }

    @Override
    public GeneratedExpression visitCall(RexCall call) {
      var generated = super.visitCall(call);
      if (!RexExpression.isExactDoubleTruncate(call)) {
        return generated;
      }
      String released =
          org.apache.flink.table.runtime.functions.SqlFunctionUtils.class.getCanonicalName()
              + ".struncate(";
      String code = generated.code();
      int offset = code.lastIndexOf(released);
      if (offset < 0) return generated;
      // Operand code precedes this call. Replace only its typed invocation, retaining
      // Flink's NULL guards/evaluation order and any DECIMAL calls in operand code.
      String exact =
          code.substring(0, offset)
              + ExactDoubleTruncateFunction.class.getCanonicalName()
              + ".truncate("
              + code.substring(offset + released.length());
      return generated.copy(
          generated.resultTerm(),
          generated.nullTerm(),
          exact,
          generated.resultType(),
          generated.literalValue());
    }
  }

  private static final class Context extends JvmFunctionCodeGeneratorContext {
    private final java.util.Map<String, String> functions = new java.util.HashMap<>();
    private final java.util.Map<String, ScalarFunction> functionInstances =
        new java.util.LinkedHashMap<>();

    Context(ReadableConfig config, ClassLoader classLoader) {
      super(config, classLoader, "context.getUserCodeClassLoader()");
    }

    @Override
    public String addReusableFunction(
        UserDefinedFunction function,
        Class<? extends FunctionContext> contextClass,
        scala.collection.Seq<String> contextArguments) {
      return functions.computeIfAbsent(
          function.functionIdentifier(),
          ignored -> {
            int reference = references().size();
            String name =
                addReusableObject(function, "expressionFunction", function.getClass().getName());
            // CodeGeneratorContext clones reusable objects. Retain the original reference here so
            // generated and direct call sites serialize and open the same task-local function
            // instance.
            references().update(reference, function);
            functionInstances.put(ignored, (ScalarFunction) function);
            return name;
          });
    }
  }
}
