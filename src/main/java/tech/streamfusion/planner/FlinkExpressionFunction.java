package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Flink's generated expression code, called once per argument batch through the scalar bridge. */
public final class FlinkExpressionFunction extends ScalarFunction
    implements tech.streamfusion.operator.NativeUdf.FunctionDependencies,
        tech.streamfusion.operator.NativeUdf.RowResult {
  private static final long serialVersionUID = 1L;

  public interface Evaluator extends Function {
    Object eval(RowData input) throws Exception;

    void open(FunctionContext context) throws Exception;

    void close() throws Exception;
  }

  private final GeneratedFunction<Evaluator> generated;
  private final LogicalType[] argumentTypes;
  private final List<ScalarFunction> functions;
  private final RowType rowResultType;
  private transient Evaluator evaluator;
  private transient GenericRowData input;

  FlinkExpressionFunction(
      RexNode expression,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader) {
    this(
        scalarBody(expression, argumentTypes, config, classLoader),
        argumentTypes,
        config,
        classLoader);
  }

  FlinkExpressionFunction(
      List<RexNode> projections,
      RexNode condition,
      LogicalType[] argumentTypes,
      RowType resultType,
      ReadableConfig config,
      ClassLoader classLoader) {
    this(
        rowBody(projections, condition, argumentTypes, resultType, config, classLoader),
        argumentTypes,
        config,
        classLoader);
  }

  private record Body(Context context, String code, RowType rowType) {}

  private static Body scalarBody(
      RexNode expression,
      LogicalType[] argumentTypes,
      ReadableConfig config,
      ClassLoader classLoader) {
    var context = new Context(config, classLoader);
    var generator = new ExprCodeGenerator(context, false);
    generator.bindInput(RowType.of(argumentTypes), "input", scala.Option.empty());
    var result = generator.generateExpression(expression);
    return new Body(
        context,
        context.reuseInputUnboxingCode()
            + result.code()
            + "\nif ("
            + result.nullTerm()
            + ") { return null; }\nreturn "
            + result.resultTerm()
            + ";\n",
        null);
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
            GenericRowData.class,
            scala.collection.JavaConverters.asScalaBuffer(projections).toSeq(),
            scala.Option.apply(condition),
            "input",
            "this",
            true,
            false,
            true);
    return new Body(context, "rowResult = null;\n" + process + "\nreturn rowResult;\n", resultType);
  }

  private FlinkExpressionFunction(
      Body body, LogicalType[] argumentTypes, ReadableConfig config, ClassLoader classLoader) {
    this.argumentTypes = argumentTypes;
    this.rowResultType = body.rowType();
    var context = body.context();
    functions = List.copyOf(context.functionInstances.values());
    String className = "FlinkExpressionEvaluator" + context.getNameCounter().getAndIncrement();
    String code =
        "public final class "
            + className
            + " implements "
            + Evaluator.class.getCanonicalName()
            + " {\n"
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
            + "public void close() throws Exception {\n"
            + context.reuseCloseCode()
            + "}\n"
            + context.reuseInnerClassDefinitionCode()
            + "}\n";
    Object[] references =
        scala.collection.JavaConverters.seqAsJavaList(context.references()).toArray();
    generated = new GeneratedFunction<>(className, code, references, config);
    generated.compile(classLoader);
  }

  @Override
  public RowType rowResultType() {
    return rowResultType;
  }

  @Override
  public List<ScalarFunction> functions() {
    return functions;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    evaluator = generated.newInstance(context.getUserCodeClassLoader());
    evaluator.open(context);
    input = new GenericRowData(argumentTypes.length);
  }

  public Object eval(Object... arguments) throws Exception {
    for (int i = 0; i < arguments.length; i++) {
      Object value = arguments[i];
      if (value instanceof String text) {
        value = StringData.fromString(text);
      } else if (value instanceof BigDecimal decimal) {
        DecimalType type = (DecimalType) argumentTypes[i];
        value =
            tech.streamfusion.arrow.DecimalAccessor.fromInternalValue(
                decimal, type.getPrecision(), type.getScale());
      }
      input.setField(i, value);
    }
    return evaluator.eval(input);
  }

  @Override
  public void close() throws Exception {
    if (evaluator != null) {
      evaluator.close();
      evaluator = null;
    }
  }

  private static final class Context extends CodeGeneratorContext {
    private final java.util.Map<String, String> functions = new java.util.HashMap<>();
    private final java.util.Map<String, ScalarFunction> functionInstances =
        new java.util.LinkedHashMap<>();

    Context(ReadableConfig config, ClassLoader classLoader) {
      super(config, classLoader);
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

    @Override
    public String addReusableConverter(
        org.apache.flink.table.types.DataType type, String classLoaderTerm) {
      return super.addReusableConverter(
          type, classLoaderTerm == null ? "context.getUserCodeClassLoader()" : classLoaderTerm);
    }
  }
}
