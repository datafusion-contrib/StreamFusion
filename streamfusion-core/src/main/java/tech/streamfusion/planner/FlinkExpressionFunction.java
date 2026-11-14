package tech.streamfusion.planner;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Executes generated expressions without depending on Flink's planner. The class name and
 * serialized fields stay stable so existing serialized operator bindings keep their runtime
 * identity.
 */
public final class FlinkExpressionFunction extends ScalarFunction
    implements tech.streamfusion.operator.NativeUdf.FunctionDependencies,
        tech.streamfusion.operator.NativeUdf.RowResult,
        tech.streamfusion.operator.NativeUdf.InternalArguments,
        tech.streamfusion.operator.NativeUdf.ProcessingTimeAware {
  private static final long serialVersionUID = 1L;

  public interface Evaluator extends Function {
    Object eval(RowData input) throws Exception;

    /** False leaves evaluation to the caller, including for older serialized evaluators. */
    default boolean tryEvalBatch(
        tech.streamfusion.arrow.ArrowReader reader,
        tech.streamfusion.arrow.ArrowWriter<RowData> writer,
        int rows)
        throws Exception {
      return false;
    }

    default void bindProcessingTime(ProcessingTimeService service) {}

    void open(FunctionContext context) throws Exception;

    void close() throws Exception;
  }

  /** The access shape emitted by Flink's PROCTIME_MATERIALIZE code generator. */
  public static final class ProcessingTimeContext {
    private final ProcessingTimeService service;

    public ProcessingTimeContext(ProcessingTimeService service) {
      this.service = service;
    }

    public ProcessingTimeContext timerService() {
      return this;
    }

    public long currentProcessingTime() {
      if (service == null) {
        throw new IllegalStateException(
            "generated processing time requires the operator's runtime clock");
      }
      return service.getCurrentProcessingTime();
    }
  }

  private final GeneratedFunction<Evaluator> generated;
  private final LogicalType[] argumentTypes;
  private final List<ScalarFunction> functions;
  private final RowType rowResultType;
  private final byte[] arrowResultSchema;
  private transient ProcessingTimeService processingTimeService;
  private transient Evaluator evaluator;
  private transient GenericRowData input;
  private boolean rowFilter;

  FlinkExpressionFunction(
      GeneratedFunction<Evaluator> generated,
      LogicalType[] argumentTypes,
      List<ScalarFunction> functions,
      RowType rowResultType,
      byte[] arrowResultSchema,
      boolean rowFilter) {
    this.generated = generated;
    this.argumentTypes = argumentTypes;
    this.functions = functions;
    this.rowResultType = rowResultType;
    this.arrowResultSchema = arrowResultSchema;
    this.rowFilter = rowFilter;
  }

  public boolean hasRowFilter() {
    return rowFilter;
  }

  @Override
  public LogicalType[] argumentTypes() {
    return argumentTypes.clone();
  }

  @Override
  public RowType rowResultType() {
    return rowResultType;
  }

  @Override
  public org.apache.arrow.vector.types.pojo.Schema arrowResultSchema() {
    return org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(
        java.nio.ByteBuffer.wrap(arrowResultSchema));
  }

  @Override
  public List<ScalarFunction> functions() {
    return functions;
  }

  @Override
  public void processingTimeService(ProcessingTimeService service) {
    if (evaluator != null) {
      throw new IllegalStateException("cannot replace a live generated expression's runtime clock");
    }
    processingTimeService = service;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    try {
      evaluator = generated.newInstance(context.getUserCodeClassLoader());
      evaluator.bindProcessingTime(processingTimeService);
      evaluator.open(context);
      input = new GenericRowData(argumentTypes.length);
    } catch (Exception | Error failure) {
      evaluator = null;
      input = null;
      processingTimeService = null;
      throw failure;
    }
  }

  public Object eval(Object... arguments) throws Exception {
    for (int i = 0; i < arguments.length; i++) {
      setArgument(i, arguments[i]);
    }
    return evaluator.eval(input);
  }

  /** Preserve materialized argument semantics without reflective varargs dispatch. */
  public Object evalColumns(Object[][] columns, int row) throws Exception {
    for (int i = 0; i < columns.length; i++) setArgument(i, columns[i][row]);
    return evaluator.eval(input);
  }

  /** The imported Arrow batch owns this row for the duration of synchronous evaluation. */
  public Object evalRow(RowData row) throws Exception {
    return evaluator.eval(row);
  }

  /** Try the generated batch loop without evaluating any rows when it is unavailable. */
  public boolean tryEvalBatch(
      tech.streamfusion.arrow.ArrowReader reader,
      tech.streamfusion.arrow.ArrowWriter<RowData> writer,
      int rows)
      throws Exception {
    return evaluator.tryEvalBatch(reader, writer, rows);
  }

  private void setArgument(int position, Object value) {
    if (value instanceof String) {
      String text = ((String) value);

      value = StringData.fromString(text);
    } else if (value instanceof BigDecimal) {
      BigDecimal decimal = ((BigDecimal) value);

      DecimalType type = (DecimalType) argumentTypes[position];
      value =
          tech.streamfusion.arrow.DecimalAccessor.fromInternalValue(
              decimal, type.getPrecision(), type.getScale());
    }
    input.setField(position, value);
  }

  @Override
  public void close() throws Exception {
    try {
      if (evaluator != null) {
        evaluator.close();
      }
    } finally {
      evaluator = null;
      input = null;
      processingTimeService = null;
    }
  }
}
