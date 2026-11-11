package tech.streamfusion.planner.common;

import java.util.List;
import tech.streamfusion.operator.NativeUdf;

/** Encoded scalar plan and representation requirements, independent of either planning mode. */
public interface EncodedExpression {
  int[] kinds();

  int[] payload();

  int[] childCounts();

  int[] projectionRoots();

  long[] longs();

  double[] doubles();

  String[] strings();

  String[] outputNames();

  int conditionRoot();

  int jvmExpressionCount();

  List<String> nativeFunctionCalls();

  List<String> jvmFunctionCalls();

  List<String> jvmExpressionDetails();

  String jvmExecutionReason();

  boolean requiresWholeStructInputs();

  boolean requiresCollectionRepresentation();

  boolean requiresOriginalStringInputs();

  NativeUdf.Binding udfBinding();

  boolean isBinaryStringProjection(int index);

  boolean isVariableBinaryProjection(int index);

  boolean exceedsTreeDepth(int limit);
}
