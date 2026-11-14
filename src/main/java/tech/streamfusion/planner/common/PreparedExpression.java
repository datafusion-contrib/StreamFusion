package tech.streamfusion.planner.common;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import tech.streamfusion.operator.NativeUdf;

/** Immutable expression pools and semantic metadata; input pruning creates a separate plan. */
public final class PreparedExpression implements EncodedExpression {
  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final int[] projectionRoots;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final String[] outputNames;
  private final int conditionRoot;
  private final int jvmExpressionCount;
  private final List<String> nativeFunctionCalls;
  private final List<String> jvmFunctionCalls;
  private final List<String> jvmExpressionDetails;
  private final String jvmExecutionReason;
  private final boolean wholeStructInputs;
  private final boolean collectionRepresentation;
  private final boolean originalStringInputs;
  private final boolean structuredInput;
  private final boolean[] binaryStringProjections;
  private final boolean[] variableBinaryProjections;
  private final NativeUdf.Binding binding;

  public static PreparedExpression copyOf(EncodedExpression expression) {
    return expression instanceof PreparedExpression
        ? (PreparedExpression) expression : new PreparedExpression(expression, null);
  }

  private PreparedExpression(EncodedExpression expression, int[] mapping) {
    kinds = expression.kinds();
    payload = expression.payload();
    if (mapping != null)
      for (int i = 0; i < kinds.length; i++)
        if (kinds[i] == ExpressionKind.INPUT_REF.code()) payload[i] = mapping[payload[i]];
    childCounts = expression.childCounts();
    projectionRoots = expression.projectionRoots();
    longs = expression.longs();
    doubles = expression.doubles();
    strings = expression.strings();
    outputNames = expression.outputNames().clone();
    conditionRoot = expression.conditionRoot();
    jvmExpressionCount = expression.jvmExpressionCount();
    nativeFunctionCalls = List.copyOf(expression.nativeFunctionCalls());
    jvmFunctionCalls = List.copyOf(expression.jvmFunctionCalls());
    jvmExpressionDetails = List.copyOf(expression.jvmExpressionDetails());
    jvmExecutionReason = expression.jvmExecutionReason();
    wholeStructInputs = expression.requiresWholeStructInputs();
    collectionRepresentation = expression.requiresCollectionRepresentation();
    originalStringInputs = expression.requiresOriginalStringInputs();
    structuredInput = expression.supportsStructuredInput();
    int outputCount = Math.max(projectionRoots.length, outputNames.length);
    binaryStringProjections = new boolean[outputCount];
    variableBinaryProjections = new boolean[outputCount];
    for (int i = 0; i < outputCount; i++) {
      binaryStringProjections[i] = expression.isBinaryStringProjection(i);
      variableBinaryProjections[i] = expression.isVariableBinaryProjection(i);
    }
    binding = expression.udfBinding().planningCopy(mapping);
  }

  @Override
  public int[] kinds() { return kinds.clone(); }

  @Override
  public int[] payload() { return payload.clone(); }

  @Override
  public int[] childCounts() { return childCounts.clone(); }

  @Override
  public int[] projectionRoots() { return projectionRoots.clone(); }

  @Override
  public long[] longs() { return longs.clone(); }

  @Override
  public double[] doubles() { return doubles.clone(); }

  @Override
  public String[] strings() { return strings.clone(); }

  @Override
  public String[] outputNames() { return outputNames.clone(); }

  @Override
  public int conditionRoot() { return conditionRoot; }

  @Override
  public int jvmExpressionCount() { return jvmExpressionCount; }

  @Override
  public List<String> nativeFunctionCalls() { return nativeFunctionCalls; }

  @Override
  public List<String> jvmFunctionCalls() { return jvmFunctionCalls; }

  @Override
  public List<String> jvmExpressionDetails() { return jvmExpressionDetails; }

  @Override
  public String jvmExecutionReason() { return jvmExecutionReason; }

  @Override
  public boolean requiresWholeStructInputs() { return wholeStructInputs; }

  @Override
  public boolean requiresCollectionRepresentation() { return collectionRepresentation; }

  @Override
  public boolean requiresOriginalStringInputs() { return originalStringInputs; }

  @Override
  public boolean supportsStructuredInput() { return structuredInput; }

  @Override
  public Set<Integer> referencedInputs() {
    Set<Integer> references = new LinkedHashSet<>();
    for (int i = 0; i < kinds.length; i++) if (kinds[i] == ExpressionKind.INPUT_REF.code()) references.add(payload[i]);
    return Collections.unmodifiableSet(references);
  }

  @Override
  public PreparedExpression remapInputs(int[] mapping) {
    return new PreparedExpression(this, mapping);
  }

  @Override
  public NativeUdf.Binding udfBinding() { return binding.planningCopy(null); }

  @Override
  public boolean isBinaryStringProjection(int index) { return index >= 0 && index < binaryStringProjections.length && binaryStringProjections[index]; }

  @Override
  public boolean isVariableBinaryProjection(int index) { return index >= 0 && index < variableBinaryProjections.length && variableBinaryProjections[index]; }

  @Override
  public boolean exceedsTreeDepth(int limit) {
    int[] pending = new int[limit];
    int depth = 0;
    for (int children : childCounts) {
      while (depth > 0 && pending[depth - 1] == 0) depth--;
      if (depth == limit) return true;
      if (depth > 0) pending[depth - 1]--;
      if (children > 0) pending[depth++] = children;
    }
    return false;
  }
}
