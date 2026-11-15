package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import tech.streamfusion.planner.common.ExpressionKind;

/** Owns the compact pre-order wire pools; semantic rules never mutate their storage directly. */
final class ExpressionBuffer {
  private final List<Integer> kinds = new ArrayList<>();
  private final List<Integer> payload = new ArrayList<>();
  private final List<Integer> children = new ArrayList<>();
  private final List<Long> longs = new ArrayList<>();
  private final List<Double> doubles = new ArrayList<>();
  private final List<String> strings = new ArrayList<>();

  void node(int kind, int value, int childCount) {
    kinds.add(kind);
    payload.add(value);
    children.add(childCount);
  }

  int nodeCount() {
    return kinds.size();
  }

  int kind(int index) {
    return kinds.get(index);
  }

  int payloadAt(int index) {
    return payload.get(index);
  }

  void rewritePayload(int index, int value) {
    payload.set(index, value);
  }

  boolean containsKind(int kind) {
    return kinds.contains(kind);
  }

  int longCount() {
    return longs.size();
  }

  void appendLong(long value) {
    longs.add(value);
  }

  int doubleCount() {
    return doubles.size();
  }

  void appendDouble(double value) {
    doubles.add(value);
  }

  int stringCount() {
    return strings.size();
  }

  void appendString(String value) {
    strings.add(value);
  }

  int[] kinds() {
    return ints(kinds);
  }

  int[] payload() {
    return ints(payload);
  }

  int[] childCounts() {
    return ints(children);
  }

  long[] longs() {
    long[] out = new long[longs.size()];
    for (int i = 0; i < out.length; i++) out[i] = longs.get(i);
    return out;
  }

  double[] doubles() {
    double[] out = new double[doubles.size()];
    for (int i = 0; i < out.length; i++) out[i] = doubles.get(i);
    return out;
  }

  String[] strings() {
    return strings.toArray(new String[0]);
  }

  Set<Integer> referencedInputs() {
    Set<Integer> inputs = new LinkedHashSet<>();
    for (int i = 0; i < kinds.size(); i++) {
      if (kinds.get(i) == ExpressionKind.INPUT_REF.code()) inputs.add(payload.get(i));
    }
    return inputs;
  }

  void remapInputs(int[] map) {
    for (int i = 0; i < kinds.size(); i++) {
      if (kinds.get(i) == ExpressionKind.INPUT_REF.code()) payload.set(i, map[payload.get(i)]);
    }
  }

  boolean exceedsTreeDepth(int limit) {
    int[] pending = new int[limit];
    int depth = 0;
    for (int childCount : children) {
      while (depth > 0 && pending[depth - 1] == 0) depth--;
      if (depth == limit) return true;
      if (depth > 0) pending[depth - 1]--;
      if (childCount > 0) pending[depth++] = childCount;
    }
    return false;
  }

  private static int[] ints(List<Integer> values) {
    int[] out = new int[values.size()];
    for (int i = 0; i < out.length; i++) out[i] = values.get(i);
    return out;
  }
}
