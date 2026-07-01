package io.github.jordepic.streamfusion.operator;

import org.apache.flink.table.data.binary.BinaryStringData;

/**
 * Host implementations of SQL builtins whose native (Rust) result can differ from Flink's, exposed as
 * plain {@code (String) -> String} static methods so the native engine can route them through a
 * columnar JVM upcall (see {@link NativeUdf}) and stay byte-identical to the host. Case folding is the
 * motivating case: Rust's {@code str::to_lowercase} follows Unicode default case mapping, while Flink's
 * {@code UPPER}/{@code LOWER} are {@code BinaryStringData.toUpperCase()}/{@code toLowerCase()} (which for
 * non-ASCII delegate to {@code String.toUpperCase()}/{@code toLowerCase()} under the JVM's locale), so
 * the two diverge on some inputs. Calling Flink's own method reproduces its result exactly.
 */
public final class NativeBuiltinFunctions {

  private NativeBuiltinFunctions() {}

  public static String lower(String s) {
    return s == null ? null : BinaryStringData.fromString(s).toLowerCase().toString();
  }

  public static String upper(String s) {
    return s == null ? null : BinaryStringData.fromString(s).toUpperCase().toString();
  }
}
