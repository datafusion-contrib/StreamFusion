package io.github.jordepic.streamfusion.operator;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.data.binary.BinaryStringData;

/**
 * Host implementations of SQL builtins whose native (Rust) result can differ from Flink's, exposed as
 * static methods so the native engine can route them through a columnar JVM upcall (see {@link
 * NativeUdf}) and stay byte-identical to the host. Case folding is the motivating case: Rust's
 * {@code str::to_lowercase} follows Unicode default case mapping, while Flink's {@code UPPER}/{@code
 * LOWER} are {@code BinaryStringData.toUpperCase()}/{@code toLowerCase()} (which for non-ASCII
 * delegate to {@code String.toUpperCase()}/{@code toLowerCase()} under the JVM's locale), so the two
 * diverge on some inputs. Calling Flink's own method reproduces its result exactly.
 *
 * <p>Methods that declare {@link BinaryStringData} parameters/results stay in UTF-8 bytes across the
 * upcall boundary: the marshaller wraps the Arrow column's bytes with {@code fromBytes} (no UTF-16
 * decode) and writes the result's bytes straight back, so Flink's ASCII fast paths never materialize
 * a {@code java.lang.String}. Methods that need {@code java.util.regex} keep {@code String}.
 */
public final class NativeBuiltinFunctions {

  private NativeBuiltinFunctions() {}

  // The ASCII fold is done here with a tight primitive loop rather than by calling
  // BinaryStringData.toLowerCase/toUpperCase: Flink's own "fast path" invokes
  // Character.toLowerCase per byte — a virtual call per character that profiled SLOWER than the
  // JDK's intrinsified String fold it was meant to avoid. For ASCII the mapping below is exactly
  // Character.toLowerCase/UpperCase (A-Z ± 32, everything else unchanged), and the first
  // non-ASCII byte delegates to Flink's own method — where its fast path also bails — so the
  // result is byte-identical to the host on every input. An unchanged string returns as-is.

  public static BinaryStringData lower(BinaryStringData s) {
    if (s == null) {
      return null;
    }
    byte[] bytes = s.toBytes();
    byte[] folded = null;
    for (int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      if (b < 0) {
        return (BinaryStringData) s.toLowerCase();
      }
      if (b >= 'A' && b <= 'Z') {
        if (folded == null) {
          folded = java.util.Arrays.copyOf(bytes, bytes.length);
        }
        folded[i] = (byte) (b + 32);
      }
    }
    return folded == null ? s : BinaryStringData.fromBytes(folded);
  }

  public static BinaryStringData upper(BinaryStringData s) {
    if (s == null) {
      return null;
    }
    byte[] bytes = s.toBytes();
    byte[] folded = null;
    for (int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      if (b < 0) {
        return (BinaryStringData) s.toUpperCase();
      }
      if (b >= 'a' && b <= 'z') {
        if (folded == null) {
          folded = java.util.Arrays.copyOf(bytes, bytes.length);
        }
        folded[i] = (byte) (b - 32);
      }
    }
    return folded == null ? s : BinaryStringData.fromBytes(folded);
  }

  /**
   * Compiled patterns, keyed by the regex string; empty caches a pattern that failed to compile.
   * {@code Pattern} is immutable and thread-safe, and the regexes reaching this map are plan
   * literals (a handful per query), so the map stays tiny and is never evicted.
   */
  private static final ConcurrentHashMap<String, Optional<Pattern>> PATTERNS =
      new ConcurrentHashMap<>();

  /**
   * Flink's {@code SqlFunctionUtils.regexpExtract}, byte-identical (same {@code java.util.regex}
   * engine, same null-in/null-out and swallow-to-null semantics), but with the {@link Pattern}
   * compiled once per regex instead of once per call — the host method recompiles every
   * invocation, which a CPU profile put at ~13% of Nexmark q21. A pattern that fails to compile
   * yields null on every call, exactly as the per-call compile-and-catch does.
   */
  public static String regexpExtract(String str, String regex, int extractIndex) {
    if (str == null || regex == null) {
      return null;
    }
    Optional<Pattern> pattern =
        PATTERNS.computeIfAbsent(
            regex,
            r -> {
              try {
                return Optional.of(Pattern.compile(r));
              } catch (Exception e) {
                return Optional.empty();
              }
            });
    if (pattern.isEmpty()) {
      return null;
    }
    try {
      Matcher m = pattern.get().matcher(str);
      if (m.find()) {
        MatchResult mr = m.toMatchResult();
        return mr.group(extractIndex);
      }
    } catch (Exception e) {
      // Same contract as the host: any evaluation failure (e.g. a group index out of range)
      // yields null rather than failing the row.
    }
    return null;
  }

  public static String regexpExtract(String str, String regex, long extractIndex) {
    return regexpExtract(str, regex, (int) extractIndex);
  }

  public static String regexpExtract(String str, String regex) {
    return regexpExtract(str, regex, 0);
  }
}
