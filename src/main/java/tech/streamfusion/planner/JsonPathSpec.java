package tech.streamfusion.planner;

import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.flink.shaded.com.jayway.jsonpath.internal.Utils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.JsonStringEncoder;

/** Admission grammar shared with the native SQL/JSON path reader. */
final class JsonPathSpec {
  private static final Pattern MODE =
      Pattern.compile("^\\s*(strict|lax)\\s+(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final String ESCAPE = "\\\\(?:u[0-9a-fA-F]{4}|[^u])";
  private static final Pattern IDENTIFIER = Pattern.compile("[\\p{L}_][\\p{L}\\p{N}_]*");
  // Dot names end at a separator or function call; a leading * is a selector.
  // Jayway skips only spaces before an index, then String.trim() removes trailing ASCII controls.
  private static final Pattern STEP =
      Pattern.compile(
          "(?<wildcard>\\.\\*|\\[ *\\* *\\])"
              + "|\\.(?<dot>[^.\\[(* ][^.\\[( ]*)"
              + "|\\[ *'(?<single>(?:[^'\\\\\\x00-\\x1f]|"
              + ESCAPE
              + ")*)' *\\]"
              + "|\\[ *\"(?<quoted>(?:[^\"\\\\\\x00-\\x1f]|"
              + ESCAPE
              + ")*)\" *\\]"
              + "|\\[ *(?<index>-?[0-9]+(?: *, *-?[0-9]+)*)[\\x00-\\x20]*\\]");

  private JsonPathSpec() {}

  static String unicodeVersion() {
    // Jackson terminates root true/false/null tokens with Character.isJavaIdentifierPart(char).
    return switch (Runtime.version().feature()) {
      case 17 -> "13.0";
      case 21 -> "15.0";
      case 24, 25 -> "16.0";
      default -> null;
    };
  }

  static String normalize(String path) {
    if (unicodeVersion() == null || path.codePoints().anyMatch(c -> c >= 0xd800 && c <= 0xdfff)) {
      return null;
    }
    var mode = MODE.matcher(path);
    String prefix = "strict ";
    if (mode.matches()) {
      prefix = mode.group(1).toLowerCase(Locale.ROOT) + " ";
      path = mode.group(2);
    }
    if (!path.startsWith("$")) {
      return null;
    }
    // Jayway trims ASCII spaces; other trailing whitespace can be part of a dot member.
    int pathEnd = path.length();
    while (pathEnd > 1 && path.charAt(pathEnd - 1) == ' ') {
      pathEnd--;
    }
    path = path.substring(0, pathEnd);
    var step = STEP.matcher(path);
    StringBuilder normalized = new StringBuilder(prefix).append('$');
    int end = 1;
    while (end < path.length()) {
      step.region(end, path.length());
      if (!step.lookingAt()) {
        return null;
      }
      if (step.group("wildcard") != null) {
        normalized.append("[*]");
      } else if (step.group("index") != null) {
        String[] indexes = step.group("index").split(" *, *", -1);
        for (String index : indexes) {
          try {
            Integer.parseInt(index);
          } catch (NumberFormatException e) {
            return null;
          }
        }
        normalized.append('[').append(String.join(",", indexes)).append(']');
      } else if (step.group("dot") != null) {
        String name = step.group("dot");
        if (IDENTIFIER.matcher(name).matches()) {
          normalized.append('.').append(name);
        } else {
          // Jayway's dot token keeps punctuation, controls and backslashes literal. Quote the
          // name for the native wire grammar without running the bracket-name unescaper.
          normalized
              .append("[\"")
              .append(JsonStringEncoder.getInstance().quoteAsString(name))
              .append("\"]");
        }
      } else {
        boolean single = step.group("single") != null;
        String name = step.group(single ? "single" : "quoted");
        if (name.indexOf('\\') >= 0) {
          name = Utils.unescape(name);
          if (name.codePoints().anyMatch(c -> c >= 0xd800 && c <= 0xdfff)) return null;
          // The native wire grammar uses JSON escaping, independent of the SQL path's quote style.
          normalized
              .append("[\"")
              .append(JsonStringEncoder.getInstance().quoteAsString(name))
              .append("\"]");
        } else {
          char quote = single ? '\'' : '"';
          normalized.append('[').append(quote).append(name).append(quote).append(']');
        }
      }
      end = step.end();
    }
    return normalized.toString();
  }
}
