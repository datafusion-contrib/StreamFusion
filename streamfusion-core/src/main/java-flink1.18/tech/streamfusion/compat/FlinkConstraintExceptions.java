package tech.streamfusion.compat;

import org.apache.flink.table.api.TableException;

/** Constraint failure types selected at build time for the target Flink line. */
public final class FlinkConstraintExceptions {
  private FlinkConstraintExceptions() {}

  public static RuntimeException notNull(String format, String fieldName) {
    return new TableException(String.format(format, fieldName));
  }
}
