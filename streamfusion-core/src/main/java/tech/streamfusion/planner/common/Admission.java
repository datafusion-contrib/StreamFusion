package tech.streamfusion.planner.common;

import java.util.Objects;
import java.util.function.Function;

/** One planning attempt, retaining its prepared value or its exact decline reason. */
public final class Admission<T> {
  public enum Status {
    NO_MATCH,
    DECLINED,
    ACCEPTED
  }

  private final Status status;
  private final T value;
  private final String reason;

  private Admission(Status status, T value, String reason) {
    this.status = status;
    this.value = value;
    this.reason = reason;
  }

  public static <T> Admission<T> noMatch() {
    return new Admission<>(Status.NO_MATCH, null, null);
  }

  public static <T> Admission<T> declined(String reason) {
    return new Admission<>(Status.DECLINED, null, Objects.requireNonNull(reason));
  }

  public static <T> Admission<T> accepted(T value) {
    return new Admission<>(Status.ACCEPTED, Objects.requireNonNull(value), null);
  }

  public Status status() {
    return status;
  }

  public boolean isAccepted() {
    return status == Status.ACCEPTED;
  }

  public T value() {
    if (!isAccepted()) throw new IllegalStateException("Planning attempt was " + status);
    return value;
  }

  public String reason() {
    return reason;
  }

  public <U> Admission<U> map(Function<? super T, ? extends U> mapping) {
    if (isAccepted()) return accepted(mapping.apply(value));
    return status == Status.NO_MATCH ? noMatch() : declined(reason);
  }
}
