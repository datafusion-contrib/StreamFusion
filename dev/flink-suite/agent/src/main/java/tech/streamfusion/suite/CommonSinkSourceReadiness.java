package tech.streamfusion.suite;

import static net.bytebuddy.matcher.ElementMatchers.named;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.implementation.bytecode.assign.Assigner;

/** Lets the bounded sink fixture receive its final coordinator event before emitting data. */
public final class CommonSinkSourceReadiness {
  private static final Map<Object, CompletableFuture<Void>> PENDING = new WeakHashMap<>();
  private static final CompletableFuture<Void> READY = CompletableFuture.completedFuture(null);

  private CommonSinkSourceReadiness() {}

  static DynamicType.Builder<?> instrument(DynamicType.Builder<?> builder) {
    return builder
        .method(named("isAvailable"))
        .intercept(MethodDelegation.to(Availability.class))
        .method(named("close"))
        .intercept(Advice.to(Release.class).wrap(SuperMethodCall.INSTANCE))
        .visit(Advice.to(Poll.class).on(named("pollNext")))
        .visit(Advice.to(Release.class).on(named("notifyNoMoreSplits")));
  }

  public static synchronized CompletableFuture<Void> available(
      Object reader, boolean noMoreSplits) {
    return noMoreSplits
        ? READY
        : PENDING.computeIfAbsent(reader, ignored -> new CompletableFuture<>());
  }

  public static void release(Object reader) {
    CompletableFuture<Void> pending;
    synchronized (CommonSinkSourceReadiness.class) {
      pending = PENDING.remove(reader);
    }
    if (pending != null) pending.complete(null);
  }

  public static final class Availability {
    public static CompletableFuture<Void> available(
        @This Object reader, @FieldValue("noMoreSplits") boolean noMoreSplits) {
      return CommonSinkSourceReadiness.available(reader, noMoreSplits);
    }
  }

  public static final class Poll {
    @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
    static boolean enter(@Advice.FieldValue("noMoreSplits") boolean noMoreSplits) {
      return !noMoreSplits;
    }

    @Advice.OnMethodExit
    static void exit(
        @Advice.Enter boolean waiting,
        @Advice.Origin Method method,
        @Advice.Return(readOnly = false, typing = Assigner.Typing.DYNAMIC) Object result)
        throws ReflectiveOperationException {
      if (waiting) result = method.getReturnType().getField("NOTHING_AVAILABLE").get(null);
    }
  }

  public static final class Release {
    @Advice.OnMethodExit(onThrowable = Throwable.class)
    static void exit(@Advice.This Object reader) {
      CommonSinkSourceReadiness.release(reader);
    }
  }
}
