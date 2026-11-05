package tech.streamfusion.suite;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

/** Test-only selection and work evidence for the released Paimon compaction fixtures. */
public final class PaimonCompactionFixtures {
  private PaimonCompactionFixtures() {}

  public static String mode() {
    return System.getProperty("streamfusion.paimon.compaction.mode", "");
  }

  private static boolean adapt() {
    String source = SqlInventory.activeTestSource();
    return mode().equals("native")
        && !source.isEmpty()
        && !source.contains("CompactProcedureITCase");
  }

  public static void install(Object store, Object write) throws Exception {
    if (!adapt() || !SqlInventory.activeTestSource().contains("org.apache.paimon.")) return;
    Class.forName(
            "tech.streamfusion.paimon.PaimonCompactionTestAdapter",
            true,
            store.getClass().getClassLoader())
        .getMethod("install", Object.class, Object.class)
        .invoke(null, store, write);
  }

  public static void bucket(Object write, Object partition, int bucket, Object container)
      throws Exception {
    if (!adapt() || !SqlInventory.activeTestSource().contains("org.apache.paimon.")) return;
    Class.forName(
            "tech.streamfusion.paimon.PaimonCompactionTestAdapter",
            true,
            write.getClass().getClassLoader())
        .getMethod("bucket", Object.class, Object.class, int.class, Object.class)
        .invoke(null, write, partition, bucket, container);
  }

  public static Object appendWrite(Object table, Object write) throws Exception {
    if (!adapt() || !SqlInventory.activeTestSource().contains("org.apache.paimon.")) return write;
    return Class.forName(
            "tech.streamfusion.paimon.PaimonCompactionTestAdapter",
            true,
            write.getClass().getClassLoader())
        .getMethod("appendWrite", Object.class, Object.class)
        .invoke(null, table, write);
  }

  public static void closeAppend(Object adapted, Throwable failure) throws Exception {
    if (adapted == null) return;
    try {
      var close = adapted.getClass().getMethod("close");
      close.setAccessible(true);
      close.invoke(adapted);
    } catch (Exception closing) {
      if (failure != null) failure.addSuppressed(closing);
      else throw closing;
    }
  }

  public static void adaptEnvironment(Object environment) throws Exception {
    if (!adapt()) return;
    ClassLoader loader = environment.getClass().getClassLoader();
    Class.forName("tech.streamfusion.paimon.NativePaimonMaintenance", true, loader)
        .getMethod(
            "accelerate",
            Class.forName(
                "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment",
                true,
                loader))
        .invoke(null, environment);
  }

  private record Rewrite(Object owner, long before) {}

  public static Object started(Object rewriter) throws Exception {
    return mode().equals("native")
        ? new Rewrite(SqlInventory.compactionScope(), count(rewriter, "nativeGroups"))
        : null;
  }

  public static void completed(
      Object rewriter,
      Object token,
      int level,
      boolean dropDelete,
      Object result,
      Throwable failure)
      throws Exception {
    Object before = result == null ? null : result.getClass().getMethod("before").invoke(result);
    Object after = result == null ? null : result.getClass().getMethod("after").invoke(result);
    record(rewriter, token, level, dropDelete, before, after, failure);
  }

  public static void completedAppend(
      Object rewriter, Object token, Object before, Object after, Throwable failure)
      throws Exception {
    record(rewriter, token, 0, false, before, after, failure);
  }

  private static void record(
      Object rewriter,
      Object token,
      int level,
      boolean dropDelete,
      Object before,
      Object after,
      Throwable failure)
      throws Exception {
    var scope = (Rewrite) token;
    Map<String, Object> work = new LinkedHashMap<>();
    Object fallback = field(rewriter, "fallbackReason");
    work.put(
        "native",
        failure == null && fallback == null && count(rewriter, "nativeGroups") > scope.before());
    work.put("output_level", level);
    work.put("drop_delete", dropDelete);
    work.put("fallback", fallback);
    work.put("failure", failure == null ? null : failure.toString());
    files(work, "before", before);
    files(work, "after", after);
    SqlInventory.compaction(scope.owner(), work);
  }

  private static void files(Map<String, Object> work, String side, Object value) throws Exception {
    if (value == null) return;
    var files = (java.util.List<?>) value;
    long rows = 0, bytes = 0;
    var formats = new java.util.TreeSet<String>();
    for (Object file : files) {
      formats.add(file.getClass().getMethod("fileFormat").invoke(file).toString());
      rows += ((Number) file.getClass().getMethod("rowCount").invoke(file)).longValue();
      bytes += ((Number) file.getClass().getMethod("fileSize").invoke(file)).longValue();
    }
    work.put(side + "_files", files.size());
    work.put(side + "_formats", formats);
    work.put(side + "_rows", rows);
    work.put(side + "_bytes", bytes);
  }

  private static long count(Object rewriter, String name) throws Exception {
    Object counter;
    try {
      counter = field(rewriter, name);
    } catch (NoSuchFieldException sharedCounters) {
      counter = field(field(rewriter, "counters"), name);
    }
    var method = counter.getClass().getMethod("getCount");
    method.setAccessible(true);
    return ((Number) method.invoke(counter)).longValue();
  }

  private static Object field(Object object, String name) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }
}
