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
    if (!adapt()) return;
    Class.forName(
            "tech.streamfusion.paimon.PaimonCompactionTestAdapter",
            true,
            store.getClass().getClassLoader())
        .getMethod("install", Object.class, Object.class)
        .invoke(null, store, write);
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
    if (result != null) {
      for (String side : new String[] {"before", "after"}) {
        var files = (java.util.List<?>) result.getClass().getMethod(side).invoke(result);
        long rows = 0, bytes = 0;
        for (Object file : files) {
          rows += ((Number) file.getClass().getMethod("rowCount").invoke(file)).longValue();
          bytes += ((Number) file.getClass().getMethod("fileSize").invoke(file)).longValue();
        }
        work.put(side + "_files", files.size());
        work.put(side + "_rows", rows);
        work.put(side + "_bytes", bytes);
      }
    }
    SqlInventory.compaction(scope.owner(), work);
  }

  private static long count(Object rewriter, String name) throws Exception {
    Object counter = field(rewriter, name);
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
