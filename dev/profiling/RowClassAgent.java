import java.lang.instrument.Instrumentation;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import static net.bytebuddy.matcher.ElementMatchers.*;

public final class RowClassAgent {
  public static void premain(String args, Instrumentation instrumentation) {
    new AgentBuilder.Default()
        .with(AgentBuilder.Listener.StreamWriting.toSystemError().withTransformationsOnly())
        .type(named("tech.streamfusion.operator.RowDataToArrowOperator"))
        .transform((builder, type, loader, module, domain) -> builder.visit(
            Advice.to(Observe.class).on(named("processElement").and(takesArguments(1)))))
        .type(named("tech.streamfusion.arrow.writers.BinaryWriter"))
        .transform((builder, type, loader, module, domain) -> builder.visit(
            Advice.to(Observe.class).on(named("doWrite").and(takesArguments(2)))))
        .installOn(instrumentation);
  }
  public static class Observe {
    @Advice.OnMethodEnter
    public static void enter(@Advice.This Object target, @Advice.Argument(0) Object input, @Advice.Origin("#t") String boundary)
        throws Exception {
      String phase = boundary.endsWith("RowDataToArrowOperator") ? "entry" : "writer";
      java.util.Properties properties = System.getProperties();
      synchronized (properties) {
        String active = "sf.row-class-probe.active." + Thread.currentThread().getId();
        if (phase.equals("entry")) {
          properties.setProperty(active, Integer.toString(System.identityHashCode(target)));
        }
        String instance = properties.getProperty(active);
        if (instance == null) throw new IllegalStateException("Writer observed without entry context");
        String prefix = "sf.row-class-probe." + phase + "." + instance + ".";
        int seen = Integer.parseInt(properties.getProperty(prefix + "sampled", "0"));
        if (seen < 2000) {
          Object row = phase.equals("entry") ? input.getClass().getMethod("getValue").invoke(input) : input;
          String name = row.getClass().getName();
          String key = prefix + "class." + name;
          properties.setProperty(key, Integer.toString(Integer.parseInt(properties.getProperty(key, "0")) + 1));
          if (name.equals("tech.streamfusion.operator.PrunedRowData")) {
            java.lang.reflect.Field field = row.getClass().getDeclaredField("row");
            field.setAccessible(true);
            Object original = field.get(row);
            String wrapped = prefix + "wrapped." + original.getClass().getName();
            properties.setProperty(wrapped, Integer.toString(Integer.parseInt(properties.getProperty(wrapped, "0")) + 1));
          }
          properties.setProperty(prefix + "sampled", Integer.toString(seen + 1));
          if (seen == 1999) {
            for (String observed : properties.stringPropertyNames()) {
              if (observed.startsWith(prefix)) {
                System.err.println("SF_ROW_CLASS," + observed + "," + properties.getProperty(observed));
              }
            }
          }
        }
      }
    }
  }
}
