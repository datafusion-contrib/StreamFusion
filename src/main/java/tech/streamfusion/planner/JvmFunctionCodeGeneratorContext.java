package tech.streamfusion.planner;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** Reuses released converters while retaining typed arrays after nullable Arrow copies. */
class JvmFunctionCodeGeneratorContext extends CodeGeneratorContext {
  private final Map<DataType, String> arrayConverters = new HashMap<>();
  private final String defaultClassLoaderTerm;
  private final boolean recursiveContainers;

  JvmFunctionCodeGeneratorContext(
      ReadableConfig config, ClassLoader classLoader, String defaultClassLoaderTerm) {
    this(config, classLoader, defaultClassLoaderTerm, false);
  }

  JvmFunctionCodeGeneratorContext(
      ReadableConfig config,
      ClassLoader classLoader,
      String defaultClassLoaderTerm,
      boolean recursiveContainers) {
    super(config, classLoader);
    this.defaultClassLoaderTerm = defaultClassLoaderTerm;
    this.recursiveContainers = recursiveContainers;
  }

  @Override
  public String addReusableConverter(
      org.apache.flink.table.types.DataType type, String classLoaderTerm) {
    if (recursiveContainers && ExternalContainerConverter.containsArray(type)) {
      return arrayConverters.computeIfAbsent(
          type,
          ignored -> {
            String term =
                addReusableObject(
                    new ExternalContainerConverter(type),
                    "containerConverter",
                    ExternalContainerConverter.class.getCanonicalName());
            addReusableOpenStatement(
                term
                    + ".open("
                    + (classLoaderTerm == null ? defaultClassLoaderTerm : classLoaderTerm)
                    + ");");
            return term;
          });
    }
    if (type.getLogicalType().is(LogicalTypeRoot.ARRAY) && type.getConversionClass().isArray()) {
      return arrayConverters.computeIfAbsent(
          type,
          ignored -> {
            String term =
                addReusableObject(
                    new ScalarArrayConverter(type),
                    "arrayConverter",
                    ScalarArrayConverter.class.getCanonicalName());
            addReusableOpenStatement(
                term
                    + ".open("
                    + (classLoaderTerm == null ? defaultClassLoaderTerm : classLoaderTerm)
                    + ");");
            return term;
          });
    }
    return super.addReusableConverter(
        type, classLoaderTerm == null ? defaultClassLoaderTerm : classLoaderTerm);
  }
}
