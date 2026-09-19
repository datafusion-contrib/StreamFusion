package tech.streamfusion.suite;

import java.util.List;
import java.util.Map;

/** Preserves the shared Paimon fixture's catalog metadata through Flink 1.18's factory API. */
public final class PaimonCatalogFixture {
  private PaimonCatalogFixture() {}

  public static Object create(
      ClassLoader loader,
      Map<?, ?> options,
      List<?> columns,
      List<?> partitions,
      List<?> primaryKeys) {
    try {
      Class<?> constraintType =
          Class.forName("org.apache.flink.table.catalog.UniqueConstraint", true, loader);
      Object constraint =
          primaryKeys.isEmpty()
              ? null
              : constraintType
                  .getMethod("primaryKey", String.class, List.class)
                  .invoke(null, "pk", primaryKeys);
      Class<?> resolvedType =
          Class.forName("org.apache.flink.table.catalog.ResolvedSchema", true, loader);
      Object resolved =
          resolvedType
              .getConstructor(List.class, List.class, constraintType)
              .newInstance(columns, List.of(), constraint);
      Class<?> schemaType = Class.forName("org.apache.flink.table.api.Schema", true, loader);
      Object builder = schemaType.getMethod("newBuilder").invoke(null);
      builder.getClass().getMethod("fromResolvedSchema", resolvedType).invoke(builder, resolved);
      Object schema = builder.getClass().getMethod("build").invoke(builder);
      Class<?> catalogType =
          Class.forName("org.apache.flink.table.catalog.CatalogTable", true, loader);
      Object origin =
          catalogType
              .getMethod("of", schemaType, String.class, List.class, Map.class)
              .invoke(null, schema, "a comment", partitions, options);
      return Class.forName("org.apache.flink.table.catalog.ResolvedCatalogTable", true, loader)
          .getConstructor(catalogType, resolvedType)
          .newInstance(origin, resolved);
    } catch (ReflectiveOperationException error) {
      throw new IllegalStateException(
          "Cannot construct the Paimon catalog fixture on Flink 1.18", error);
    }
  }
}
