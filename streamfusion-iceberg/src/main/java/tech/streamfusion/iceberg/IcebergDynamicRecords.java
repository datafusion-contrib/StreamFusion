package tech.streamfusion.iceberg;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

/** Exact released control-record contract; payloads are never serialized as Java rows. */
final class IcebergDynamicRecords {
  private static final Class<?> TYPE;
  private static final Constructor<?> CONSTRUCTOR;
  private static final Method TABLE, BRANCH, SCHEMA, SPEC, KEY, UPSERT, EQUALITY;

  static {
    try {
      TYPE = Class.forName("org.apache.iceberg.flink.sink.dynamic.DynamicRecordInternal");
      CONSTRUCTOR =
          TYPE.getDeclaredConstructor(
              String.class,
              String.class,
              Schema.class,
              RowData.class,
              PartitionSpec.class,
              int.class,
              boolean.class,
              Set.class);
      CONSTRUCTOR.setAccessible(true);
      TABLE = method("tableName");
      BRANCH = method("branch");
      SCHEMA = method("schema");
      SPEC = method("spec");
      KEY = method("writerKey");
      UPSERT = method("upsertMode");
      EQUALITY = method("equalityFields");
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private static Method method(String name) throws NoSuchMethodException {
    var method = TYPE.getDeclaredMethod(name);
    method.setAccessible(true);
    return method;
  }

  @SuppressWarnings("unchecked")
  static IcebergDynamicBatch.Target target(
      Object record,
      java.util.Map<Schema, String> schemas,
      java.util.Map<PartitionSpec, String> specs) {
    try {
      return new IcebergDynamicBatch.Target(
          (String) TABLE.invoke(record),
          (String) BRANCH.invoke(record),
          schemas.computeIfAbsent((Schema) SCHEMA.invoke(record), SchemaParser::toJson),
          specs.computeIfAbsent((PartitionSpec) SPEC.invoke(record), PartitionSpecParser::toJson),
          (int) KEY.invoke(record),
          (boolean) UPSERT.invoke(record),
          (Set<Integer>) EQUALITY.invoke(record));
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot inspect released dynamic control record", failure);
    }
  }

  static Object record(
      IcebergDynamicBatch.Target target, Schema schema, PartitionSpec spec, RowData row) {
    try {
      return CONSTRUCTOR.newInstance(
          target.table(),
          target.branch(),
          schema,
          row,
          spec,
          target.writerKey(),
          target.upsert(),
          target.equalityFields());
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot create released dynamic control record", failure);
    }
  }
}
