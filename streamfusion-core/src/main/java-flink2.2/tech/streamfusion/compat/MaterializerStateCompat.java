package tech.streamfusion.compat;

import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.sequencedmultisetstate.linked.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Released V2 serializers stay behind the version boundary. */
public final class MaterializerStateCompat {
  private MaterializerStateCompat() {}

  public static boolean supportsV2() {
    return true;
  }

  public static GeneratedRecordEqualiser equaliser(RowType type) {
    return new EqualiserCodeGenerator(type, MaterializerStateCompat.class.getClassLoader())
        .generateRecordEqualiser("CanonicalMaterializerKeyEq");
  }

  public static GeneratedHashFunction hash(RowType type) {
    return HashCodeGenerator.generateRowHash(
        new CodeGeneratorContext(
            new Configuration(), MaterializerStateCompat.class.getClassLoader()),
        type,
        "CanonicalMaterializerKeyHash",
        java.util.stream.IntStream.range(0, type.getFieldCount()).toArray());
  }

  public static CompositeSerializer<?> nodes(TypeSerializer<RowData> rows) {
    return new NodeSerializer(rows);
  }

  public static CompositeSerializer<?> indexes() {
    return new RowSqnInfoSerializer();
  }

  public static CompositeSerializer<?> highest() {
    return new MetaSqnInfoSerializer();
  }

  public static TypeSerializer<?> key(
      RowDataSerializer rows, GeneratedRecordEqualiser eq, GeneratedHashFunction hash) {
    var loader = MaterializerStateCompat.class.getClassLoader();
    return new RowDataKeySerializer(
        rows, eq.newInstance(loader), hash.newInstance(loader), eq, hash);
  }
}
