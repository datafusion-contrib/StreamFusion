package tech.streamfusion.doris;

import org.apache.flink.table.types.logical.RowType;

/** A declared Flink type alone does not establish the layout returned by Doris. */
public final class DorisSchemas {
  private DorisSchemas() {}

  /** Checks the actual Arrow layout before installing a buffer-sharing source. */
  public static String incompatibility(
      org.apache.doris.flink.cfg.DorisOptions options,
      org.apache.doris.flink.cfg.DorisReadOptions reads,
      RowType type) {
    var probe = reads.copy();
    var expected = tech.streamfusion.arrow.ArrowConversion.toArrowSchema(type);
    try (var reader = DorisBatchReader.probe(options, probe, expected)) {
      if (reader == null) return "No Arrow schema is available for this scan";
      var actual = reader.schema();
      if (actual == null) return "No Arrow schema is available for this scan";
      if (!expected.getFields().isEmpty()
          && actual.getFields().size() != expected.getFields().size())
        return "Arrow field count differs from the declared schema";
      if (!expected.getFields().isEmpty()) {
        String projection =
            DorisArrowProjection.normalize(
                reads.getReadFields(), actual, expected, java.time.ZoneId.systemDefault());
        if (!projection.equals(reads.getReadFields())) {
          reads.setReadFields(projection);
          probe.setReadFields(projection);
          try (var adjusted = DorisBatchReader.probe(options, probe, expected)) {
            actual = adjusted.schema();
          }
        }
      }
      for (int i = 0; i < expected.getFields().size(); i++)
        DorisArrowBuffers.layout(actual.getFields().get(i), expected.getFields().get(i));
      return null;
    } catch (Exception incompatible) {
      return "Arrow schema check failed: " + incompatible;
    }
  }
}
