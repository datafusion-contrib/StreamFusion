package tech.streamfusion.paimon;

import org.apache.paimon.types.RowType;

/** Admission contract for Paimon formats whose data writers accept Arrow bundles. */
public interface NativePaimonFileFormat {
  String nativeWriterFallbackReason(RowType type);

  String nativeWriterFallbackReason(RowType type, String compression);
}
