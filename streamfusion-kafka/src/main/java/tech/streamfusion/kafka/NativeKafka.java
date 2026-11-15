package tech.streamfusion.kafka;

import tech.streamfusion.NativeExtensionLoader;

/** JNI entry point for native Kafka record serialization. Kafka I/O remains in Flink. */
public final class NativeKafka {

  static {
    NativeExtensionLoader.load(
        NativeKafka.class,
        "kafka",
        NativeKafka::nativeBuildVersion,
        NativeKafka::liveNativeHandles);
  }

  private NativeKafka() {}

  /** The loaded extension library's StreamFusion build stamp (the loader's version check). */
  private static native String nativeBuildVersion();

  /** Live handles owned by this library, used by the shared leak sentinel. */
  public static native String liveNativeHandles();

  /** Whether the native Kafka serialization extension loaded successfully. */
  public static native boolean isLoaded();

  /** Compatibility capability query; production planning uses the owning format's admission. */
  public static boolean encodeFormatSupported(int format) {
    return tech.streamfusion.format.NativeFormatProviders.forIdentifier(
            tech.streamfusion.format.FormatCodes.identifier(format))
        .map(provider -> provider.encoderFactory() != null)
        .orElse(false);
  }

  public static native byte[] spellFloatingPoint(double[] doubles, float[] floats);

  private static tech.streamfusion.format.EncodeFormat plan(int code, String options) {
    var provider =
        tech.streamfusion.format.NativeFormatProviders.forIdentifier(
                tech.streamfusion.format.FormatCodes.identifier(code))
            .orElseThrow(
                () -> new IllegalArgumentException("format artifact is not installed: " + code));
    var factory = provider.encoderFactory();
    if (factory == null)
      throw new IllegalArgumentException("format has no native encoder: " + code);
    return tech.streamfusion.format.EncodeFormat.resolved(
        code, options, null, tech.streamfusion.format.FormatCodes.isCdc(code), factory);
  }

  private static org.apache.arrow.vector.VectorSchemaRoot takeInput(long array, long schema) {
    return org.apache.arrow.c.Data.importVectorSchemaRoot(
        tech.streamfusion.operator.NativeAllocator.SHARED,
        org.apache.arrow.c.ArrowArray.wrap(array),
        org.apache.arrow.c.ArrowSchema.wrap(schema),
        tech.streamfusion.operator.NativeAllocator.DICTIONARIES);
  }

  public static byte[][] encodeKafkaBatch(
      long array, long schema, int format, String options, String[] types, String[] names)
      throws Exception {
    try (var input = takeInput(array, schema)) {
      int arity = input.getFieldVectors().size();
      if (input.getVector(tech.streamfusion.operator.RowDataArrowConverter.ROW_KIND_COLUMN)
          instanceof org.apache.arrow.vector.TinyIntVector) arity--;
      int[] fields = java.util.stream.IntStream.range(0, arity).toArray();
      try (var encoder = plan(format, options).createEncoder(types, names, fields)) {
        var encoded = encoder.encode(input);
        byte[][] output = new byte[encoded.size()][];
        for (int i = 0; i < output.length; i++) output[i] = encoded.message(i);
        return output;
      }
    }
  }

  public static NativeKafkaEncodedBatch encodeKafkaRecordBatch(
      long array,
      long schema,
      int format,
      String options,
      int keyFormat,
      String keyOptions,
      String[] types,
      String[] names,
      int[] keyFields,
      int[] valueFields,
      boolean upsert)
      throws Exception {
    try (var input = takeInput(array, schema);
        var encoder =
            new KafkaMessageEncoder(
                plan(format, options),
                plan(keyFormat, keyOptions),
                types,
                names,
                keyFields,
                valueFields,
                upsert)) {
      return encoder.encode(input);
    }
  }

  public static byte[][][] encodeKafkaRecords(
      long array,
      long schema,
      int format,
      String options,
      int keyFormat,
      String keyOptions,
      String[] types,
      String[] names,
      int[] keyFields,
      int[] valueFields,
      boolean upsert)
      throws Exception {
    var records =
        encodeKafkaRecordBatch(
            array,
            schema,
            format,
            options,
            keyFormat,
            keyOptions,
            types,
            names,
            keyFields,
            valueFields,
            upsert);
    byte[][] keys = new byte[records.size()][];
    byte[][] values = new byte[records.size()][];
    for (int i = 0; i < records.size(); i++) {
      var row = records.record(i);
      keys[i] = row.key();
      values[i] = row.value();
    }
    return new byte[][][] {keys, values};
  }
}
