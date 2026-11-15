package tech.streamfusion.orc;

import tech.streamfusion.format.ColumnarFileCodec;

/** Adapts the optional Orc module to the shared columnar file lifecycle. */
public final class OrcCodec implements ColumnarFileCodec {
  private final String schema;
  private final OrcVectorWriter.Factory writerFactory;
  private final boolean legacyTimestamp;

  public OrcCodec(String schema) {
    this(schema, false);
  }

  public OrcCodec(String schema, boolean legacyTimestamp) {
    this(schema, legacyTimestamp, null);
  }

  public OrcCodec(String schema, boolean legacyTimestamp, OrcVectorWriter.Factory factory) {
    this.schema = schema;
    this.legacyTimestamp = legacyTimestamp;
    this.writerFactory = factory;
  }

  public Encoder createEncoder(
      org.apache.arrow.vector.types.pojo.Schema schema,
      int[] partitions,
      String[] keys,
      String[] values,
      boolean changelog,
      java.io.OutputStream output)
      throws java.io.IOException {
    if (changelog) throw new IllegalArgumentException("ORC filesystem sinks require inserts");
    return new OrcEncoder(
        tech.streamfusion.arrow.ArrowFileBatch.schema(schema),
        this.schema,
        partitions,
        keys,
        values,
        output,
        writerFactory == null ? new FlinkOrcWriter() : writerFactory);
  }

  public long createDecoder(Object in, long length, long schema, String[] names, int rows) {
    return NativeOrc.createOrcDecoder(
        in,
        length,
        schema,
        names,
        rows,
        legacyTimestamp ? java.util.TimeZone.getDefault().getID() : "");
  }

  public boolean next(long h, long a, long s) {
    return NativeOrc.orcDecoderNext(h, a, s);
  }

  public long readerMemory(long h) {
    return NativeOrc.orcDecoderMaxStripeBytes(h);
  }

  public void closeDecoder(long h) {
    NativeOrc.closeOrcDecoder(h);
  }
}
