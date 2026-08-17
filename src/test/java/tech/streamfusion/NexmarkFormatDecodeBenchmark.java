package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import tech.streamfusion.format.NativeFormatContext;
import tech.streamfusion.format.NativeFormatProvider;
import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.avro.AvroFormatProvider;
import tech.streamfusion.format.json.JsonFormatProvider;
import tech.streamfusion.format.protobuf.ProtobufFormatProvider;
import tech.streamfusion.proto.NexmarkEvent;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Measures only Nexmark Kafka value decoding into each engine's destination representation. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NexmarkFormatDecodeBenchmark {

  private static final int BATCH_ROWS =
      Integer.parseInt(System.getenv().getOrDefault("SF_DECODE_BATCH_ROWS", "8192"));
  private static final int WARMUP_SECONDS =
      Integer.parseInt(System.getenv().getOrDefault("SF_DECODE_WARMUP_SECONDS", "1"));
  private static final int RUN_SECONDS =
      Integer.parseInt(System.getenv().getOrDefault("SF_DECODE_SECONDS", "2"));
  private static final int RUNS =
      Integer.parseInt(System.getenv().getOrDefault("SF_DECODE_RUNS", "3"));
  private static volatile Object blackhole;

  @Test
  void decodeThroughput() throws Exception {
    RowType rowType = NexmarkKafkaBenchmark.nexmarkRowType();
    System.out.printf(
        "%n##### NEXMARK FORMAT DECODE (%d-row batches; best of %d) #####%n",
        BATCH_ROWS, RUNS);
    System.out.println(
        "format    Java RowData M rows/s   ns/row   Native Arrow M rows/s   ns/row   Native/Java");
    for (String format : List.of("json", "avro", "protobuf")) {
      byte[][] messages = messages(format, rowType);
      JavaDecoder javaDecoder = javaDecoder(format, rowType);
      NativeFormatProvider provider = nativeProvider(format);
      Map<String, String> options = nativeOptions(format);
      try (NativeBatchDecoder nativeDecoder =
          new NativeBatchDecoder(rowType, messages, provider, options)) {
        assertEquals(BATCH_ROWS, javaDecoder.decode(messages));
        assertEquals(BATCH_ROWS, nativeDecoder.decode());
        double javaRowsPerSecond = best(javaDecoder, messages);
        double nativeRowsPerSecond = best(nativeDecoder);
        System.out.printf(
            Locale.ROOT,
            "%-10s %15.3f %8.1f %23.3f %8.1f %12.2fx%n",
            format,
            javaRowsPerSecond / 1_000_000.0,
            1_000_000_000.0 / javaRowsPerSecond,
            nativeRowsPerSecond / 1_000_000.0,
            1_000_000_000.0 / nativeRowsPerSecond,
            nativeRowsPerSecond / javaRowsPerSecond);
      }
    }
  }

  private static double best(JavaDecoder decoder, byte[][] messages) throws Exception {
    runFor(WARMUP_SECONDS, () -> decoder.decode(messages));
    double best = 0;
    for (int run = 0; run < RUNS; run++) {
      best = Math.max(best, runFor(RUN_SECONDS, () -> decoder.decode(messages)));
    }
    return best;
  }

  private static double best(NativeBatchDecoder decoder) throws Exception {
    runFor(WARMUP_SECONDS, decoder::decode);
    double best = 0;
    for (int run = 0; run < RUNS; run++) {
      best = Math.max(best, runFor(RUN_SECONDS, decoder::decode));
    }
    return best;
  }

  private static double runFor(int seconds, DecodeBatch decode) throws Exception {
    long started = System.nanoTime();
    long deadline = started + seconds * 1_000_000_000L;
    long rows = 0;
    do {
      rows += decode.run();
    } while (System.nanoTime() < deadline);
    return rows * 1_000_000_000.0 / (System.nanoTime() - started);
  }

  private static byte[][] messages(String format, RowType rowType) throws Exception {
    byte[][] messages = new byte[BATCH_ROWS][];
    Schema schema =
        "avro".equals(format)
            ? AvroSchemaConverter.convertToSchema(rowType.copy(false))
            : null;
    GenericDatumWriter<GenericRecord> writer =
        schema == null ? null : new GenericDatumWriter<>(schema);
    for (int i = 0; i < messages.length; i++) {
      switch (format) {
        case "json":
          messages[i] = NexmarkKafkaBenchmark.event(i).getBytes(StandardCharsets.UTF_8);
          break;
        case "avro":
          ByteArrayOutputStream output = new ByteArrayOutputStream();
          BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
          writer.write(NexmarkKafkaBenchmark.avroEvent(i, schema), encoder);
          encoder.flush();
          messages[i] = output.toByteArray();
          break;
        case "protobuf":
          messages[i] = NexmarkKafkaBenchmark.protobufEvent(i).toByteArray();
          break;
        default:
          throw new IllegalArgumentException("unknown format: " + format);
      }
    }
    return messages;
  }

  private static JavaDecoder javaDecoder(String format, RowType rowType) throws Exception {
    RowType physicalType = (RowType) rowType.copy(false);
    InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(physicalType);
    switch (format) {
      case "json":
        JsonRowDataDeserializationSchema json =
            new JsonRowDataDeserializationSchema(
                physicalType, typeInfo, false, false, TimestampFormat.SQL);
        json.open(initializationContext());
        return messages -> {
          LastRow collector = new LastRow();
          for (byte[] message : messages) {
            json.deserialize(message, collector);
          }
          blackhole = collector.last;
          return collector.count;
        };
      case "avro":
        AvroRowDataDeserializationSchema avro =
            new AvroRowDataDeserializationSchema(
                physicalType, typeInfo, AvroEncoding.BINARY, true);
        avro.open(initializationContext());
        return rowDecoder(avro);
      case "protobuf":
        PbRowDataDeserializationSchema protobuf =
            new PbRowDataDeserializationSchema(
                physicalType,
                typeInfo,
                new PbFormatConfig(NexmarkEvent.class.getName(), false, false, ""));
        protobuf.open(initializationContext());
        return rowDecoder(protobuf);
      default:
        throw new IllegalArgumentException("unknown format: " + format);
    }
  }

  private static JavaDecoder rowDecoder(DeserializationSchema<RowData> decoder) {
    return messages -> {
      RowData last = null;
      int count = 0;
      for (byte[] message : messages) {
        last = decoder.deserialize(message);
        if (last != null) {
          count++;
        }
      }
      blackhole = last;
      return count;
    };
  }

  private static DeserializationSchema.InitializationContext initializationContext() {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
      }

      @Override
      public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(NexmarkFormatDecodeBenchmark.class.getClassLoader());
      }
    };
  }

  private static NativeFormatProvider nativeProvider(String format) {
    switch (format) {
      case "json":
        return new JsonFormatProvider();
      case "avro":
        return new AvroFormatProvider();
      case "protobuf":
        return new ProtobufFormatProvider();
      default:
        throw new IllegalArgumentException("unknown format: " + format);
    }
  }

  private static Map<String, String> nativeOptions(String format) {
    if ("protobuf".equals(format)) {
      return Map.of(
          "format", "protobuf", "protobuf.message-class-name", NexmarkEvent.class.getName());
    }
    return Map.of("format", format);
  }

  @FunctionalInterface
  private interface DecodeBatch {
    int run() throws Exception;
  }

  @FunctionalInterface
  private interface JavaDecoder {
    int decode(byte[][] messages) throws Exception;
  }

  private static final class LastRow implements Collector<RowData> {
    private RowData last;
    private int count;

    @Override
    public void collect(RowData row) {
      last = row;
      count++;
    }

    @Override
    public void close() {}
  }

  private static final class NativeBatchDecoder implements AutoCloseable {
    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private final CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
    private final NativeMessageDecoder decoder;
    private final VectorSchemaRoot input;

    private NativeBatchDecoder(
        RowType rowType,
        byte[][] messages,
        NativeFormatProvider provider,
        Map<String, String> options)
        throws Exception {
      VarBinaryVector bodies = new VarBinaryVector("body", allocator);
      bodies.allocateNew();
      for (int i = 0; i < messages.length; i++) {
        bodies.setSafe(i, messages[i]);
      }
      bodies.setValueCount(messages.length);
      input = new VectorSchemaRoot(List.of(bodies));
      input.setRowCount(messages.length);
      decoder =
          provider
              .createDecoder(new NativeFormatContext(rowType, rowType, options, false))
              .create();
      decoder.open(allocator, rowType);
    }

    private int decode() throws Exception {
      try (ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, input, dictionaries, inArray, inSchema);
        decoder.decodeInto(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
        try (VectorSchemaRoot output =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
          blackhole = output.getVector(0);
          return output.getRowCount();
        }
      }
    }

    @Override
    public void close() throws Exception {
      decoder.close();
      input.close();
      dictionaries.close();
      allocator.close();
    }
  }
}
