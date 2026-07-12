package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.jordepic.streamfusion.format.NativeBodyBatchDecoder;
import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.json.JsonFormatProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The body-batch decoder (the fused source's fetch-thread decode) delegates JSON to the provider. */
@Tag("streamfusion-json")
class NativeBodyBatchJsonDecodeTest {

  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()},
          new String[] {"id", "name", "score"});

  @Test
  void decodesJsonBodiesToTypedBatch() throws Exception {
    try (NativeBodyBatchDecoder decoder = decoder()) {
      List<List<Object>> rows =
          decodeRows(
              decoder,
              bodies(
                  NativeAllocator.SHARED,
                  "{\"id\": 1, \"name\": \"a\", \"score\": 1.5}",
                  "{\"id\": 2, \"name\": \"b\", \"score\": 2.5}",
                  "{\"id\": 3, \"name\": \"c\", \"score\": 3.5}"));
      assertEquals(
          List.of(
              List.of(1L, "a", 1.5),
              List.of(2L, "b", 2.5),
              List.of(3L, "c", 3.5)),
          rows);
    }
  }

  /** Fields absent from a document decode as SQL NULLs, matching the host JSON format. */
  @Test
  void missingFieldsBecomeNull() throws Exception {
    try (NativeBodyBatchDecoder decoder = decoder()) {
      assertEquals(
          List.of(List.of(7L)), decodeRows(decoder, bodies(NativeAllocator.SHARED, "{\"id\": 7}")));
    }
  }

  private static NativeBodyBatchDecoder decoder() throws Exception {
    return new NativeBodyBatchDecoder(
        new JsonFormatProvider()
            .createDecoder(new NativeFormatContext(OUTPUT, OUTPUT, Map.of("format", "json"), false)),
        OUTPUT,
        NativeAllocator.SHARED);
  }

  /** Builds an Arrow batch of one binary column ("body") holding the raw JSON documents. */
  private static VectorSchemaRoot bodies(BufferAllocator allocator, String... docs) {
    VarBinaryVector vector = new VarBinaryVector("body", allocator);
    vector.allocateNew(docs.length);
    for (int i = 0; i < docs.length; i++) {
      vector.setSafe(i, docs[i].getBytes(StandardCharsets.UTF_8));
    }
    vector.setValueCount(docs.length);
    VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector));
    root.setRowCount(docs.length);
    return root;
  }

  private static List<List<Object>> decodeRows(NativeBodyBatchDecoder decoder, VectorSchemaRoot bodies)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (VectorSchemaRoot root = decoder.decode(bodies)) {
      for (RowData r : RowDataArrowConverter.read(root, OUTPUT)) {
        List<Object> fields = new ArrayList<>(3);
        fields.add(r.getLong(0));
        if (!r.isNullAt(1)) {
          fields.add(r.getString(1).toString());
        }
        if (!r.isNullAt(2)) {
          fields.add(r.getDouble(2));
        }
        rows.add(fields);
      }
    }
    return rows;
  }
}
