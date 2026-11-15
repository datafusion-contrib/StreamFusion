package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RecoveryResultEncodingTest {
  private final RowDataSerializer serializer =
      new RowDataSerializer(
          (RowType)
              DataTypes.ROW(
                      DataTypes.FIELD("v", DataTypes.BYTES()),
                      DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.BYTES())),
                      DataTypes.FIELD("m", DataTypes.MULTISET(DataTypes.BYTES())))
                  .getLogicalType());

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void transportKeepsNestedBytesNullsCountsAndChangelogKinds(boolean compressed) throws Exception {
    byte[] wide = new byte[16_384];
    for (int i = 0; i < wide.length; i++) wide[i] = (byte) i;
    var multiset = new LinkedHashMap<byte[], Integer>();
    multiset.put(wide, 7);
    multiset.put(null, 3);
    var populated =
        GenericRowData.of(
            wide,
            new GenericArrayData(new byte[][] {wide, null, new byte[0]}),
            new GenericMapData(multiset));
    for (var row : new GenericRowData[] {populated, GenericRowData.of(null, null, null)}) {
      for (RowKind kind : RowKind.values()) {
        row.setRowKind(kind);
        byte[] encoded = RecoveryResultEncoding.encode(serializer, row, compressed);
        RowData decoded = RecoveryResultEncoding.decode(serializer, encoded, compressed);
        assertArrayEquals(bytes(row), bytes(decoded));
        if (row == populated && compressed) assertTrue(encoded.length < bytes(row).length / 4);
        if (!compressed) assertArrayEquals(bytes(row), encoded);
      }
    }
  }

  @Test
  void corruptCompressedResultsFailInsteadOfBecomingEmptyResults() {
    assertThrows(
        IOException.class,
        () -> RecoveryResultEncoding.decode(serializer, new byte[] {1, 2, 3}, true));
  }

  private byte[] bytes(RowData row) throws IOException {
    var output = new DataOutputSerializer(64);
    serializer.serialize(row, output);
    return output.getCopyOfBuffer();
  }
}
