package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class RowDataArrowConverterTest {

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType(),
            new BooleanType(),
            new VarCharType(VarCharType.MAX_LENGTH)
          },
          new String[] {"a", "b", "c", "d", "e", "f", "g", "h"});

  @Test
  void roundTripsEveryColumnTypeAndNulls() {
    GenericRowData first = new GenericRowData(8);
    first.setField(0, (byte) 1);
    first.setField(1, (short) 2);
    first.setField(2, 3);
    first.setField(3, 4L);
    first.setField(4, 5.5f);
    first.setField(5, 6.5);
    first.setField(6, true);
    first.setField(7, StringData.fromString("hello"));

    // A row that is null in every column exercises the null path on every vector type.
    GenericRowData nulls = new GenericRowData(8);
    for (int c = 0; c < 8; c++) {
      nulls.setField(c, null);
    }

    List<RowData> rows = List.of(first, nulls);
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, SCHEMA, allocator)) {
      assertEquals(2, root.getRowCount());
      List<RowData> back = RowDataArrowConverter.read(root, SCHEMA);
      assertEquals(2, back.size());
      GenericRowData backFirst = (GenericRowData) back.get(0);
      GenericRowData backNulls = (GenericRowData) back.get(1);
      for (int c = 0; c < 8; c++) {
        assertEquals(first.getField(c), backFirst.getField(c), "column " + c);
        assertEquals(null, backNulls.getField(c), "null column " + c);
      }
    }
  }

  @Test
  void reportsUnsupportedSchemas() {
    assertTrue(RowDataArrowConverter.supports(SCHEMA));
    RowType withTimestamp =
        RowType.of(new LogicalType[] {new IntType(), new TimestampType(3)}, new String[] {"a", "t"});
    assertFalse(RowDataArrowConverter.supports(withTimestamp));
  }
}
