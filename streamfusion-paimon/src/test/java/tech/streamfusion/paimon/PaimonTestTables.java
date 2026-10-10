package tech.streamfusion.paimon;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InternalRowUtils;

/**
 * One fixture schema in its Paimon and Flink forms, and rows built from the same values in both
 * forms, so a natively written table can be compared with a twin Paimon wrote itself.
 */
final class PaimonTestTables {
  private PaimonTestTables() {}

  static String fileFormat() {
    String format = System.getenv().getOrDefault("SF_PAIMON_FILE_FORMAT", "parquet");
    if (!List.of("parquet", "orc").contains(format)) throw new IllegalArgumentException(format);
    return format;
  }

  static Map<String, String> fileOptions(Map<String, String> options) {
    Map<String, String> result = new LinkedHashMap<>(options);
    result.putIfAbsent("file.format", fileFormat());
    return result;
  }

  static final RowType FLINK_TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(false),
            new VarCharType(VarCharType.MAX_LENGTH),
            new DecimalType(10, 2),
            new DecimalType(20, 4),
            new TimestampType(3),
            new LocalZonedTimestampType(6),
            new DateType(),
            new ArrayType(new IntType()),
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
            RowType.of(
                new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                new String[] {"a", "b"}),
            new BooleanType(),
            new DoubleType(),
            new VarBinaryType(VarBinaryType.MAX_LENGTH),
            new VarCharType(VarCharType.MAX_LENGTH)
          },
          new String[] {
            "id", "name", "price", "big", "ts", "ts6", "dt", "tags", "attrs", "nested", "flag",
            "dbl", "bin", "pt"
          });

  static final RowType PRIMARY_KEY_FLINK_TYPE =
      RowType.of(
          new LogicalType[] {
            new BigIntType(false),
            new VarCharType(false, VarCharType.MAX_LENGTH),
            new VarCharType(VarCharType.MAX_LENGTH),
            new DecimalType(10, 2),
            new TimestampType(3),
            new DateType(),
            new BooleanType(),
            new DoubleType(),
            new VarBinaryType(VarBinaryType.MAX_LENGTH),
            new TinyIntType(),
            new VarCharType(false, VarCharType.MAX_LENGTH)
          },
          new String[] {
            "id", "cat", "name", "price", "ts", "dt", "flag", "dbl", "bin", "small", "pt"
          });

  /** A partitioned primary-key table keyed on (id, cat, pt); the trimmed key is (id, cat). */
  static Schema primaryKeySchema(Map<String, String> options) {
    return Schema.newBuilder()
        .column("id", DataTypes.BIGINT().notNull())
        .column("cat", DataTypes.STRING().notNull())
        .column("name", DataTypes.STRING())
        .column("price", DataTypes.DECIMAL(10, 2))
        .column("ts", DataTypes.TIMESTAMP(3))
        .column("dt", DataTypes.DATE())
        .column("flag", DataTypes.BOOLEAN())
        .column("dbl", DataTypes.DOUBLE())
        .column("bin", DataTypes.BYTES())
        .column("small", DataTypes.TINYINT())
        .column("pt", DataTypes.STRING().notNull())
        .primaryKey("id", "cat", "pt")
        .partitionKeys("pt")
        .options(fileOptions(options))
        .build();
  }

  static FileStoreTable createPrimaryKeyTable(java.nio.file.Path dir, Map<String, String> options)
      throws Exception {
    Path path = new Path(dir.toUri());
    new SchemaManager(LocalFileIO.create(), path).createTable(primaryKeySchema(options));
    return FileStoreTableFactory.create(LocalFileIO.create(), path);
  }

  /**
   * An upsert changelog over {@code keys} distinct keys: the first row of a key inserts it, later
   * rows update or (every fifth row) delete it, and a key always stays in one partition.
   */
  static List<Object[]> changelog(int n, int keys) {
    List<Object[]> rows = new ArrayList<>();
    java.util.Set<String> seen = new java.util.HashSet<>();
    for (int i = 0; i < n; i++) {
      long id = i % keys;
      String cat = "c" + (id % 3);
      String pt = "p" + (id % 2);
      RowKind kind =
          seen.add(id + cat) ? RowKind.INSERT : i % 5 == 4 ? RowKind.DELETE : RowKind.UPDATE_AFTER;
      boolean nulls = i % 7 == 6;
      rows.add(
          new Object[] {
            kind,
            id,
            cat,
            nulls
                ? null
                : "name-" + i + (i % 5 == 0 ? "-with-a-long-suffix-over-sixteen-chars" : ""),
            nulls ? null : BigDecimal.valueOf(i * 1_00L + 7, 2),
            nulls ? null : 1_700_000_000_000L + i * 1_000L,
            nulls ? null : 19_000 + i,
            nulls ? null : i % 2 == 0,
            nulls ? null : i * 0.5,
            nulls ? null : ("bin" + i).getBytes(StandardCharsets.UTF_8),
            nulls ? null : (byte) (i % 100),
            pt
          });
    }
    return rows;
  }

  static RowData primaryKeyFlinkRow(Object[] v) {
    GenericRowData row = new GenericRowData((RowKind) v[0], v.length - 1);
    row.setField(0, v[1]);
    row.setField(1, StringData.fromString((String) v[2]));
    row.setField(2, v[3] == null ? null : StringData.fromString((String) v[3]));
    row.setField(3, v[4] == null ? null : DecimalData.fromBigDecimal((BigDecimal) v[4], 10, 2));
    row.setField(4, v[5] == null ? null : TimestampData.fromEpochMillis((Long) v[5]));
    row.setField(5, v[6]);
    row.setField(6, v[7]);
    row.setField(7, v[8]);
    row.setField(8, v[9]);
    row.setField(9, v[10]);
    row.setField(10, StringData.fromString((String) v[11]));
    return row;
  }

  static InternalRow primaryKeyPaimonRow(Object[] v) {
    GenericRow row =
        new GenericRow(
            org.apache.paimon.types.RowKind.fromByteValue(((RowKind) v[0]).toByteValue()),
            v.length - 1);
    row.setField(0, v[1]);
    row.setField(1, BinaryString.fromString((String) v[2]));
    row.setField(2, v[3] == null ? null : BinaryString.fromString((String) v[3]));
    row.setField(3, v[4] == null ? null : Decimal.fromBigDecimal((BigDecimal) v[4], 10, 2));
    row.setField(4, v[5] == null ? null : Timestamp.fromEpochMillis((Long) v[5]));
    row.setField(5, v[6]);
    row.setField(6, v[7]);
    row.setField(7, v[8]);
    row.setField(8, v[9]);
    row.setField(9, v[10]);
    row.setField(10, BinaryString.fromString((String) v[11]));
    return row;
  }

  /** Key bounds and key statistics of a primary-key data file. */
  static String describeKeys(DataFileMeta file, org.apache.paimon.types.RowType keyType) {
    return "minKey="
        + render(file.minKey(), keyType)
        + " maxKey="
        + render(file.maxKey(), keyType)
        + " keyMin="
        + render(file.keyStats().minValues(), keyType)
        + " keyMax="
        + render(file.keyStats().maxValues(), keyType)
        + " keyNulls="
        + Arrays.toString(file.keyStats().nullCounts().toLongArray());
  }

  static Schema paimonSchema(Map<String, String> options) {
    return Schema.newBuilder()
        .column("id", DataTypes.BIGINT().notNull())
        .column("name", DataTypes.STRING())
        .column("price", DataTypes.DECIMAL(10, 2))
        .column("big", DataTypes.DECIMAL(20, 4))
        .column("ts", DataTypes.TIMESTAMP(3))
        .column("ts6", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))
        .column("dt", DataTypes.DATE())
        .column("tags", DataTypes.ARRAY(DataTypes.INT()))
        .column("attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
        .column(
            "nested",
            DataTypes.ROW(
                DataTypes.FIELD(100, "a", DataTypes.INT()),
                DataTypes.FIELD(101, "b", DataTypes.STRING())))
        .column("flag", DataTypes.BOOLEAN())
        .column("dbl", DataTypes.DOUBLE())
        .column("bin", DataTypes.BYTES())
        .column("pt", DataTypes.STRING())
        .partitionKeys("pt")
        .options(fileOptions(options))
        .build();
  }

  static FileStoreTable createTable(java.nio.file.Path dir, Map<String, String> options)
      throws Exception {
    Path path = new Path(dir.toUri());
    new SchemaManager(LocalFileIO.create(), path).createTable(paimonSchema(options));
    return FileStoreTableFactory.create(LocalFileIO.create(), path);
  }

  /** Every {@code n}th row nulls out a different column family, including the partition value. */
  static List<Object[]> values(int n) {
    List<Object[]> rows = new ArrayList<>();
    String[] partitions = {"2026-01-01", "2026-01-02", null, "long-partition-value-beyond-sixteen"};
    for (int i = 0; i < n; i++) {
      boolean nulls = i % 7 == 6;
      Map<String, Long> attrs = new LinkedHashMap<>();
      attrs.put("k" + (i % 3), (long) i);
      attrs.put("z", null);
      rows.add(
          new Object[] {
            (long) i,
            nulls
                ? null
                : "name-" + i + (i % 5 == 0 ? "-with-a-long-suffix-over-sixteen-chars" : ""),
            nulls ? null : BigDecimal.valueOf(i * 1_00L + 7, 2),
            nulls ? null : new BigDecimal("12345678901234.5678").add(BigDecimal.valueOf(i)),
            nulls ? null : 1_700_000_000_000L + i * 1_000L,
            nulls ? null : new long[] {1_700_000_000_000L + i, (i % 1000) * 1_000L},
            nulls ? null : 19_000 + i,
            nulls ? null : new Integer[] {i, null, i * 2},
            nulls ? null : attrs,
            nulls ? null : new Object[] {i % 2 == 0 ? null : i, "b" + i},
            nulls ? null : i % 2 == 0,
            nulls ? null : i * 0.5,
            nulls ? null : ("bin" + i).getBytes(StandardCharsets.UTF_8),
            partitions[i % partitions.length]
          });
    }
    return rows;
  }

  static RowData flinkRow(Object[] v) {
    GenericRowData row = new GenericRowData(v.length);
    row.setField(0, v[0]);
    row.setField(1, v[1] == null ? null : StringData.fromString((String) v[1]));
    row.setField(2, v[2] == null ? null : DecimalData.fromBigDecimal((BigDecimal) v[2], 10, 2));
    row.setField(3, v[3] == null ? null : DecimalData.fromBigDecimal((BigDecimal) v[3], 20, 4));
    row.setField(4, v[4] == null ? null : TimestampData.fromEpochMillis((Long) v[4]));
    long[] micros = (long[]) v[5];
    row.setField(
        5, micros == null ? null : TimestampData.fromEpochMillis(micros[0], (int) micros[1]));
    row.setField(6, v[6]);
    row.setField(7, v[7] == null ? null : new GenericArrayData((Integer[]) v[7]));
    if (v[8] != null) {
      Map<StringData, Long> attrs = new LinkedHashMap<>();
      ((Map<?, ?>) v[8])
          .forEach((k, value) -> attrs.put(StringData.fromString((String) k), (Long) value));
      row.setField(8, new GenericMapData(attrs));
    }
    if (v[9] != null) {
      Object[] nested = (Object[]) v[9];
      GenericRowData inner = new GenericRowData(2);
      inner.setField(0, nested[0]);
      inner.setField(1, StringData.fromString((String) nested[1]));
      row.setField(9, inner);
    }
    row.setField(10, v[10]);
    row.setField(11, v[11]);
    row.setField(12, v[12]);
    row.setField(13, v[13] == null ? null : StringData.fromString((String) v[13]));
    return row;
  }

  static InternalRow paimonRow(Object[] v) {
    GenericRow row = new GenericRow(v.length);
    row.setField(0, v[0]);
    row.setField(1, v[1] == null ? null : BinaryString.fromString((String) v[1]));
    row.setField(2, v[2] == null ? null : Decimal.fromBigDecimal((BigDecimal) v[2], 10, 2));
    row.setField(3, v[3] == null ? null : Decimal.fromBigDecimal((BigDecimal) v[3], 20, 4));
    row.setField(4, v[4] == null ? null : Timestamp.fromEpochMillis((Long) v[4]));
    long[] micros = (long[]) v[5];
    row.setField(5, micros == null ? null : Timestamp.fromEpochMillis(micros[0], (int) micros[1]));
    row.setField(6, v[6]);
    row.setField(7, v[7] == null ? null : new GenericArray((Integer[]) v[7]));
    if (v[8] != null) {
      Map<BinaryString, Long> attrs = new LinkedHashMap<>();
      ((Map<?, ?>) v[8])
          .forEach((k, value) -> attrs.put(BinaryString.fromString((String) k), (Long) value));
      row.setField(8, new GenericMap(attrs));
    }
    if (v[9] != null) {
      Object[] nested = (Object[]) v[9];
      GenericRow inner = new GenericRow(2);
      inner.setField(0, nested[0]);
      inner.setField(1, BinaryString.fromString((String) nested[1]));
      row.setField(9, inner);
    }
    row.setField(10, v[10]);
    row.setField(11, v[11]);
    row.setField(12, v[12]);
    row.setField(13, v[13] == null ? null : BinaryString.fromString((String) v[13]));
    return row;
  }

  /**
   * parquet-rs follows the Parquet specification's signed-zero rule for floating-point statistics
   * (a minimum of +0.0 is recorded as -0.0) while parquet-mr records the value as seen; the bounds
   * are numerically equal, so comparisons treat the two zeros alike.
   */
  private static double withoutSignedZero(double value) {
    return value + 0.0;
  }

  /** A type-driven rendering of a Paimon row so rows from different readers compare by value. */
  static String render(InternalRow row, org.apache.paimon.types.RowType type) {
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < type.getFieldCount(); i++) {
      fields.add(render(InternalRowUtils.get(row, i, type.getTypeAt(i)), type.getTypeAt(i)));
    }
    return String.join("|", fields);
  }

  private static String render(Object value, DataType type) {
    if (value == null) {
      return "null";
    }
    switch (type.getTypeRoot()) {
      case ARRAY:
        {
          {
        InternalArray array = (InternalArray) value;
        DataType element = ((org.apache.paimon.types.ArrayType) type).getElementType();
        List<String> items = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
          items.add(render(InternalRowUtils.get(array, i, element), element));
        }
        return "[" + String.join(",", items) + "]";
      }
        }
      case MAP:
        {
          {
        InternalMap map = (InternalMap) value;
        org.apache.paimon.types.MapType mapType = (org.apache.paimon.types.MapType) type;
        List<String> entries = new ArrayList<>();
        for (int i = 0; i < map.size(); i++) {
          entries.add(
              render(
                      InternalRowUtils.get(map.keyArray(), i, mapType.getKeyType()),
                      mapType.getKeyType())
                  + "="
                  + render(
                      InternalRowUtils.get(map.valueArray(), i, mapType.getValueType()),
                      mapType.getValueType()));
        }
        return "{" + entries.stream().sorted().collect(Collectors.joining(",")) + "}";
      }
        }
      case ROW:
        {
          {
        return "(" + render((InternalRow) value, (org.apache.paimon.types.RowType) type) + ")";
      }
        }
      case BINARY:
      case VARBINARY:
        {
          {
        return Arrays.toString((byte[]) value);
      }
        }
      case FLOAT:
      case DOUBLE:
        {
          {
        return Double.toString(withoutSignedZero(((Number) value).doubleValue()));
      }
        }
      default:
        {
          {
        return value.toString();
      }
        }
    }
  }

  /** Dense statistics carry only the columns named by {@code valueStatsCols}. */
  static String describe(DataFileMeta file, org.apache.paimon.types.RowType rowType) {
    org.apache.paimon.stats.SimpleStats stats = file.valueStats();
    if (file.valueStatsCols() != null) {
      rowType = rowType.project(file.valueStatsCols());
    }
    return "min="
        + render(stats.minValues(), rowType)
        + "\nmax="
        + render(stats.maxValues(), rowType)
        + "\nnulls="
        + java.util.Arrays.toString(stats.nullCounts().toLongArray());
  }

  static List<String> readRows(FileStoreTable table, org.apache.paimon.types.RowType rowType)
      throws Exception {
    List<String> rows = new ArrayList<>();
    table
        .newReadBuilder()
        .newRead()
        .createReader(table.newReadBuilder().newScan().plan())
        .forEachRemaining(row -> rows.add(render(row, rowType)));
    rows.sort(String::compareTo);
    return rows;
  }

  static Map<String, List<DataFileMeta>> dataFiles(FileStoreTable table) {
    Map<String, List<DataFileMeta>> files = new TreeMap<>();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      files
          .computeIfAbsent(dataSplit.partition() + "@" + dataSplit.bucket(), k -> new ArrayList<>())
          .addAll(dataSplit.dataFiles());
    }
    return files;
  }

  /** The schema, row-group row counts, and per-column codecs of every data file, by destination. */
  static Map<String, List<String>> footers(FileStoreTable table) throws Exception {
    Map<String, List<String>> footers = new TreeMap<>();
    LocalFileIO fileIO = LocalFileIO.create();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      List<String> described = new ArrayList<>();
      for (DataFileMeta file : dataSplit.dataFiles()) {
        Path path = new Path(dataSplit.bucketPath(), file.fileName());
        if (file.fileFormat().equals("orc")) {
          try (var reader =
              org.apache.paimon.shade.org.apache.orc.OrcFile.createReader(
                  new org.apache.hadoop.fs.Path(path.toUri()),
                  org.apache.paimon.shade.org.apache.orc.OrcFile.readerOptions(
                      new org.apache.hadoop.conf.Configuration()))) {
            String description =
                reader.getSchema()
                    + "\nrows="
                    + reader.getNumberOfRows()
                    + " compression="
                    + reader.getCompressionKind();
            for (var type : reader.getTypes()) description += "\n" + type;
            described.add(description);
          }
          continue;
        }
        try (ParquetFileReader reader =
            ParquetUtil.getParquetReader(fileIO, path, file.fileSize(), new Options())) {
          ParquetMetadata footer = reader.getFooter();
          StringBuilder description =
              new StringBuilder(footer.getFileMetaData().getSchema().toString());
          for (BlockMetaData block : footer.getBlocks()) {
            description.append("\nrows=").append(block.getRowCount());
            for (ColumnChunkMetaData column : block.getColumns()) {
              description
                  .append(' ')
                  .append(column.getPath())
                  .append(':')
                  .append(column.getCodec());
            }
          }
          described.add(description.toString());
        }
      }
      footers.put(dataSplit.partition() + "@" + dataSplit.bucket(), described);
    }
    return footers;
  }
}
