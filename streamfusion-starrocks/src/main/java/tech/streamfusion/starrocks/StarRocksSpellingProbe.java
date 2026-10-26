package tech.streamfusion.starrocks;

import com.starrocks.connector.flink.row.sink.StarRocksCsvSerializer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.tools.JsonWrapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.InstantiationUtil;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.format.LogicalTypeDescriptors;

/**
 * Referee for the spellings the stock serializer takes from the JVM. CSV numbers use the legacy
 * {@code Float/Double.toString} (JDK 18 and earlier) of the JVM that writes them. DATE uses a
 * {@code SimpleDateFormat} that the connector creates where the job is planned, so its zone, locale
 * and calendar travel with the job, applied to a {@code java.sql.Date} each writer task builds in
 * its own zone. The stock serializer and the native encoder both spell a corpus of divergent
 * values; any difference keeps the connector's own serializer.
 */
public final class StarRocksSpellingProbe {
  private static final long[] DIVERGENT_DOUBLE_BITS = {
    0x43B010EECEFE7DC2L, 0xC3D4DDBEC2FB191DL, 0xC3ADB50F5FBBE557L
  };
  private static final int[] CALENDAR_DAYS = {
    -719_529, -719_528, -719_163, -719_162, -141_715, -141_714, -141_438, -141_437, -141_428,
    -141_427, -141_426, -1, 0, 11_016, 2_932_896, 2_932_897
  };

  private StarRocksSpellingProbe() {}

  /**
   * Why the native spelling differs for one of the sink's types when this JVM both plans and runs
   * the job, or null.
   */
  public static String mismatch(RowType sinkType, boolean json) throws IOException {
    return mismatch(sinkType, json, dateConverter(), ZoneId.systemDefault());
  }

  /**
   * Why the native spelling differs for one of the sink's types on this writer task, given the DATE
   * converter and zone of the JVM that planned the job, or null.
   */
  static String mismatch(StarRocksEncoding encoding) throws IOException {
    return mismatch(encoding.type, encoding.json, encoding.dateConverter, encoding.plannerZone);
  }

  private static String mismatch(
      RowType sinkType,
      boolean json,
      StarRocksTableRowTransformer dateConverter,
      ZoneId plannerZone)
      throws IOException {
    for (LogicalTypeRoot root :
        List.of(LogicalTypeRoot.FLOAT, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.DATE)) {
      LogicalType column =
          sinkType.getChildren().stream().filter(type -> type.is(root)).findFirst().orElse(null);
      if (column == null || json && root != LogicalTypeRoot.DATE) continue;
      List<RowData> rows;
      try {
        rows = probeRows(root, plannerZone);
      } catch (ZoneRulesException unknown) {
        return "the planning JVM's time zone " + plannerZone + " is unknown to this JVM";
      }
      StarRocksTableRowTransformer converter =
          root == LogicalTypeRoot.DATE ? copy(dateConverter) : converter(column);
      if (!stock(converter, rows).equals(nativeSpelling(column, rows))) {
        return root == LogicalTypeRoot.DATE
            ? "DATE spelling differs from this JVM's SimpleDateFormat (default locale, calendar"
                + " or time zone)"
            : root + " spelling differs from this JVM's legacy Float/Double.toString (JDK 19+)";
      }
    }
    return null;
  }

  /**
   * The connector's converter for one DATE column, built as its sink builds one. The formatter it
   * holds keeps this JVM's default zone, locale and calendar wherever it is deserialized.
   */
  static StarRocksTableRowTransformer dateConverter() {
    return converter(new DateType());
  }

  private static StarRocksTableRowTransformer converter(LogicalType column) {
    RowType type = RowType.of(new LogicalType[] {column}, new String[] {"v"});
    var converter = new StarRocksTableRowTransformer(InternalTypeInfo.of(type));
    converter.setTableSchema(
        TableSchema.builder().field("v", TypeConversions.fromLogicalToDataType(column)).build());
    return converter;
  }

  private static StarRocksTableRowTransformer copy(StarRocksTableRowTransformer converter)
      throws IOException {
    try {
      return InstantiationUtil.clone(converter, StarRocksSpellingProbe.class.getClassLoader());
    } catch (ClassNotFoundException incompatible) {
      throw new IOException("cannot copy the connector's DATE converter", incompatible);
    }
  }

  /**
   * Values whose spellings differ between JVMs. DATE covers the calendar cutover, the year range
   * and the days around every offset change of this JVM's zone and of the planning JVM's zone.
   */
  static List<RowData> probeRows(LogicalTypeRoot root, ZoneId plannerZone) {
    List<RowData> rows = new ArrayList<>();
    switch (root) {
      case DOUBLE:
        Random doubles = new Random(0x5DF0A7D0);
        for (double edge :
            new double[] {
              Double.NaN,
              Double.POSITIVE_INFINITY,
              Double.NEGATIVE_INFINITY,
              0.0d,
              -0.0d,
              Double.MIN_VALUE,
              Double.MIN_NORMAL,
              Double.MAX_VALUE,
              0.1d,
              1.0d / 3.0d,
              1e-4d,
              1e-3d,
              1e7d,
              1e8d
            }) rows.add(GenericRowData.of(edge));
        for (long bits : DIVERGENT_DOUBLE_BITS)
          rows.add(GenericRowData.of(Double.longBitsToDouble(bits)));
        while (rows.size() < 81) {
          double value = Double.longBitsToDouble(doubles.nextLong());
          if (!Double.isNaN(value)) rows.add(GenericRowData.of(value));
        }
        return rows;
      case FLOAT:
        Random floats = new Random(0x5DF0A7F0);
        for (float edge :
            new float[] {
              Float.NaN,
              Float.POSITIVE_INFINITY,
              Float.NEGATIVE_INFINITY,
              0.0f,
              -0.0f,
              Float.MIN_VALUE,
              Float.MIN_NORMAL,
              Float.MAX_VALUE,
              0.1f,
              1.0f / 3.0f,
              1e-4f,
              1e-3f,
              1e7f,
              1e8f
            }) rows.add(GenericRowData.of(edge));
        while (rows.size() < 142) {
          float value = Float.intBitsToFloat(floats.nextInt());
          if (!Float.isNaN(value)) rows.add(GenericRowData.of(value));
        }
        return rows;
      default:
        TreeSet<Integer> days = new TreeSet<>();
        for (int day : CALENDAR_DAYS) days.add(day);
        // A zone transition can move or skip a local midnight, or change how far apart the two
        // zones are, which shifts the stock date.
        for (ZoneId zone : List.of(ZoneId.systemDefault(), plannerZone))
          for (ZoneOffsetTransition transition : zone.getRules().getTransitions()) {
            long first = transition.getDateTimeBefore().toLocalDate().toEpochDay() - 1;
            long last = transition.getDateTimeAfter().toLocalDate().toEpochDay() + 1;
            for (long day = first; day <= last; day++) days.add((int) day);
          }
        for (int day : days) rows.add(GenericRowData.of(day));
        return rows;
    }
  }

  /** The rows a connector converter and its CSV serializer write, after opening the converter. */
  static List<String> stock(StarRocksTableRowTransformer converter, List<RowData> rows) {
    converter.setRuntimeContext(null);
    JsonWrapper json = new JsonWrapper();
    converter.setFastJsonWrapper(json);
    var serializer = new StarRocksCsvSerializer("\t");
    serializer.open(new StarRocksISerializer.SerializerContext(json));
    List<String> spelled = new ArrayList<>(rows.size());
    for (RowData row : rows) spelled.add(serializer.serialize(converter.transform(row, false)));
    return spelled;
  }

  static List<String> nativeSpelling(LogicalType column, List<RowData> rows) throws IOException {
    RowType type = RowType.of(new LogicalType[] {column}, new String[] {"v"});
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (RowData row : rows) writer.write(row);
      writer.finish();
      long handle =
          NativeStarRocks.createEncoder(
              false,
              "\t".getBytes(StandardCharsets.UTF_8),
              LogicalTypeDescriptors.of(type),
              new String[] {"v"},
              new int[0],
              false,
              true);
      try {
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
          NativeStarRocks.begin(handle, array.memoryAddress(), schema.memoryAddress());
        }
        List<String> spelled = new ArrayList<>(rows.size());
        int[] ends = new int[rows.size()];
        int[] count = new int[1];
        for (byte[] slab; (slab = NativeStarRocks.next(handle, ends, count)) != null; ) {
          int start = 0;
          for (int row = 0; row < count[0]; row++) {
            spelled.add(new String(slab, start, ends[row] - start, StandardCharsets.UTF_8));
            start = ends[row];
          }
        }
        return spelled;
      } finally {
        NativeStarRocks.reset(handle);
        NativeStarRocks.closeEncoder(handle);
      }
    }
  }
}
