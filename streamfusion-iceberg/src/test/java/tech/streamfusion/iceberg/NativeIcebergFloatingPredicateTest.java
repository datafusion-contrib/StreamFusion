package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NativeIcebergFloatingPredicateTest {
  @TempDir Path temp;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            single ->
                Stream.of(
                        "eq_zero",
                        "not_eq_zero",
                        "lt_zero",
                        "lte_zero",
                        "gt_zero",
                        "gte_zero",
                        "eq_inf",
                        "not_eq_inf",
                        "in",
                        "not_in",
                        "nan",
                        "not_nan",
                        "null",
                        "not_null")
                    .map(shape -> Arguments.of(single, shape)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void floatingPredicatesMatchJavaForNaNsSignedZerosAndNulls(boolean single, String shape)
      throws Exception {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2, "number", single ? Types.FloatType.get() : Types.DoubleType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("floats").toUri().toString());
    var factory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/data.parquet")),
            table.spec(),
            null);
    Double[] values = {
      Double.NEGATIVE_INFINITY, -1d, -0d, 0d, 1d, Double.NaN, Double.POSITIVE_INFINITY, null
    };
    try (writer) {
      var record = GenericRecord.create(table.schema());
      for (int id = 0; id < values.length; id++) {
        record.setField("id", id);
        if (values[id] == null) record.setField("number", null);
        else if (single) record.setField("number", values[id].floatValue());
        else record.setField("number", values[id]);
        writer.write(record);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    Number infinity;
    if (single) infinity = Float.valueOf(Float.POSITIVE_INFINITY);
    else infinity = Double.valueOf(Double.POSITIVE_INFINITY);
    Expression predicate =
        switch (shape) {
          case "eq_zero" -> Expressions.equal("number", -0d);
          case "not_eq_zero" -> Expressions.notEqual("number", -0d);
          case "lt_zero" -> Expressions.lessThan("number", 0d);
          case "lte_zero" -> Expressions.lessThanOrEqual("number", 0d);
          case "gt_zero" -> Expressions.greaterThan("number", 0d);
          case "gte_zero" -> Expressions.greaterThanOrEqual("number", 0d);
          case "eq_inf" -> Expressions.equal("number", infinity);
          case "not_eq_inf" -> Expressions.notEqual("number", infinity);
          case "in" -> Expressions.in("number", -0d, 0d, 1d, infinity);
          case "not_in" -> Expressions.notIn("number", -0d, 0d, 1d, infinity);
          case "nan" -> Expressions.isNaN("number");
          case "not_nan" -> Expressions.notNaN("number");
          case "null" -> Expressions.isNull("number");
          case "not_null" -> Expressions.notNull("number");
          default -> throw new AssertionError(shape);
        };
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        var expected = new ArrayList<Integer>();
        var stock =
            new RowDataReaderFunction(
                new Configuration(),
                table.schema(),
                table.schema(),
                null,
                true,
                table.io(),
                table.encryption(),
                List.of(predicate));
        var split =
            IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(List.of(task)));
        try (var input = stock.apply(split)) {
          while (input.hasNext()) {
            var records = input.next();
            try {
              while (records.nextSplit() != null) {
                org.apache.iceberg.flink.source.reader.RecordAndPosition<
                        org.apache.flink.table.data.RowData>
                    row;
                while ((row = records.nextRecordFromSplit()) != null)
                  expected.add(row.record().getInt(0));
              }
            } finally {
              records.recycle();
            }
          }
        }
        var actual = new ArrayList<Integer>();
        try (var reader =
            new IcebergNativeTaskReader(
                task,
                table.schema(),
                List.of(1),
                table.io(),
                2,
                0,
                List.of(predicate),
                true,
                null)) {
          org.apache.arrow.vector.VectorSchemaRoot next;
          while ((next = reader.next()) != null) {
            try (var root = next) {
              for (int row = 0; row < root.getRowCount(); row++)
                actual.add((Integer) root.getVector(0).getObject(row));
            }
          }
        }
        assertEquals(expected, actual, shape + " single=" + single);
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }
}
