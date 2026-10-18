package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeIcebergPredicateTest {
  @TempDir Path temp;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gte",
        "and",
        "in",
        "or",
        "not_in",
        "not_prefix",
        "not",
        "lt_string",
        "not_eq_string",
        "not_in_string"
      })
  void nativeFilteringMatchesTheReleasedJavaReaderAndItsResumeUnits(String shape) throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, true);
    List<Expression> filters =
        List.of(
            switch (shape) {
              case "gte" -> Expressions.greaterThanOrEqual("id", 20);
              case "and" ->
                  Expressions.and(Expressions.lessThan("id", 50), Expressions.notEqual("id", 13));
              case "in" -> Expressions.in("id", 1, 7, 17, 23, 40, 77, 100);
              case "or" ->
                  Expressions.or(
                      Expressions.isNull("payload"), Expressions.startsWith("payload", "v-é🐍-1"));
              case "not_in" -> Expressions.notIn("id", 1, 2, 3);
              case "not_prefix" -> Expressions.notStartsWith("payload", "v-é🐍-1");
              case "not" -> Expressions.not(Expressions.lessThanOrEqual("id", 30));
              case "lt_string" -> Expressions.lessThan("payload", "v-é🐍-3");
              case "not_eq_string" -> Expressions.notEqual("payload", "v-é🐍-3");
              case "not_in_string" -> Expressions.notIn("payload", "v-é🐍-3", "v-é🐍-4");
              default -> throw new AssertionError(shape);
            });
    var projection = table.schema().select("payload");
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        for (int skip : new int[] {0, 2}) {
          var expected = stock(table, task, projection, filters, skip);
          var actual = new ArrayList<String>();
          try (var reader =
              new IcebergNativeTaskReader(
                  task, table.schema(), List.of(2), table.io(), 7, skip, filters, true, null)) {
            org.apache.arrow.vector.VectorSchemaRoot next;
            while ((next = reader.next()) != null) {
              try (var root = next) {
                assertEquals(
                    List.of("payload"),
                    root.getSchema().getFields().stream().map(f -> f.getName()).toList());
                for (int row = 0; row < root.getRowCount(); row++) {
                  Object value = root.getVector(0).getObject(row);
                  actual.add(value == null ? null : value.toString());
                }
              }
            }
          }
          assertEquals(expected, actual, shape + " offset=" + skip);
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static List<String> stock(
      Table table, FileScanTask task, Schema projection, List<Expression> filters, int skip)
      throws Exception {
    var function =
        new RowDataReaderFunction(
            new Configuration(),
            table.schema(),
            table.schema(),
            null,
            true,
            table.io(),
            table.encryption(),
            filters);
    int payload = table.schema().columns().indexOf(table.schema().findField("payload"));
    var split =
        IcebergSourceSplit.fromCombinedScanTask(new BaseCombinedScanTask(List.of(task)), 0, skip);
    var result = new ArrayList<String>();
    try (var input = function.apply(split)) {
      while (input.hasNext()) {
        var records = input.next();
        try {
          while (records.nextSplit() != null) {
            org.apache.iceberg.flink.source.reader.RecordAndPosition<
                    org.apache.flink.table.data.RowData>
                row;
            while ((row = records.nextRecordFromSplit()) != null) {
              result.add(
                  row.record().isNullAt(payload)
                      ? null
                      : row.record().getString(payload).toString());
            }
          }
        } finally {
          records.recycle();
        }
      }
    }
    return result;
  }
}
