package tech.streamfusion;

import java.nio.charset.StandardCharsets;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;

/** Released Flink comparisons over the prepared inputs used by the Rust driver. */
public final class PairwiseStringComparisonBenchmark {
  private PairwiseStringComparisonBenchmark() {}

  public static void main(String[] args) {
    int iterations = args.length == 0 ? 128 : Integer.parseInt(args[0]);
    int rounds = args.length < 2 ? 7 : Integer.parseInt(args[1]);
    boolean equality = args.length >= 3 && args[2].equals("eq");
    boolean ascii = args.length >= 4 && args[3].equals("ascii");
    System.out.println("engine,mode,shape,width,trial,warmup,ns_per_row,checksum");
    for (String mode : new String[] {"bytes", "utf16", "disjoint", "mixed"}) {
      for (String shape : new String[] {"early", "prefix"}) {
        for (int width : new int[] {24, 192}) {
          RowData[] input = input(mode, shape, width, equality, ascii);
          for (int trial = 0; trial < rounds + 5; trial++) {
            long checksum = 0;
            long start = System.nanoTime();
            for (int iteration = 0; iteration < iterations; iteration++) {
              checksum += equality ? countEqual(input) : countLess(input);
            }
            double perRow = (System.nanoTime() - start) / (double) (iterations * input.length);
            System.out.printf(
                java.util.Locale.ROOT, "flink,%s,%s,%d,%d,%b,%.3f,%d%n",
                mode, shape, width, trial, trial < 5, perRow, checksum);
          }
        }
      }
    }
  }

  private static int countLess(RowData[] input) {
    int count = 0;
    for (RowData row : input) {
      if (!row.isNullAt(0) && row.getString(0).compareTo(row.getString(1)) < 0) count++;
    }
    return count;
  }

  private static int countEqual(RowData[] input) {
    int count = 0;
    for (RowData row : input) {
      if (!row.isNullAt(0) && row.getString(0).equals(row.getString(1))) count++;
    }
    return count;
  }

  private static RowData[] input(String mode, String shape, int width, boolean equality, boolean ascii) {
    var rows = new RowData[1024];
    String filler = ascii ? "m".repeat(width) : "\u4e2d".repeat(width / 3);
    String first = ascii ? "a" : "\uD800\uDC00";
    String second = ascii ? "z" : "\uE000";
    for (int row = 0; row < rows.length; row++) {
      var values = new GenericRowData(2);
      for (int column = 0; column < 2; column++) {
        if (row % 31 == 0 && column == 0) continue;
        boolean differs = column == 1 && (!equality || row % 2 != 0);
        String distinctive = (row % 8 == 1) == differs ? first : second;
        String value = shape.equals("early") ? distinctive + filler : filler + distinctive;
        boolean cached;
        switch (mode) {
          case "utf16":
            cached = true;
            break;
          case "disjoint":
            cached = row % 2 == column;
            break;
          case "mixed":
            cached = (row % 4 & (1 << column)) == 0;
            break;
          default:
            cached = false;
        }
        var string = BinaryStringData.fromBytes(value.getBytes(StandardCharsets.UTF_8));
        if (cached) string.toString();
        values.setField(column, string);
      }
      rows[row] = values;
    }
    return rows;
  }
}
