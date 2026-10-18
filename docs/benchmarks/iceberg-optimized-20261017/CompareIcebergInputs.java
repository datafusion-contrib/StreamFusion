import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import org.apache.iceberg.shaded.org.apache.parquet.format.*;
public class CompareIcebergInputs {
  static int footerStart(byte[] data) {
    return data.length - 8 - ByteBuffer.wrap(data, data.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }
  static FileMetaData footer(byte[] data) throws Exception {
    var metadata = Util.readFileMetaData(new ByteArrayInputStream(data, footerStart(data), data.length-8-footerStart(data)));
    for (var rowGroup : metadata.getRow_groups()) for (var column : rowGroup.getColumns()) {
      var m = column.getMeta_data();
      m.getEncodings().sort(Comparator.comparingInt(Encoding::getValue));
      if (m.isSetEncoding_stats()) m.getEncoding_stats().sort(Comparator.comparingInt((PageEncodingStats p) -> p.getPage_type().getValue()).thenComparingInt(p -> p.getEncoding().getValue()));
    }
    return metadata;
  }
  public static void main(String[] args) throws Exception {
    Path baseline=Path.of(args[0]), current=Path.of(args[1]); int checked=0, differing=0;
    try (var files=Files.walk(baseline.resolve("warehouse/bench"))) {
      for (var file : files.filter(p -> p.toString().contains("/input_") && p.toString().endsWith(".parquet")).toList()) {
        var relative=baseline.relativize(file); var a=Files.readAllBytes(file); var fa=footer(a);
        {
          var b=Files.readAllBytes(current.resolve(relative));
          if (!Arrays.equals(a,0,footerStart(a),b,0,footerStart(b))) throw new AssertionError("Data pages differ: "+relative);
          if (!fa.equals(footer(b))) throw new AssertionError("Canonical metadata differs: "+relative);
          checked++; if (!Arrays.equals(a,b)) differing++;
        }
      }
    }
    System.out.println("{\"comparedFiles\":"+checked+",\"differentFileBytes\":"+differing+",\"identicalDataPages\":true,\"equalDecodedFootersAfterSortingUnorderedEncodingLists\":true}");
  }
}
