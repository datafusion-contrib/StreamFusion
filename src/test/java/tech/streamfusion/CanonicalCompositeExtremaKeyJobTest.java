package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

class CanonicalCompositeExtremaKeyJobTest extends CanonicalRetractingExtremaJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(root.resolve("input-columns.sql"), "ka SMALLINT,kb DECIMAL(18,2),v BIGINT");
    var input = new ArrayList<String>();
    for (String line : Files.readAllLines(root.resolve("input.csv"))) {
      var cells = line.split(",", -1);
      input.add(cells[0] + "," + cells[1] + "," + cells[1] + "," + cells[2]);
    }
    Files.write(root.resolve("input.csv"), input);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT CAST(ka AS BIGINT),MIN(v),MAX(v) FROM input_table GROUP BY"
            + " ka,kb");
    Files.writeString(root.resolve("required-extrema-key-fields"), "2");
    return root;
  }
}
