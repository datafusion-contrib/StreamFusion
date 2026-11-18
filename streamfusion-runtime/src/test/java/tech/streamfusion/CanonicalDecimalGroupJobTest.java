package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalDecimalGroupJobTest extends CanonicalGroupArithmeticJobTest {
  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(CanonicalGroupArithmeticJobTest.cases());
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.writeString(
        root.resolve("query.sql"),
        "INSERT INTO output_table SELECT k,CAST(SUM(CAST(v AS DECIMAL(38,2))) AS BIGINT),"
            + "CAST(AVG(CAST(v AS DECIMAL(38,2))) AS BIGINT) FROM input_table GROUP BY k");
    return root;
  }
}
