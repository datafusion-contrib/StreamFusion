package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalRetractingAggregateGraphJobTest extends CanonicalRetractingTopNJobTest {
  static Stream<Arguments> cases() {
    return CanonicalRetractingTopNJobTest.cases().filter(test -> test.get()[0].equals("rank"));
  }

  @Override
  Path prepare(String name, String variant, String mode) throws Exception {
    var root = super.prepare(name, variant, mode);
    Files.writeString(root.resolve("uid-generation"), "ALWAYS");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeColumnarGroupAgg,NativeColumnarTopN,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "GroupAgg,TopN");
    return root;
  }
}
