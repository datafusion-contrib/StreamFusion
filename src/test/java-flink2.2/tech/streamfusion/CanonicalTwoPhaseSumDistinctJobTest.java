package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalTwoPhaseSumDistinctJobTest extends CanonicalMiniBatchSumDistinctJobTest {
  @Override
  String statefulFamily() {
    return "NativeColumnarGroupAgg";
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.deleteIfExists(root.resolve("one-phase-mini-batch"));
    Files.writeString(root.resolve("two-phase-mini-batch"), "5");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-global-group-aggregate");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeColumnarLocalGroupAgg,NativeColumnarGroupAgg,RowDataToArrow,ArrowToRowData");
    Files.writeString(root.resolve("native-input-families"), "LocalGroup,NativeColumnarGroupAgg");
    return root;
  }
}
