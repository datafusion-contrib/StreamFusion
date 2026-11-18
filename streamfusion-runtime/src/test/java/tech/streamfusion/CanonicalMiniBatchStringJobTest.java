package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalMiniBatchStringJobTest extends CanonicalAppendStringExtremaJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(
            test ->
                !Boolean.getBoolean("sf.canonical.minibatch.job.smoke")
                    || test.get()[0].equals("continue")
                        && !(boolean) test.get()[1]
                        && !(boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"))
        .filter(
            test ->
                System.getProperty("sf.canonical.minibatch.job.mode", "all").equals("all")
                    || test.get()[0].equals(System.getProperty("sf.canonical.minibatch.job.mode")));
  }

  @Override
  String statefulFamily() {
    return twoPhase() ? "NativeColumnarGroupAgg" : super.statefulFamily();
  }

  boolean twoPhase() {
    return false;
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("same-cut-stock-reference"));
    Files.writeString(
        root.resolve(twoPhase() ? "two-phase-mini-batch" : "one-phase-mini-batch"), "5");
    Files.writeString(root.resolve("mini-batch-latency"), "1 h");
    if (twoPhase()) {
      Files.writeString(root.resolve("required-plan-node"), "stream-exec-global-group-aggregate");
      Files.writeString(
          root.resolve("native-operator"),
          "NativeColumnarLocalGroupAgg,NativeColumnarGroupAgg,RowDataToArrow,ArrowToRowData");
      Files.writeString(root.resolve("native-input-families"), "LocalGroup,NativeColumnarGroupAgg");
    }
    return root;
  }
}
