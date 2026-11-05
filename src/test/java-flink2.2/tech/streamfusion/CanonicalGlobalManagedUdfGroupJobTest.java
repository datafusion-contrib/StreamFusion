package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;

class CanonicalGlobalManagedUdfGroupJobTest extends CanonicalMiniBatchManagedUdfGroupJobTest {
  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("merge-history-aggregate"));
    Files.delete(root.resolve("one-phase-mini-batch"));
    Files.writeString(root.resolve("two-phase-mini-batch"), "5");
    Files.writeString(root.resolve("required-plan-node"), "stream-exec-global-group-aggregate");
    Files.writeString(
        root.resolve("native-operator"),
        "NativeColumnarLocalJvmGroup,NativeColumnarJvmGroup,RowDataToArrow,ArrowToRowData");
    Files.writeString(
        root.resolve("native-input-families"),
        "NativeColumnarLocalJvmGroup,NativeColumnarJvmGroup");
    Files.writeString(
        root.resolve("require-checkpoint-flush-families"),
        "NativeColumnarLocalJvmGroup,NativeColumnarJvmGroup");
    return root;
  }
}
