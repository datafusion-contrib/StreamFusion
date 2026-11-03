package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGroupArithmeticAppendJobTest extends CanonicalGroupArithmeticJobTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupArithmeticJobTest.cases()
        .filter(
            test ->
                !Boolean.getBoolean("streamfusion.test.appendGroupJobSmoke")
                    || test.get()[0].equals("continue")
                        && !(boolean) test.get()[1]
                        && (boolean) test.get()[2]
                        && test.get()[3].equals("hashmap"));
  }

  @Override
  Path prepare(String name, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, mode, ordinary);
    Files.createFile(root.resolve("input_table-insert-only"));
    if (!mode.equals("rescale"))
      Files.write(
          root.resolve("input.csv"),
          List.of(
              "INSERT,1,null",
              "INSERT,1,7",
              "INSERT,1,4",
              "INSERT,4,null",
              "INSERT,2,10",
              "INSERT,null,20",
              "INSERT,1,-11",
              "INSERT,2,null",
              "INSERT,1,-5",
              "INSERT,1,2",
              "INSERT,2,-10",
              "INSERT,null,-20",
              "INSERT,null,null",
              "INSERT,3,null",
              "INSERT,3,6",
              "INSERT,3,5",
              "INSERT,1,null",
              "INSERT,1,3",
              "INSERT,2,null",
              "INSERT,4,null"));
    return root;
  }

  @Override
  Map<String, String> expectedTable(String mode) {
    return mode.equals("rescale")
        ? super.expectedTable(mode)
        : Map.of("null", "0|0", "1", "0|0", "2", "0|0", "3", "11|5", "4", "null|null");
  }
}
