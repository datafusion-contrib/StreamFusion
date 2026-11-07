package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUnboundedOverConstantJobTest extends CanonicalUnboundedOverJobTest {
  static Stream<Arguments> continuedCases() {
    return Stream.of("rows", "range")
        .flatMap(
            kind ->
                Stream.of(false, true)
                    .flatMap(
                        ordinary ->
                            Stream.of("hashmap", "rocksdb")
                                .map(
                                    backend ->
                                        Arguments.of(kind, "continue", ordinary, true, backend))));
  }

  @Override
  Path prepare(String name, String kind, String mode, boolean ordinary) throws Exception {
    var root = super.prepare(name, kind, mode, ordinary);
    var query = Files.readString(root.resolve("query.sql"));
    Files.writeString(
        root.resolve("query.sql"),
        query.replace(
            "SUM(v) OVER w,AVG(v) OVER w", "COUNT(*) OVER w,SUM(CAST(7 AS BIGINT)) OVER w"));
    return root;
  }

  @Override
  Map<String, String> expectedTable(String kind, String mode) {
    if (!mode.equals("rescale"))
      return global() ? Map.of("0", "11|77") : Map.of("1", "6|42", "2", "3|21", "null", "2|14");
    if (global()) return Map.of("0", "384|2688");
    var expected = new HashMap<String, String>();
    for (int key = 0; key < 64; key++)
      expected.put(key == 0 ? "null" : Integer.toString(key), "6|42");
    return expected;
  }
}
