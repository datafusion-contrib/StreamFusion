package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
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
    return global() ? Map.of("0", "11|77") : Map.of("1", "6|42", "2", "3|21", "null", "2|14");
  }
}
