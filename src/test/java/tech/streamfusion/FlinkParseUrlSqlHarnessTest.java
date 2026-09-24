package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkParseUrlSqlHarnessTest {
  @Test
  void rawComponentsAndDynamicArgumentsMatchJavaUrl() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT PARSE_URL(u,p), PARSE_URL(u,p,k), PARSE_URL(u,'QUERY','q'), "
            + "PARSE_URL(u,'HOST'), PARSE_URL(u,'PATH'), PARSE_URL(u,'AUTHORITY') FROM src");
  }

  @Test
  void consumersAndEmptyBatchesRetainNullAndRawQueryValues() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT CONCAT(PARSE_URL(u,p), '!') FROM src "
            + "WHERE PARSE_URL(u,'QUERY','q') = 'a%2Bb+c'");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT PARSE_URL(u,p,k) FROM src WHERE n = 99");
  }

  private TableEnvironment environment() {
    String url = "http://user:pw@EXAMPLE.com:80/a/../b?q=a%2Bb+c&q=second#fragment";
    List<Row> rows = new ArrayList<>();
    for (String part : new String[] {"HOST", "PATH", "QUERY", "REF", "PROTOCOL", "FILE",
        "AUTHORITY", "USERINFO", "host", "UNKNOWN"}) rows.add(Row.of(url, part, "q", 0));
    rows.addAll(List.of(Row.of(url, "QUERY", "missing", 1), Row.of(url, "QUERY", null, 2),
        Row.of(url, null, "q", 3), Row.of(null, "HOST", "q", 4),
        Row.of("not a url", "HOST", "q", 5), Row.of("http://example.org", "PATH", "q", 6),
        Row.of("http://example.org/?q=&a.b=1", "QUERY", "a.b", 7)));
    return BuiltinFunctionParity.environment(ROW(FIELD("u", STRING()), FIELD("p", STRING()),
        FIELD("k", STRING()), FIELD("n", INT())), rows);
  }
}
