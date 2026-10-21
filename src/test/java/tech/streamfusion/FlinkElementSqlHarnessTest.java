package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkElementSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void singletonsEmptyAndNullArraysMatchFlink(String type) throws Exception {
    if (!tech.streamfusion.compat.FlinkCompat.TABLE_RUNTIME_ELEMENT_EXCEPTION
        && java.util.Set.of("TINYINT","SMALLINT").contains(type)) {
      var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment(type),
          "SELECT ELEMENT(ARRAY_SLICE(a,1,1)) FROM src");
      assertNotNull(runs.host().failure());
      runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.FALLBACK);
    } else FlinkArrayEditSqlHarnessTest.verify(type,"ELEMENT(ARRAY_SLICE(a,1,1))","ELEMENT");
  }

  @Test
  void multipleElementsKeepTheExceptionTypeAndMessage() {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ELEMENT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(runs.host().rootCause().getClass(),"Array has more than one element.",
        runs.host().phase(),NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ELEMENT(a) FROM src WHERE id<0").assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @Test
  void multipleFailureSitesKeepFlinkRowEvaluationOrder() {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT 1/(id-1),ELEMENT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(runs.host().rootCause().getClass(),runs.host().phase(),NativeFailureParity.Route.NATIVE);
  }
}
