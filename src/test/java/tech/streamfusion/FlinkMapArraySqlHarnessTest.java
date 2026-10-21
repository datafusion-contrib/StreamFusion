package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkMapArraySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings={"BOOLEAN","TINYINT","SMALLINT","INT","BIGINT","DECIMAL"})
  void mapFromArraysKeepsNullDuplicateKeysAndEntryOrder(String type) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP_FROM_ARRAYS(a,a)","MAP_FROM_ARRAYS");
    FlinkArrayEditSqlHarnessTest.verify(type,"MAP_FROM_ARRAYS(a,ARRAY_REVERSE(a))","MAP_FROM_ARRAYS");
  }

  @Test
  void differentLengthsKeepTheReleasedErrorAndFilteredBatchesDoNotEvaluate() {
    var runs=NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT MAP_FROM_ARRAYS(a,b) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(org.apache.flink.util.FlinkRuntimeException.class,
        runs.host().rootCause().getMessage(),runs.host().phase(),NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(()->FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT MAP_FROM_ARRAYS(a,b) FROM src WHERE id<0").assertSuccess(NativeFailureParity.Route.NATIVE);
  }
}
