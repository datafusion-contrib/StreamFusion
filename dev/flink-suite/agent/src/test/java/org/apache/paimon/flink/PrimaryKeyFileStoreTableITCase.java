package org.apache.paimon.flink;

import java.util.*;
import org.apache.flink.types.Row;

public class PrimaryKeyFileStoreTableITCase {
  public List<Row> rows;
  public int consumed;
  public boolean fail;

  public static class ResultChecker {
    public void addChangelog(Row row) {}

    public static void access$100(ResultChecker checker, Row row) {
      checker.addChangelog(row);
    }
  }

  private void checkChangelogTestResult(int producers) {
    ResultChecker checker = new ResultChecker();
    int unused = 0;
    int endCnt = 0;
    Iterator<Row> it = rows.iterator();
    while (it.hasNext()) {
      Row row = it.next();
      ResultChecker.access$100(checker, row);
      consumed = consumed + 1;
      if (fail) throw new IllegalStateException("fixture failure");
      if ((Long) row.getField(2) >= 10000) {
        endCnt++;
        if (endCnt == producers * 4 * 64) break;
      }
    }
  }
}
