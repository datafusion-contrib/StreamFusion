package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * Two-phase (mini-batch) non-windowed {@code GROUP BY}: the planner splits the aggregate into a
 * local pre-aggregate, a keyed shuffle, and a global merge, above a {@code MiniBatchAssigner}. The
 * native side runs a stateless per-batch local pre-aggregate (eliding the assigner) and merges the
 * partials in the same native group-aggregate operator the single-phase path uses — so the whole
 * pipeline is one columnar island. The collapsed changelog must match the host two-phase result.
 */
class FlinkTwoPhaseGroupAggregateSqlHarnessTest {

  @Test
  void keyedTwoPhaseGroupByMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-keyed-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, SUM(v) AS s, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY k");
  }

  @Test
  void globalTwoPhaseAggregateMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-global-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input), "SELECT SUM(v) AS s, COUNT(*) AS c FROM t");
  }

  @Test
  void doubleValueTwoPhaseMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-double-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input), "SELECT k, SUM(vd) AS s, MAX(vd) AS mx FROM t GROUP BY k");
  }

  @Test
  void intMinMaxCountTwoPhaseMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-int-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, MIN(vi) AS mn, MAX(vi) AS mx, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void avgTwoPhaseMatchesHost() throws Exception {
    // AVG is the one two-phase aggregate whose partial spans TWO columns (the widened sum, then the
    // non-null count); mixing it with single-partial aggregates exercises the positional offsets on
    // both halves. Integer AVG must keep Flink's truncate-toward-zero division and cast-back.
    Path input = Files.createTempDirectory("twophase-avg-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, AVG(v) AS av, SUM(v) AS s, AVG(vi) AS avi, AVG(vd) AS avd, COUNT(*) AS c"
            + " FROM t GROUP BY k");
  }

  @Test
  void avgNarrowTypesTwoPhaseMatchesHost() throws Exception {
    // AVG over SMALLINT/TINYINT/FLOAT: the local's sum partial widens (bigint for the integers,
    // double for float) and the global's emit casts back to the narrow input type. The negative
    // values keep the truncate-toward-zero division observable on the integer averages.
    Path input = Files.createTempDirectory("twophase-avg-narrow-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, AVG(vs) AS avs, AVG(vt) AS avt, AVG(vf) AS avf, COUNT(*) AS c"
            + " FROM t GROUP BY k");
  }

  @Test
  void globalAvgTwoPhaseMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-avg-global-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input), "SELECT AVG(vi) AS avi, AVG(vd) AS avd FROM t");
  }

  @Test
  void decimalSumMinMaxTwoPhaseMatchesHost() throws Exception {
    // Decimal SUM's partial widens to DECIMAL(38, s) (the i128 running sum at the input scale);
    // MIN/MAX partials keep DECIMAL(p, s) and merge through the retractable extremes multiset.
    Path input = Files.createTempDirectory("twophase-decimal-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, SUM(vdec) AS s, MIN(vdec) AS mn, MAX(vdec) AS mx, COUNT(*) AS c"
            + " FROM t GROUP BY k");
  }

  @Test
  void decimalAvgTwoPhaseMatchesHost() throws Exception {
    // Decimal AVG merges SUM's DECIMAL(38, s) partial plus the bigint count, then divides with
    // Flink's exact decimal division into findAvgAggType's DECIMAL(38, max(6, s)).
    Path input = Files.createTempDirectory("twophase-decimal-avg-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, AVG(vdec) AS av, SUM(vdec) AS s, AVG(v) AS avb FROM t GROUP BY k");
  }

  @Test
  void decimalOverflowTwoPhaseMatchesHost() throws Exception {
    // Sums near the DECIMAL(38, 0) bound: the local bundle's partial overflows to NULL and the
    // merge must latch the result NULL exactly as the host does.
    Path input = Files.createTempDirectory("twophase-decimal-overflow-in");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment writeEnv = StreamTableEnvironment.create(env);
    writeEnv.executeSql(
        "CREATE TABLE overflow_write (k BIGINT, v DECIMAL(38, 0)) WITH ('connector' ="
            + " 'filesystem', 'path' = '"
            + input.toUri()
            + "', 'format' = 'parquet')");
    writeEnv
        .executeSql(
            "INSERT INTO overflow_write VALUES"
                + " (1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))),"
                + " (1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))),"
                + " (2, CAST('5' AS DECIMAL(38, 0)))")
        .await();
    Supplier<TableEnvironment> read =
        () -> {
          StreamExecutionEnvironment readExec = StreamExecutionEnvironment.getExecutionEnvironment();
          readExec.setParallelism(1);
          StreamTableEnvironment tEnv = StreamTableEnvironment.create(readExec);
          tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
          tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
          tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
          tEnv.getConfig().set("table.exec.mini-batch.size", "100");
          tEnv.executeSql(
              "CREATE TABLE overflow_t (k BIGINT, v DECIMAL(38, 0)) WITH ('connector' ="
                  + " 'filesystem', 'path' = '"
                  + input.toUri()
                  + "', 'format' = 'parquet')");
          return tEnv;
        };
    NativeParity.assertChangelogParity(
        read, "SELECT k, SUM(v) AS s, AVG(v) AS av FROM overflow_t GROUP BY k");
  }

  @Test
  void filteredAggregatesTwoPhaseMatchesHost() throws Exception {
    // FILTER clauses (Nexmark q15-q17's shape): the planner materializes each predicate as an
    // IS TRUE boolean column in the Calc below the local, which gates every fold on it; the global
    // merge is filter-blind because the partials arrive already filtered. The filtered AVG spans
    // two partials, so its filter must gate both the sum and the count state.
    Path input = Files.createTempDirectory("twophase-filter-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, COUNT(*) AS c, COUNT(*) FILTER (WHERE v < 15) AS cf,"
            + " SUM(v) FILTER (WHERE vi > 2) AS sf, MIN(v) FILTER (WHERE vd > 1.0) AS mnf,"
            + " AVG(v) FILTER (WHERE v > 5) AS avf FROM t GROUP BY k");
  }

  @Test
  void filteredAggregatesAcrossSmallBundlesMatchesHost() throws Exception {
    // mini-batch.size 2 forces several local flushes, so a key whose bundle matched NO filter rows
    // still emits a partial (NULL sum / zero count) that the global must fold as empty.
    Path input = Files.createTempDirectory("twophase-filter-bundles-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT k, SUM(v) FILTER (WHERE vd > 3.0) AS sf, COUNT(vi) FILTER (WHERE v < 0) AS cf"
            + " FROM t GROUP BY k");
  }

  @Test
  void countDistinctTwoPhaseMatchesHost() throws Exception {
    // Without the distinct split, a distinct aggregate under mini-batch plans as Local → Global
    // with a distinct MapView partial: the native local emits its bundle's (value, count) set as a
    // trailing view column, and the global merges it into per-key distinct state.
    Path input = Files.createTempDirectory("twophase-distinct-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input),
        "SELECT k, COUNT(DISTINCT u) AS du, SUM(v) AS s, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void distinctAcrossSmallBundlesTwoPhaseMatchesHost() throws Exception {
    // mini-batch.size 2 forces several local flushes per key, so the global merges MULTIPLE view
    // batches into the same distinct state — values repeating across bundles must count once.
    Path input = Files.createTempDirectory("twophase-distinct-bundles-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT k, COUNT(DISTINCT u) AS du, COUNT(DISTINCT us) AS ds, SUM(v) AS s"
            + " FROM t GROUP BY k");
  }

  @Test
  void sharedDistinctViewTwoPhaseMatchesHost() throws Exception {
    // COUNT(DISTINCT u) and SUM(DISTINCT u) share one view column (Flink dedups distinct infos by
    // arg set); COUNT(DISTINCT us) gets its own. Exercises the view-position bookkeeping.
    Path input = Files.createTempDirectory("twophase-distinct-shared-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT k, COUNT(DISTINCT u) AS du, SUM(DISTINCT u) AS su, COUNT(DISTINCT us) AS ds"
            + " FROM t GROUP BY k");
  }

  @Test
  void stringMinMaxTwoPhaseMatchesHost() throws Exception {
    // Nexmark q16 plans MAX over a DATE_FORMAT string into the local: the string extreme rides the
    // split as its own partial type, merged byte-lexicographically on both halves (divergences/07
    // covers the comparison). Small bundles force cross-bundle extreme merges in the global.
    Path input = Files.createTempDirectory("twophase-string-extreme-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT k, MAX(us) AS mx, MIN(us) AS mn, MAX(us) FILTER (WHERE v < 15) AS mxf,"
            + " COUNT(DISTINCT u) AS du, SUM(v) AS s FROM t GROUP BY k");
  }

  @Test
  void filteredDistinctTwoPhaseMatchesHost() throws Exception {
    // Nexmark q15/q16's shape: the same distinct arg under several filters. Flink shares one
    // MapView across the instances with a per-filter bitmask value; the native local instead keeps
    // one set per (arg, filter) pair — a filtered distinct is an unfiltered distinct over the
    // filtered row subset, so the final counts are identical. mini-batch.size 2 forces the global
    // to merge each instance's view across several bundles.
    Path input = Files.createTempDirectory("twophase-distinct-filter-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT k, COUNT(DISTINCT u) AS du, COUNT(DISTINCT u) FILTER (WHERE v < 15) AS duf,"
            + " COUNT(DISTINCT us) FILTER (WHERE vi > 2) AS dsf,"
            + " SUM(DISTINCT u) FILTER (WHERE vd > 0) AS suf, COUNT(*) AS c FROM t GROUP BY k");
  }

  @Test
  void retractingInputTwoPhaseMatchesHost() throws Exception {
    // Nexmark q4's shape: an aggregate over another aggregate's changelog. The outer local folds
    // -U/+U rows by subtracting, and — with no bare COUNT(*) to reuse — Flink appends a count1
    // COUNT(*) partial ("_$count1$_") whose merged per-key sum drives liveness in the global. The
    // AVG accumulator is layout-invariant under retraction, so the walk models only the extra
    // trailing bigint.
    Path input = Files.createTempDirectory("twophase-retract-avg-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT u, AVG(mx) AS av FROM"
            + " (SELECT k, u, MAX(v) AS mx FROM t GROUP BY k, u) GROUP BY u");
  }

  @Test
  void retractingInputCountStarReuseMatchesHost() throws Exception {
    // Grouping by the inner aggregate's value makes rows MIGRATE between outer keys as the inner
    // MAX moves, so a vacated key's merged count1 reaches zero — the global must emit -D and drop
    // the state (Flink's RecordCounter path). The bare COUNT(*) is reused as the count1 (no
    // inserted partial), and the filtered COUNT exercises FILTER under retraction (the filter
    // gates the local's subtracts too).
    Path input = Files.createTempDirectory("twophase-retract-count-in");
    writeInput(input);
    NativeParity.assertChangelogParity(
        readEnvironment(input, 2),
        "SELECT mx, COUNT(*) AS c, COUNT(*) FILTER (WHERE mx > 10) AS cf, AVG(mx) AS av FROM"
            + " (SELECT k, MAX(v) AS mx FROM t GROUP BY k) GROUP BY mx");
  }

  @Test
  void retractingSumTwoPhaseFallsBackToHost() throws Exception {
    // SUM under retraction declares Flink's two-field (sum, count) retract accumulator, which the
    // positional walk does not model — the whole query stays on the host.
    Path input = Files.createTempDirectory("twophase-retract-sum-in");
    writeInput(input);
    NativeParity.assertFallback(
        readEnvironment(input, 2),
        "SELECT u, SUM(mx) AS s FROM"
            + " (SELECT k, u, MAX(v) AS mx FROM t GROUP BY k, u) GROUP BY u");
  }

  @Test
  void stateTtlEmitsUnsuppressedUpdatesAndMatchesHost() throws Exception {
    // TTL lives on the global half (the local is transient). Bundle size 2 makes key 1's MIN see a
    // later, non-improving bundle — an unchanged transition the TTL-off run suppresses and the
    // TTL-on run (1h, nothing expires in-test) must emit as an identical -U/+U pair, kind-compared
    // against the host.
    Path input = Files.createTempDirectory("twophase-ttl-in");
    writeInput(input);
    Supplier<TableEnvironment> environment =
        () -> {
          TableEnvironment tEnv = readEnvironment(input, 2).get();
          tEnv.getConfig().set("table.exec.state.ttl", "1 h");
          return tEnv;
        };
    NativeParity.assertKindedParity(environment, "SELECT k, MIN(v) AS m FROM t GROUP BY k");
  }

  @Test
  void stateTtlHintRoutesTheGlobalHalfAndMatchesHost() throws Exception {
    // A STATE_TTL hint with the job retention at 0 must switch the global merge into TTL emission,
    // mirroring Flink's hint-over-config precedence on the two-phase plan.
    Path input = Files.createTempDirectory("twophase-ttl-hint-in");
    writeInput(input);
    NativeParity.assertKindedParity(
        readEnvironment(input, 2),
        "SELECT /*+ STATE_TTL('t' = '1h') */ k, MIN(v) AS m FROM t GROUP BY k");
  }

  @Test
  void distinctSplitChainFallsBackToHost() throws Exception {
    // With table.optimizer.distinct-agg.split.enabled the plan becomes the five-node incremental
    // chain (PartialLocal → Exchange → IncrementalGroupAggregate → Exchange → FinalGlobal) over a
    // Calc computing the bucket key; the incremental node deliberately has no native path
    // (wontdos/52-distinct-split-chain.md), so the whole query stays on the host. (The default
    // no-split plan for the same query runs natively above.)
    Path input = Files.createTempDirectory("twophase-distinct-split-in");
    writeInput(input);
    Supplier<TableEnvironment> environment =
        () -> {
          TableEnvironment tEnv = readEnvironment(input).get();
          tEnv.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true");
          return tEnv;
        };
    NativeParity.assertFallback(
        environment, "SELECT k, COUNT(DISTINCT u) AS du, SUM(v) AS s FROM t GROUP BY k");
  }

  @Test
  void perOperatorFlagKeepsTwoPhaseOnHost() throws Exception {
    Path input = Files.createTempDirectory("twophase-flag-in");
    writeInput(input);
    System.setProperty("streamfusion.operator.localGroupAggregate.enabled", "false");
    try {
      NativeParity.assertFallbackReasonContains(
          readEnvironment(input),
          "SELECT k, SUM(v) AS s FROM t GROUP BY k",
          "localGroupAggregate: disabled by config");
    } finally {
      System.clearProperty("streamfusion.operator.localGroupAggregate.enabled");
    }
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, vd DOUBLE, vi INT, vs SMALLINT, vt TINYINT,"
            + " vf FLOAT, vdec DECIMAL(10, 2), u BIGINT, us VARCHAR) WITH ('connector' ="
            + " 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    // The negative rows make integer AVG's truncate-toward-zero division observable (-7 + 4 + 9
    // over key 2 divides negatively at some prefixes of the changelog). The u/us columns repeat
    // values within and across keys so distinct aggregates see real multiplicities.
    tEnv.executeSql(
            "INSERT INTO in_write VALUES"
                + " (1, 10, 1.5, 7, CAST(100 AS SMALLINT), CAST(3 AS TINYINT), CAST(1.25 AS FLOAT),"
                + " 12.34, 10, 'a'),"
                + " (1, 20, 2.5, 3, CAST(-7 AS SMALLINT), CAST(-2 AS TINYINT), CAST(2.5 AS FLOAT),"
                + " -0.07, 10, 'b'),"
                + " (2, 5, 0.5, 9, CAST(250 AS SMALLINT), CAST(9 AS TINYINT), CAST(-0.75 AS FLOAT),"
                + " 99999999.99, 30, 'x'),"
                + " (1, 30, 3.5, 1, CAST(42 AS SMALLINT), CAST(5 AS TINYINT), CAST(4.5 AS FLOAT),"
                + " 3.00, 20, 'a'),"
                + " (2, 15, 1.0, 4, CAST(-11 AS SMALLINT), CAST(-4 AS TINYINT), CAST(5.125 AS FLOAT),"
                + " -42.42, 30, 'x'),"
                + " (2, -7, -1.25, -8, CAST(-3 AS SMALLINT), CAST(-7 AS TINYINT), CAST(0.5 AS FLOAT),"
                + " 0.01, 30, 'y')")
        .await();
  }

  private static Supplier<TableEnvironment> readEnvironment(Path directory) {
    return readEnvironment(directory, 100);
  }

  private static Supplier<TableEnvironment> readEnvironment(Path directory, int miniBatchSize) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
      tEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
      tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      tEnv.getConfig().set("table.exec.mini-batch.size", String.valueOf(miniBatchSize));
      tEnv.executeSql(
          "CREATE TABLE t (k BIGINT, v BIGINT, vd DOUBLE, vi INT, vs SMALLINT, vt TINYINT,"
              + " vf FLOAT, vdec DECIMAL(10, 2), u BIGINT, us VARCHAR) WITH ('connector' ="
              + " 'filesystem', 'path' = '"
              + directory.toUri()
              + "', 'format' = 'parquet')");
      return tEnv;
    };
  }
}
