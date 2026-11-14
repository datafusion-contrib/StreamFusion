package tech.streamfusion;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalUpdatingRankBoundJobTest extends CanonicalScalarRankJobTest {
  static Stream<Arguments> cases() {
    return CanonicalTestMatrix.select(
        Stream.of("DOUBLE", "DECIMAL(38,4)")
            .flatMap(
                type ->
                    CanonicalGroupArithmeticJobTest.cases()
                        .map(
                            args ->
                                Arguments.of(
                                    "update",
                                    type,
                                    args.get()[0],
                                    args.get()[1],
                                    args.get()[2],
                                    args.get()[3]))));
  }

  @Override
  String updatingInput() {
    return "(SELECT k,v,id,p,COUNT(*) AS c,COALESCE(MOD(k,3)+1,0) AS b"
        + " FROM input_table GROUP BY k,v,id,p)";
  }

  @Override
  String rankPredicate() {
    return "rn<=b";
  }

  @Override
  int rankEnd(String key) {
    return key.equals("null") ? 0 : (int) Math.floorMod(Long.parseLong(key), 3) + 1;
  }
}
