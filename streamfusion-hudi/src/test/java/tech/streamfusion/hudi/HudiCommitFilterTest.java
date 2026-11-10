package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hudi.common.table.log.InstantRange;
import org.junit.jupiter.api.Test;
import tech.streamfusion.Native;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.planner.HudiCommitFilter;

class HudiCommitFilterTest {
  @Test
  void existingRustExpressionsMatchJavaRangesAndProjectPayloads() {
    for (InstantRange range :
        List.of(
            new InstantRange.ExactMatchRange(Set.of("001", "005")),
            new InstantRange.ExactMatchRange(Set.of("003")),
            new InstantRange.ExactMatchRange(Set.of("099")))) {
      try (var filter = new HudiCommitFilter(range, List.of("name", "id"))) {
        for (int trial = 0; trial < 2; trial++) {
          var name = new VarCharVector("name", NativeAllocator.SHARED);
          var id = new BigIntVector("id", NativeAllocator.SHARED);
          var commit = new VarCharVector("_hoodie_commit_time", NativeAllocator.SHARED);
          var input = new VectorSchemaRoot(List.of(name, id, commit));
          var expected = new ArrayList<Long>();
          try {
            input.allocateNew();
            for (int i = 0; i < 6; i++) {
              String instant = "00" + i;
              id.setSafe(i, i);
              if (i % 2 == 0) name.setNull(i);
              else
                name.setSafe(i, ("value-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
              commit.setSafe(i, instant.getBytes(java.nio.charset.StandardCharsets.UTF_8));
              if (range.isInRange(instant)) expected.add((long) i);
            }
            input.setRowCount(6);
          } catch (Throwable failure) {
            input.close();
            throw failure;
          }
          try (var result = filter.apply(input)) {
            assertEquals(
                List.of("name", "id"),
                result.getSchema().getFields().stream()
                    .map(org.apache.arrow.vector.types.pojo.Field::getName)
                    .collect(java.util.stream.Collectors.toList()));
            assertEquals(expected.size(), result.getRowCount());
            for (int i = 0; i < result.getRowCount(); i++) {
              long value = ((BigIntVector) result.getVector("id")).get(i);
              assertEquals(expected.get(i), value);
              assertEquals(
                  value % 2 == 0 ? null : "value-" + value,
                  result.getVector("name").isNull(i)
                      ? null
                      : result.getVector("name").getObject(i).toString());
            }
          }
        }
      }
    }
    assertTrue(Native.liveNativeHandles().isBlank());
  }
}
