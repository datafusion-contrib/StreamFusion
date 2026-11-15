package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import org.junit.jupiter.api.Test;

class PaimonUnorderedJoinAssertionTest {
  @Test
  void changesOnlyTheKnownAssertionWithBothOriginalQueries() {
    List<String> calls = new ArrayList<>();
    var visitor =
        PaimonUnorderedJoinAssertion.adapt(
            new MethodVisitor(Opcodes.ASM9) {
              @Override
              public void visitMethodInsn(
                  int opcode, String owner, String name, String descriptor, boolean itf) {
                calls.add(owner + "." + name + descriptor);
              }
            });
    queries(visitor);
    assertion(visitor);
    visitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "Other", "containsExactly", "()V", false);
    visitor.visitEnd();
    assertEquals(
        List.of(
            "org/assertj/core/api/ListAssert.containsExactlyInAnyOrder"
                + PaimonUnorderedJoinAssertion.ASSERTION,
            "Other.containsExactly()V"),
        calls);
  }

  @Test
  void rejectsChangedQueriesIncludingAnAddedOrderBy() {
    var visitor = PaimonUnorderedJoinAssertion.adapt(new MethodVisitor(Opcodes.ASM9) {});
    visitor.visitLdcInsn(PaimonUnorderedJoinAssertion.QUERY + "ON Q.id = P.id ORDER BY Q.id");
    assertion(visitor);
    assertThrows(IllegalStateException.class, visitor::visitEnd);
  }

  @Test
  void rejectsMissingOrAdditionalAssertions() {
    for (int count : new int[] {0, 2}) {
      var visitor = PaimonUnorderedJoinAssertion.adapt(new MethodVisitor(Opcodes.ASM9) {});
      queries(visitor);
      for (int i = 0; i < count; i++) assertion(visitor);
      assertThrows(IllegalStateException.class, visitor::visitEnd);
    }
  }

  private static void queries(MethodVisitor visitor) {
    visitor.visitLdcInsn(PaimonUnorderedJoinAssertion.QUERY + "ON Q.id = P.id");
    visitor.visitLdcInsn(
        PaimonUnorderedJoinAssertion.QUERY + "FOR SYSTEM_TIME AS OF Q.proctime ON Q.id = P.id");
  }

  private static void assertion(MethodVisitor visitor) {
    visitor.visitMethodInsn(
        Opcodes.INVOKEVIRTUAL,
        "org/assertj/core/api/ListAssert",
        "containsExactly",
        PaimonUnorderedJoinAssertion.ASSERTION,
        false);
  }
}
