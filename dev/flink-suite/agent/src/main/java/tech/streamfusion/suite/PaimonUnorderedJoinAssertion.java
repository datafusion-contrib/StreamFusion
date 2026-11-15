package tech.streamfusion.suite;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

/** Compares complete result bags for the released partition lookup fixture's unordered joins. */
final class PaimonUnorderedJoinAssertion {
  static final String QUERY =
      "SELECT Q.id, P.v FROM Q INNER JOIN P /*+ OPTIONS('scan.partitions' ="
          + " 'pt1=max_pt(),pt2=max_pt()') */ ";
  static final String ASSERTION =
      "([Ljava/lang/Object;)Lorg/assertj/core/api/AbstractIterableAssert;";

  private PaimonUnorderedJoinAssertion() {}

  static AsmVisitorWrapper adapter() {
    return new AsmVisitorWrapper.ForDeclaredMethods()
        .method(
            named("testScanWithSpecifiedPartitionsWithLevelMaxPt").and(takesArguments(0)),
            (type, method, visitor, context, pool, writerFlags, readerFlags) -> adapt(visitor));
  }

  static MethodVisitor adapt(MethodVisitor visitor) {
    return new MethodVisitor(Opcodes.ASM9, visitor) {
      int queries;
      int assertions;

      @Override
      public void visitLdcInsn(Object value) {
        if ((QUERY + "ON Q.id = P.id").equals(value)
            || (QUERY + "FOR SYSTEM_TIME AS OF Q.proctime ON Q.id = P.id").equals(value)) {
          queries++;
        }
        super.visitLdcInsn(value);
      }

      @Override
      public void visitMethodInsn(
          int opcode, String owner, String name, String descriptor, boolean itf) {
        if (opcode == Opcodes.INVOKEVIRTUAL
            && owner.equals("org/assertj/core/api/ListAssert")
            && name.equals("containsExactly")
            && descriptor.equals(ASSERTION)) {
          name = "containsExactlyInAnyOrder";
          assertions++;
        }
        super.visitMethodInsn(opcode, owner, name, descriptor, itf);
      }

      @Override
      public void visitEnd() {
        if (queries != 2 || assertions != 1) {
          throw new IllegalStateException("Unknown released Paimon unordered join assertion");
        }
        super.visitEnd();
      }
    };
  }
}
