package tech.streamfusion.suite;

import static net.bytebuddy.matcher.ElementMatchers.named;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

/**
 * Counts current final positive keys in the released PrimaryKeyFileStoreTableITCase helper. Every
 * caller creates T with (pt STRING, k INT, v1 BIGINT, v2 STRING), primary key (pt, k). This adapter
 * is restricted to that class and its private checkChangelogTestResult(int) method.
 */
public final class PaimonChangelogCompletion {
  private static final ThreadLocal<Set<List<Object>>> FINAL_KEYS = new ThreadLocal<>();

  private PaimonChangelogCompletion() {}

  public static void begin() {
    FINAL_KEYS.set(new HashSet<>());
  }

  public static void clear() {
    FINAL_KEYS.remove();
  }

  public static int count() {
    Set<List<Object>> keys = FINAL_KEYS.get();
    if (keys == null) {
      throw new IllegalStateException("Paimon completion scope missing");
    }
    return keys.size();
  }

  public static void track(Object row) {
    try {
      var field = row.getClass().getMethod("getField", int.class);
      var key = java.util.Arrays.asList(field.invoke(row, 0), field.invoke(row, 1));
      String kind = ((Enum<?>) row.getClass().getMethod("getKind").invoke(row)).name();
      Set<List<Object>> keys = FINAL_KEYS.get();
      if (keys == null) {
        throw new IllegalStateException("Paimon completion scope missing");
      }
      if (kind.equals("UPDATE_BEFORE") || kind.equals("DELETE")) {
        keys.remove(key);
      } else if (kind.equals("INSERT") || kind.equals("UPDATE_AFTER")) {
        if (((Number) field.invoke(row, 2)).longValue() >= 10000L) {
          keys.add(key);
        } else {
          keys.remove(key);
        }
      } else {
        throw new IllegalStateException("Unknown Paimon row kind " + kind);
      }
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot inspect released Paimon changelog row", failure);
    }
  }

  static AsmVisitorWrapper adapter() {
    return new AsmVisitorWrapper.ForDeclaredMethods()
        .writerFlags(net.bytebuddy.jar.asm.ClassWriter.COMPUTE_MAXS)
        .method(
            named("checkChangelogTestResult"),
            (type, method, visitor, context, pool, writerFlags, readerFlags) -> adapt(visitor));
  }

  static MethodVisitor adapt(MethodVisitor visitor) {
    return new MethodVisitor(Opcodes.ASM9, visitor) {
      int tracked, increments;

      @Override
      public void visitMethodInsn(
          int opcode, String owner, String name, String descriptor, boolean itf) {
        if (owner.equals("org/apache/paimon/flink/PrimaryKeyFileStoreTableITCase$ResultChecker")
            && ((name.equals("addChangelog")
                    && descriptor.equals("(Lorg/apache/flink/types/Row;)V"))
                || (opcode == Opcodes.INVOKESTATIC
                    && name.equals("access$100")
                    && descriptor.equals(
                        "(Lorg/apache/paimon/flink/PrimaryKeyFileStoreTableITCase$ResultChecker;Lorg/apache/flink/types/Row;)V")))) {
          super.visitInsn(Opcodes.DUP);
          super.visitMethodInsn(
              Opcodes.INVOKESTATIC,
              "tech/streamfusion/suite/PaimonChangelogCompletion",
              "track",
              "(Ljava/lang/Object;)V",
              false);
          tracked++;
        }
        super.visitMethodInsn(opcode, owner, name, descriptor, itf);
      }

      @Override
      public void visitIincInsn(int variable, int increment) {
        if (variable != 4 || increment != 1) {
          throw new IllegalStateException("Unknown Paimon completion increment");
        }
        super.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            "tech/streamfusion/suite/PaimonChangelogCompletion",
            "count",
            "()I",
            false);
        super.visitVarInsn(Opcodes.ISTORE, variable);
        increments++;
      }

      @Override
      public void visitEnd() {
        if (tracked != 1 || increments != 1) {
          throw new IllegalStateException(
              "Unknown released Paimon completion bytecode: " + tracked + "/" + increments);
        }
        super.visitEnd();
      }
    };
  }

  public static final class Scope {
    @Advice.OnMethodEnter
    public static void enter() {
      begin();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void exit() {
      clear();
    }
  }
}
