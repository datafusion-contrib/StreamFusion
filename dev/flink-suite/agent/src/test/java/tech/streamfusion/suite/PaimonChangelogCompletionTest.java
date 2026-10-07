package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.*;

import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Opcodes;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class PaimonChangelogCompletionTest {
  @Test
  void duplicateTerminalPairsDoNotCompleteMissingKeys() {
    PaimonChangelogCompletion.begin();
    try {
      for (int key = 0; key < 488; key++) track(RowKind.INSERT, key, 10000);
      for (int key = 0; key < 12; key++) {
        track(RowKind.UPDATE_BEFORE, key, 10000);
        track(RowKind.UPDATE_AFTER, key, 10000);
      }
      assertEquals(488, PaimonChangelogCompletion.count());
      for (int key = 488; key < 512; key++) track(RowKind.INSERT, key, 10000);
      assertEquals(512, PaimonChangelogCompletion.count());
      track(RowKind.DELETE, 0, 10000);
      assertEquals(511, PaimonChangelogCompletion.count());
      track(RowKind.INSERT, 0, 5);
      assertEquals(511, PaimonChangelogCompletion.count());
      track(RowKind.UPDATE_BEFORE, 0, 5);
      track(RowKind.UPDATE_AFTER, 0, 10000);
      assertEquals(512, PaimonChangelogCompletion.count());
    } finally {
      PaimonChangelogCompletion.clear();
    }
    assertThrows(IllegalStateException.class, PaimonChangelogCompletion::count);
    PaimonChangelogCompletion.begin();
    assertEquals(0, PaimonChangelogCompletion.count());
    PaimonChangelogCompletion.clear();
  }

  @Test
  void unknownReleasedLoopFailsClosed() {
    ClassWriter writer = new ClassWriter(0);
    var visitor =
        PaimonChangelogCompletion.adapt(
            writer.visitMethod(Opcodes.ACC_PUBLIC, "unknown", "()V", null, null));
    assertThrows(IllegalStateException.class, visitor::visitEnd);
  }

  @Test
  void scopeAdviceClearsExceptionalAndRepeatedInvocations() {
    PaimonChangelogCompletion.Scope.enter();
    track(RowKind.INSERT, 1, 10000);
    assertEquals(1, PaimonChangelogCompletion.count());
    PaimonChangelogCompletion.Scope.exit();
    assertThrows(IllegalStateException.class, PaimonChangelogCompletion::count);
    PaimonChangelogCompletion.Scope.enter();
    try {
      assertEquals(0, PaimonChangelogCompletion.count());
    } finally {
      PaimonChangelogCompletion.Scope.exit();
    }
  }

  @Test
  void adapterPreservesCheckerCallAndReplacesOnlyCounterIncrement() {
    var calls = new java.util.ArrayList<String>();
    var sink =
        new net.bytebuddy.jar.asm.MethodVisitor(Opcodes.ASM9) {
          @Override
          public void visitMethodInsn(
              int opcode, String owner, String name, String descriptor, boolean itf) {
            calls.add(name + descriptor);
          }

          @Override
          public void visitVarInsn(int opcode, int variable) {
            calls.add(opcode + ":" + variable);
          }

          @Override
          public void visitIincInsn(int variable, int increment) {
            fail("Counter must be replaced");
          }
        };
    var visitor = PaimonChangelogCompletion.adapt(sink);
    visitor.visitMethodInsn(
        Opcodes.INVOKEVIRTUAL,
        "org/apache/paimon/flink/PrimaryKeyFileStoreTableITCase$ResultChecker",
        "addChangelog",
        "(Lorg/apache/flink/types/Row;)V",
        false);
    visitor.visitIincInsn(4, 1);
    visitor.visitMethodInsn(
        Opcodes.INVOKEVIRTUAL,
        "org/apache/paimon/flink/PrimaryKeyFileStoreTableITCase$ResultChecker",
        "assertResult",
        "(I)V",
        false);
    visitor.visitEnd();
    assertEquals(
        java.util.List.of(
            "track(Ljava/lang/Object;)V",
            "addChangelog(Lorg/apache/flink/types/Row;)V",
            "count()I",
            Opcodes.ISTORE + ":4",
            "assertResult(I)V"),
        calls);
  }

  @Test
  void releasedJavaEightSyntheticCheckerAccessIsPreserved() {
    var calls = new java.util.ArrayList<String>();
    var visitor =
        PaimonChangelogCompletion.adapt(
            new net.bytebuddy.jar.asm.MethodVisitor(Opcodes.ASM9) {
              @Override
              public void visitMethodInsn(
                  int opcode, String owner, String name, String descriptor, boolean itf) {
                calls.add(name);
              }
            });
    visitor.visitMethodInsn(
        Opcodes.INVOKESTATIC,
        "org/apache/paimon/flink/PrimaryKeyFileStoreTableITCase$ResultChecker",
        "access$100",
        "(Lorg/apache/paimon/flink/PrimaryKeyFileStoreTableITCase$ResultChecker;Lorg/apache/flink/types/Row;)V",
        false);
    visitor.visitIincInsn(4, 1);
    visitor.visitEnd();
    assertEquals(java.util.List.of("track", "access$100", "count"), calls);
  }

  @Test
  void transformsReleasedBytecodeWhenArtifactsAreProvided() throws Exception {
    String artifacts = System.getProperty("paimon.completion.artifacts", "");
    org.junit.jupiter.api.Assumptions.assumeFalse(
        artifacts.isEmpty(), "Provide released test jars for compatibility validation");
    for (String path : artifacts.split(java.io.File.pathSeparator)) {
      try (var jar = new java.util.jar.JarFile(path)) {
        var entry = jar.getJarEntry("org/apache/paimon/flink/PrimaryKeyFileStoreTableITCase.class");
        byte[] bytes = jar.getInputStream(entry).readAllBytes();
        var writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        int[] methods = {0};
        new net.bytebuddy.jar.asm.ClassReader(bytes)
            .accept(
                new net.bytebuddy.jar.asm.ClassVisitor(Opcodes.ASM9, writer) {
                  @Override
                  public net.bytebuddy.jar.asm.MethodVisitor visitMethod(
                      int access, String name, String desc, String signature, String[] exceptions) {
                    var method = super.visitMethod(access, name, desc, signature, exceptions);
                    if (name.equals("checkChangelogTestResult") && desc.equals("(I)V")) {
                      methods[0]++;
                      return PaimonChangelogCompletion.adapt(method);
                    }
                    return method;
                  }
                },
                0);
        assertEquals(1, methods[0], path);
        assertTrue(writer.toByteArray().length > bytes.length, path);
      }
    }
  }

  @Test
  void composedAdapterAndAdviceVerifyExecuteAndClearOnExceptions() throws Exception {
    Class<?> original = org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase.class;
    Class<?> transformed =
        new net.bytebuddy.ByteBuddy()
            .redefine(original)
            .visit(
                net.bytebuddy.asm.Advice.to(PaimonChangelogCompletion.Scope.class)
                    .on(net.bytebuddy.matcher.ElementMatchers.named("checkChangelogTestResult")))
            .visit(PaimonChangelogCompletion.adapter())
            .make()
            .load(
                original.getClassLoader(),
                net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default.CHILD_FIRST)
            .getLoaded();
    var method = transformed.getDeclaredMethod("checkChangelogTestResult", int.class);
    method.setAccessible(true);
    var rows = new java.util.ArrayList<Row>();
    for (int key = 0; key < 244; key++) rows.add(Row.ofKind(RowKind.INSERT, "0", key, 10000L, "x"));
    for (int key = 0; key < 6; key++) {
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, "0", key, 10000L, "x"));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, "0", key, 10000L, "x"));
    }
    for (int key = 244; key < 256; key++)
      rows.add(Row.ofKind(RowKind.INSERT, "0", key, 10000L, "x"));
    Object fixture = transformed.getConstructor().newInstance();
    transformed.getField("rows").set(fixture, rows);
    method.invoke(fixture, 1);
    assertEquals(268, transformed.getField("consumed").getInt(fixture));
    assertThrows(IllegalStateException.class, PaimonChangelogCompletion::count);
    transformed.getField("fail").setBoolean(fixture, true);
    var failure =
        assertThrows(
            java.lang.reflect.InvocationTargetException.class, () -> method.invoke(fixture, 1));
    assertEquals("fixture failure", failure.getCause().getMessage());
    assertThrows(IllegalStateException.class, PaimonChangelogCompletion::count);
  }

  private static void track(RowKind kind, int key, long value) {
    PaimonChangelogCompletion.track(Row.ofKind(kind, "0", key, value, value + ".str"));
  }
}
