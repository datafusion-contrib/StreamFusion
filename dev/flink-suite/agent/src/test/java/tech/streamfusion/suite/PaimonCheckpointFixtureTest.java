package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.junit.jupiter.api.Test;

class PaimonCheckpointFixtureTest {
  @Test
  void linksTheMovedFieldsToEquivalentReleasedSettings() throws Exception {
    ClassWriter bytecode = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    String type = "tech/streamfusion/suite/LegacyRecoveryProbe";
    bytecode.visit(Opcodes.V17, Opcodes.ACC_PUBLIC, type, null, "java/lang/Object", null);
    fieldMethod(
        bytecode,
        "retentionOption",
        "CheckpointingOptions",
        "EXTERNALIZED_CHECKPOINT_RETENTION",
        "ConfigOption");
    fieldMethod(
        bytecode,
        "retentionValue",
        "ExternalizedCheckpointRetention",
        "RETAIN_ON_CANCELLATION",
        "ExternalizedCheckpointRetention");
    fieldMethod(
        bytecode,
        "ignoreUnclaimedState",
        "StateRecoveryOptions",
        "SAVEPOINT_IGNORE_UNCLAIMED_STATE",
        "ConfigOption");
    MethodVisitor path =
        PaimonCheckpointFixture.adapt(
            bytecode.visitMethod(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                "restorePath",
                "()Ljava/lang/String;",
                null,
                null));
    path.visitCode();
    path.visitLdcInsn("execution.state-recovery.path");
    path.visitInsn(Opcodes.ARETURN);
    path.visitMaxs(0, 0);
    path.visitEnd();
    bytecode.visitEnd();
    byte[] bytes = bytecode.toByteArray();
    Class<?> probe =
        new ClassLoader(getClass().getClassLoader()) {
          Class<?> loadProbe() {
            return defineClass(null, bytes, 0, bytes.length);
          }
        }.loadProbe();

    assertSame(
        ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
        probe.getMethod("retentionOption").invoke(null));
    assertSame(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION,
        probe.getMethod("retentionValue").invoke(null));
    assertSame(
        SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE,
        probe.getMethod("ignoreUnclaimedState").invoke(null));
    assertEquals(
        SavepointConfigOptions.SAVEPOINT_PATH.key(), probe.getMethod("restorePath").invoke(null));
    Configuration configuration = new Configuration();
    configuration.set(
        ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    configuration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
    assertEquals(
        "RETAIN_ON_CANCELLATION",
        configuration.toMap().get("execution.checkpointing.externalized-checkpoint-retention"));
    assertEquals("true", configuration.toMap().get("execution.savepoint.ignore-unclaimed-state"));
  }

  @Test
  void appliesTheRestorePathAndIgnorePolicyToTheLegacyStreamGraph() {
    Configuration configuration = new Configuration();
    configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, "file:///checkpoint/chk-1");
    configuration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
    var graph =
        new org.apache.flink.streaming.api.graph.StreamGraph(
            new org.apache.flink.api.common.ExecutionConfig(),
            new org.apache.flink.streaming.api.environment.CheckpointConfig(),
            org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.none());
    PaimonCheckpointFixture.applyRestoreSettings(configuration, graph);
    assertEquals("file:///checkpoint/chk-1", graph.getSavepointRestoreSettings().getRestorePath());
    assertEquals(true, graph.getSavepointRestoreSettings().allowNonRestoredState());
    PaimonCheckpointFixture.applyRestoreSettings(new Configuration(), graph);
    assertSame(
        org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.none(),
        graph.getSavepointRestoreSettings());
  }

  private static void fieldMethod(
      ClassWriter bytecode, String method, String owner, String field, String fieldType) {
    MethodVisitor visitor =
        PaimonCheckpointFixture.adapt(
            bytecode.visitMethod(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                method,
                "()Ljava/lang/Object;",
                null,
                null));
    visitor.visitCode();
    visitor.visitFieldInsn(
        Opcodes.GETSTATIC,
        "org/apache/flink/configuration/" + owner,
        field,
        "Lorg/apache/flink/configuration/" + fieldType + ";");
    visitor.visitInsn(Opcodes.ARETURN);
    visitor.visitMaxs(0, 0);
    visitor.visitEnd();
  }
}
