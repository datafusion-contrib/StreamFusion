package tech.streamfusion.suite;

import static net.bytebuddy.matcher.ElementMatchers.any;

import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

/** Uses Flink 1.18's equivalent checkpoint settings without replacing recovery test bodies. */
public final class PaimonCheckpointFixture {
  private static final String CONFIGURATION = "org/apache/flink/configuration/";
  private static final String ENVIRONMENT = "org/apache/flink/streaming/api/environment/";

  private PaimonCheckpointFixture() {}

  static boolean supports(String name) {
    return name.equals("org.apache.paimon.flink.FlinkJobRecoveryITCase")
        || name.equals("org.apache.paimon.flink.RescaleBucketITCase");
  }

  public static boolean isRecoveryTest() {
    return StackWalker.getInstance()
        .walk(frames -> frames.anyMatch(frame -> supports(frame.getClassName())));
  }

  public static void applyRestoreSettings(Object configuration, Object pipeline) {
    try {
      ClassLoader loader = pipeline.getClass().getClassLoader();
      Class<?> settings =
          Class.forName("org.apache.flink.runtime.jobgraph.SavepointRestoreSettings", true, loader);
      Class<?> readableConfig =
          Class.forName("org.apache.flink.configuration.ReadableConfig", true, loader);
      Object restore =
          settings.getMethod("fromConfiguration", readableConfig).invoke(null, configuration);
      pipeline
          .getClass()
          .getMethod("setSavepointRestoreSettings", settings)
          .invoke(pipeline, restore);
    } catch (ReflectiveOperationException error) {
      throw new IllegalStateException(
          "Cannot apply the Paimon recovery fixture on Flink 1.18", error);
    }
  }

  static AsmVisitorWrapper adapter() {
    return new AsmVisitorWrapper.ForDeclaredMethods()
        .method(
            any(),
            (type, method, visitor, context, pool, writerFlags, readerFlags) -> adapt(visitor));
  }

  static MethodVisitor adapt(MethodVisitor visitor) {
    return new MethodVisitor(Opcodes.ASM9, visitor) {
      @Override
      public void visitLdcInsn(Object value) {
        if ("execution.state-recovery.path".equals(value)) {
          value = "execution.savepoint.path";
        }
        super.visitLdcInsn(value);
      }

      @Override
      public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
        if (opcode == Opcodes.GETSTATIC) {
          if (owner.equals(CONFIGURATION + "CheckpointingOptions")
              && name.equals("EXTERNALIZED_CHECKPOINT_RETENTION")) {
            owner = ENVIRONMENT + "ExecutionCheckpointingOptions";
            name = "EXTERNALIZED_CHECKPOINT";
          } else if (owner.equals(CONFIGURATION + "ExternalizedCheckpointRetention")
              && name.equals("RETAIN_ON_CANCELLATION")) {
            owner = ENVIRONMENT + "CheckpointConfig$ExternalizedCheckpointCleanup";
            descriptor = "L" + owner + ";";
          } else if (owner.equals(CONFIGURATION + "StateRecoveryOptions")
              && name.equals("SAVEPOINT_IGNORE_UNCLAIMED_STATE")) {
            owner = "org/apache/flink/runtime/jobgraph/SavepointConfigOptions";
          }
        }
        super.visitFieldInsn(opcode, owner, name, descriptor);
      }
    };
  }
}
