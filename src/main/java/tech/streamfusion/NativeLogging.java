package tech.streamfusion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives the native libraries' log stream. Every StreamFusion native library installs a Rust
 * {@code log}-facade bridge in its {@code JNI_OnLoad} that upcalls {@link #log} from whatever
 * thread produced the event (librdkafka's own broker threads included, attached as daemons), so
 * native and librdkafka logging follows the deployment's SLF4J configuration instead of being
 * dropped. Level values follow the Rust {@code log} crate; the logger name is the Rust log target
 * with {@code ::} rewritten to {@code .} (librdkafka's stream arrives under {@code librdkafka}).
 */
public final class NativeLogging {

  private NativeLogging() {}

  /** Called from native code; 1=error, 2=warn, 3=info, 4=debug, anything else trace. */
  static void log(int level, String logger, String message) {
    Logger target = LoggerFactory.getLogger(logger);
    switch (level) {
      case 1 -> target.error(message);
      case 2 -> target.warn(message);
      case 3 -> target.info(message);
      case 4 -> target.debug(message);
      default -> target.trace(message);
    }
  }
}
