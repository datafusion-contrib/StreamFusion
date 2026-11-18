package tech.streamfusion;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

/** Surefire executes separate JUnit roots for successive classes in a reused fork. */
public final class SharedFlinkClusterSession implements LauncherSessionListener {
  private static final ThreadLocal<SharedFlinkClusterSession> CURRENT = new ThreadLocal<>();
  private SharedFlinkClusterSession previous;
  private SharedFlinkCluster.SessionCluster cluster;

  static SharedFlinkCluster.SessionCluster currentCluster() {
    var current = CURRENT.get();
    if (current == null) return null;
    if (current.cluster == null) current.cluster = new SharedFlinkCluster.SessionCluster();
    return current.cluster;
  }

  @Override
  public void launcherSessionOpened(LauncherSession session) {
    previous = CURRENT.get();
    CURRENT.set(this);
  }

  @Override
  public void launcherSessionClosed(LauncherSession session) {
    try {
      if (cluster != null) cluster.close();
    } finally {
      if (previous == null) CURRENT.remove();
      else CURRENT.set(previous);
    }
  }
}
