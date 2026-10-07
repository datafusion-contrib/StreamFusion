package tech.streamfusion.fluss;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import org.apache.fluss.config.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/** Released broker cluster with separate internal and host-accessible listeners. */
public final class FlussTestCluster implements AutoCloseable {
  private final Network network = Network.newNetwork();
  private final GenericContainer<?> zookeeper;
  private final Broker coordinator;
  private final Broker tablet;

  public FlussTestCluster() throws IOException {
    zookeeper =
        new GenericContainer<>("zookeeper:3.9.2")
            .withNetwork(network)
            .withNetworkAliases("zk")
            .withExposedPorts(2181)
            .waitingFor(Wait.forListeningPort());
    coordinator = new Broker("coordinator", "coordinatorServer", reservePort());
    tablet = new Broker("tablet", "tabletServer", reservePort());
  }

  public void start() {
    zookeeper.start();
    coordinator.start();
    tablet.start();
    Wait.forLogMessage(".*New tablet server callback for tablet server.*\\n", 1)
        .withStartupTimeout(Duration.ofMinutes(2))
        .waitUntilReady(coordinator);
  }

  String logs() {
    return coordinator.getLogs() + "\n" + tablet.getLogs();
  }

  public Configuration config() {
    Configuration config = new Configuration();
    config.setString("bootstrap.servers", coordinator.getHost() + ":" + coordinator.externalPort);
    return config;
  }

  void pauseTablet() {
    tablet.getDockerClient().pauseContainerCmd(tablet.getContainerId()).exec();
  }

  void resumeTablet() {
    tablet.getDockerClient().unpauseContainerCmd(tablet.getContainerId()).exec();
  }

  private final class Broker extends GenericContainer<Broker> {
    private final int externalPort;

    Broker(String alias, String role, int externalPort) {
      super("apache/fluss:1.0.0");
      this.externalPort = externalPort;
      withNetwork(network).withNetworkAliases(alias);
      addFixedExposedPort(externalPort, 9124);
      withCommand(role);
      withEnv(
          "FLUSS_PROPERTIES",
          "zookeeper.address: zk:2181\n"
              + "bind.listeners: INTERNAL://0.0.0.0:9123, FLUSS://0.0.0.0:9124\n"
              + "advertised.listeners: INTERNAL://"
              + alias
              + ":9123, FLUSS://"
              + getHost()
              + ":"
              + externalPort
              + "\n"
              + "internal.listener.name: INTERNAL\n"
              + "remote.data.dir: file:///tmp/remote-data\n"
              + "data.dir: /tmp/fluss/data\n"
              + "default.replication.factor: 1\n");
      withEnv("FLUSS_ENV_JAVA_OPTS", "-Xms128m -Xmx1g -XX:MaxDirectMemorySize=512m");
      waitingFor(Wait.forLogMessage(".*Successfully start Netty server.*\\n", 1))
          .withStartupTimeout(Duration.ofMinutes(2));
    }
  }

  private static int reservePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @Override
  public void close() {
    tablet.close();
    coordinator.close();
    zookeeper.close();
    network.close();
  }
}
