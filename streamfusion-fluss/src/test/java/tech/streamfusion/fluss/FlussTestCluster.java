package tech.streamfusion.fluss;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.io.IOException;
import java.time.Duration;
import org.apache.fluss.config.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/** Released broker cluster with separate internal and host-accessible listeners. */
public final class FlussTestCluster implements AutoCloseable {
  private final Network network = Network.newNetwork();
  private final GenericContainer<?> zookeeper;
  private final Broker coordinator;
  private final Broker tablet;
  private final boolean sasl;

  public FlussTestCluster() throws IOException {
    this(false);
  }

  public FlussTestCluster(boolean sasl) throws IOException {
    this.sasl = sasl;
    zookeeper =
        new GenericContainer<>("zookeeper:3.9.2")
            .withNetwork(network)
            .withNetworkAliases("zk")
            .withExposedPorts(2181)
            .waitingFor(Wait.forListeningPort());
    coordinator = new Broker("coordinator", "coordinatorServer");
    tablet = new Broker("tablet", "tabletServer");
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
    config.setString(
        "bootstrap.servers", coordinator.getHost() + ":" + coordinator.getMappedPort(9124));
    if (sasl) {
      config.setString("client.security.protocol", "SASL");
      config.setString("client.security.sasl.mechanism", "PLAIN");
      config.setString("client.security.sasl.username", "fixture");
      config.setString("client.security.sasl.password", "fixture-password");
    }
    return config;
  }

  void pauseTablet() {
    tablet.getDockerClient().pauseContainerCmd(tablet.getContainerId()).exec();
  }

  void resumeTablet() {
    tablet.getDockerClient().unpauseContainerCmd(tablet.getContainerId()).exec();
  }

  private final class Broker extends GenericContainer<Broker> {
    private final String alias;
    private final String role;

    Broker(String alias, String role) {
      super("apache/fluss:1.0.0");
      this.alias = alias;
      this.role = role;
      withNetwork(network).withNetworkAliases(alias);
      withExposedPorts(9124);
      withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/sh"));
      withCommand(
          "-c",
          "while [ ! -f /tmp/fluss-start.sh ]; do sleep 0.1; done; exec /bin/sh"
              + " /tmp/fluss-start.sh");
      withEnv("FLUSS_ENV_JAVA_OPTS", "-Xms128m -Xmx1g -XX:MaxDirectMemorySize=512m");
      waitingFor(Wait.forLogMessage(".*Successfully start Netty server.*\\n", 1))
          .withStartupTimeout(Duration.ofMinutes(2));
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
      String properties =
          "zookeeper.address: zk:2181\n"
              + "bind.listeners: INTERNAL://0.0.0.0:9123, FLUSS://0.0.0.0:9124\n"
              + "advertised.listeners: INTERNAL://"
              + alias
              + ":9123, FLUSS://"
              + getHost()
              + ":"
              + getMappedPort(9124)
              + "\n"
              + "internal.listener.name: INTERNAL\n"
              + "remote.data.dir: file:///tmp/remote-data\n"
              + "data.dir: /tmp/fluss/data\n"
              + "default.replication.factor: 1\n"
              + (sasl
                  ? "security.protocol.map: FLUSS:SASL, INTERNAL:PLAINTEXT\n"
                      + "security.sasl.enabled.mechanisms: PLAIN\n"
                      + "security.sasl.plain.jaas.config:"
                      + " org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required"
                      + " user_fixture=\"fixture-password\";\n"
                  : "");
      String script =
          "export FLUSS_PROPERTIES='"
              + properties.replace("'", "'\"'\"'")
              + "'\nexec /docker-entrypoint.sh "
              + role
              + "\n";
      copyFileToContainer(Transferable.of(script), "/tmp/fluss-start.sh");
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
