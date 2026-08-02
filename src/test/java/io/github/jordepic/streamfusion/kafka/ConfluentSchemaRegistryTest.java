package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The registry write side the sink's open-time schema registration uses. */
class ConfluentSchemaRegistryTest {

  private static final String SCHEMA =
      "{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"org.apache.flink.avro.generated\","
          + "\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

  private HttpServer server;
  private HttpServer failoverServer;
  private final CountDownLatch hangRelease = new CountDownLatch(1);

  @AfterEach
  void stop() {
    hangRelease.countDown();
    if (server != null) {
      server.stop(0);
    }
    if (failoverServer != null) {
      failoverServer.stop(0);
    }
  }

  @Test
  void registersTheSchemaUnderItsSubjectAndReturnsTheId() throws Exception {
    AtomicReference<String> posted = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/subjects/orders-value/versions",
        exchange -> {
          posted.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] body = "{\"id\":7}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()));
    assertNotNull(registry);
    assertEquals(7, registry.register("orders-value", SCHEMA));
    // The schema rides the versions POST as an escaped JSON string, the registry API's envelope.
    assertEquals(SCHEMA, new ObjectMapper().readTree(posted.get()).get("schema").asText());
  }

  /** An incompatible schema (the registry's 409) fails the job with the registry's message. */
  @Test
  void surfacesTheRegistrysRejection() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/subjects/orders-value/versions",
        exchange -> {
          byte[] body =
              "{\"error_code\":409,\"message\":\"Schema being registered is incompatible\"}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(409, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()));
    IOException failure =
        assertThrows(IOException.class, () -> registry.register("orders-value", SCHEMA));
    assertTrue(failure.getCause().getMessage().contains("incompatible"), failure::toString);
  }

  /** A 200 whose body lacks the envelope's field is a malformed registry, not an NPE. */
  @Test
  void aResponseWithoutTheSchemaFieldIsAnIOException() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/schemas/ids/7", exchange -> respond(exchange, "{\"subject\":\"s\"}"));
    server.createContext(
        "/subjects/orders-value/versions", exchange -> respond(exchange, "{\"version\":1}"));
    server.start();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()));
    IOException fetch = assertThrows(IOException.class, () -> registry.fetchWriterSchema(7));
    assertTrue(fetch.getCause().getMessage().contains("no \"schema\" field"), fetch::toString);
    IOException register =
        assertThrows(IOException.class, () -> registry.register("orders-value", SCHEMA));
    assertTrue(register.getCause().getMessage().contains("no \"id\" field"), register::toString);
  }

  /** A registry that accepts the connection but never answers fails within the request timeout. */
  @Test
  @Timeout(20)
  void aHungRegistryFailsTheCallWithinTheTimeout() throws Exception {
    server = hungServer();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()))
            .withTimeoutMillis(250);
    assertThrows(IOException.class, () -> registry.fetchWriterSchema(7));
    assertThrows(IOException.class, () -> registry.register("orders-value", SCHEMA));
  }

  /** A timeout is an IOException like any refused connection: the next URL still gets tried. */
  @Test
  @Timeout(20)
  void aHungRegistryFailsOverToTheNextUrl() throws Exception {
    server = hungServer();
    failoverServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    String quoted = SCHEMA.replace("\"", "\\\"");
    failoverServer.createContext(
        "/schemas/ids/7", exchange -> respond(exchange, "{\"schema\":\"" + quoted + "\"}"));
    failoverServer.start();

    String urls =
        registryUrl() + ",http://localhost:" + failoverServer.getAddress().getPort();
    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", urls)).withTimeoutMillis(250);
    assertEquals(SCHEMA, registry.fetchWriterSchema(7).toString());
  }

  /** The sink-side option gate mirrors the decode side's untranslated-option fallbacks. */
  @Test
  void declinesUntranslatedRegistryOptions() {
    assertNull(ConfluentSchemaRegistry.fromFormatOptions(Map.of()));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of("url", "http://r:8081", "schema", "{\"type\":\"string\"}")));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of("url", "http://r:8081", "schema-registry.schema", "{\"type\":\"string\"}")));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of("url", "http://r:8081", "bearer-auth.token", "t")));
    assertNotNull(
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("schema-registry.url", "http://r:8081")));
  }

  private String registryUrl() {
    return "http://localhost:" + server.getAddress().getPort();
  }

  private HttpServer hungServer() throws IOException {
    HttpServer hung = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    hung.createContext(
        "/",
        exchange -> {
          try {
            hangRelease.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          exchange.close();
        });
    hung.start();
    return hung;
  }

  private static void respond(HttpExchange exchange, String json) throws IOException {
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }
}
