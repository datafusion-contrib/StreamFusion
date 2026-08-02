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
import java.util.Base64;
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
    AtomicReference<String> authorization = new AtomicReference<>("unset");
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/subjects/orders-value/versions",
        exchange -> {
          posted.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          respond(exchange, "{\"id\":7}");
        });
    server.start();

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", registryUrl()));
    assertNotNull(registry);
    assertEquals(7, registry.register("orders-value", SCHEMA));
    // The schema rides the versions POST as an escaped JSON string, the registry API's envelope.
    assertEquals(SCHEMA, new ObjectMapper().readTree(posted.get()).get("schema").asText());
    assertNull(authorization.get());
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

  /**
   * The two header-only auth schemes send the Confluent client's exact Authorization header on the
   * decode's fetch-by-id and the sink's registration alike; a dangling credential without its
   * source is ignored the way the Confluent client ignores it (see the registration test above).
   */
  @Test
  void userInfoBasicAuthRidesFetchAndRegistration() throws Exception {
    AtomicReference<String> fetchAuth = new AtomicReference<>();
    AtomicReference<String> registerAuth = new AtomicReference<>();
    server = authCapturingServer(fetchAuth, registerAuth);

    // The decode side's table-option spelling, gated and translated through fromOptions.
    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromOptions(
            Map.of(
                "format", "avro-confluent",
                "avro-confluent.url", registryUrl(),
                "avro-confluent.basic-auth.credentials-source", "USER_INFO",
                "avro-confluent.basic-auth.user-info", "user:pass"));
    assertNotNull(registry);
    assertEquals(SCHEMA, registry.fetchWriterSchema(7).toString());
    assertEquals(7, registry.register("orders-value", SCHEMA));
    // The Confluent client's header: Basic over the base64 of the raw user-info string.
    String expected =
        "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8));
    assertEquals(expected, fetchAuth.get());
    assertEquals(expected, registerAuth.get());
  }

  @Test
  void staticTokenBearerAuthRidesFetchAndRegistration() throws Exception {
    AtomicReference<String> fetchAuth = new AtomicReference<>();
    AtomicReference<String> registerAuth = new AtomicReference<>();
    server = authCapturingServer(fetchAuth, registerAuth);

    ConfluentSchemaRegistry registry =
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of(
                "url", registryUrl(),
                "bearer-auth.credentials-source", "STATIC_TOKEN",
                "bearer-auth.token", "secret-token"));
    assertNotNull(registry);
    assertEquals(SCHEMA, registry.fetchWriterSchema(7).toString());
    assertEquals(7, registry.register("orders-value", SCHEMA));
    assertEquals("Bearer secret-token", fetchAuth.get());
    assertEquals("Bearer secret-token", registerAuth.get());
  }

  /** The sink-side option gate mirrors the decode side's untranslated-option fallbacks. */
  @Test
  void declinesUntranslatedRegistryOptions() {
    assertNull(ConfluentSchemaRegistry.fromFormatOptions(Map.of()));
    assertNull(withUrl("schema", "{\"type\":\"string\"}"));
    assertNull(withUrl("schema-registry.schema", "{\"type\":\"string\"}"));
    assertNull(withUrl("ssl.truststore.location", "/certs/truststore.jks"));
    assertNull(withUrl("properties", "http.connect.timeout.ms:100"));
    // Credential sources needing more than a header from the options: URL reads the credential
    // out of the registry URL, SASL_INHERIT the Kafka JAAS login, the OAuth sources a token flow.
    assertNull(withUrl("basic-auth.credentials-source", "URL"));
    assertNull(withUrl("basic-auth.credentials-source", "SASL_INHERIT"));
    assertNull(withUrl("bearer-auth.credentials-source", "OAUTHBEARER"));
    // A translated source missing its credential falls back, so Flink raises the Confluent
    // client's own ConfigException; both sources at once is the same ConfigException.
    assertNull(withUrl("basic-auth.credentials-source", "USER_INFO"));
    assertNull(withUrl("bearer-auth.credentials-source", "STATIC_TOKEN"));
    assertNull(
        ConfluentSchemaRegistry.fromFormatOptions(
            Map.of(
                "url", "http://r:8081",
                "basic-auth.credentials-source", "USER_INFO",
                "basic-auth.user-info", "u:p",
                "bearer-auth.credentials-source", "STATIC_TOKEN",
                "bearer-auth.token", "t")));
    assertNotNull(
        ConfluentSchemaRegistry.fromFormatOptions(Map.of("schema-registry.url", "http://r:8081")));
    // A dangling credential without its source is ignored, exactly like the Confluent client.
    assertNotNull(withUrl("basic-auth.user-info", "u:p"));
    assertNotNull(withUrl("bearer-auth.token", "t"));
  }

  private static ConfluentSchemaRegistry withUrl(String key, String value) {
    return ConfluentSchemaRegistry.fromFormatOptions(Map.of("url", "http://r:8081", key, value));
  }

  private String registryUrl() {
    return "http://localhost:" + server.getAddress().getPort();
  }

  private HttpServer authCapturingServer(
      AtomicReference<String> fetchAuth, AtomicReference<String> registerAuth) throws IOException {
    HttpServer capturing = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    String quoted = SCHEMA.replace("\"", "\\\"");
    capturing.createContext(
        "/schemas/ids/7",
        exchange -> {
          fetchAuth.set(exchange.getRequestHeaders().getFirst("Authorization"));
          respond(exchange, "{\"schema\":\"" + quoted + "\"}");
        });
    capturing.createContext(
        "/subjects/orders-value/versions",
        exchange -> {
          registerAuth.set(exchange.getRequestHeaders().getFirst("Authorization"));
          respond(exchange, "{\"id\":7}");
        });
    capturing.start();
    return capturing;
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
