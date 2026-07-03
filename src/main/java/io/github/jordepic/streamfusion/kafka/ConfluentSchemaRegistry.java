package io.github.jordepic.streamfusion.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;

/**
 * The Confluent schema registry's read side, as the native {@code avro-confluent} decode needs it:
 * fetch a writer schema by the id each message is framed with ({@code GET /schemas/ids/<id>}), exactly
 * the lookup Flink's own deserializer makes through the registry client. Deliberately a plain HTTP
 * client rather than a dependency on the Confluent client library — the only call used is this one
 * GET, and staying dependency-free keeps the build self-contained. Registry auth/SSL options are not
 * translated, so the planner routes only tables without them (they fall back to Flink).
 *
 * <p>Serializable (the URL list travels in the operator to distributed task managers); the HTTP client
 * is created lazily on first fetch.
 */
public class ConfluentSchemaRegistry implements Serializable {

  private static final long serialVersionUID = 1L;

  /** {@code avro-confluent} options with no native translation: any of them present → fall back. */
  private static final Set<String> UNSUPPORTED_OPTIONS =
      Set.of(
          "schema",
          "properties",
          "ssl.keystore.location",
          "ssl.keystore.password",
          "ssl.truststore.location",
          "ssl.truststore.password",
          "basic-auth.credentials-source",
          "basic-auth.user-info",
          "bearer-auth.credentials-source",
          "bearer-auth.token");

  private final String[] urls;

  private transient HttpClient client;

  private ConfluentSchemaRegistry(String[] urls) {
    this.urls = urls;
  }

  /**
   * Builds the registry accessor from a table's options, or null when the format's registry options
   * are ones the native path doesn't translate (an explicit reader {@code schema}, auth, SSL, or
   * pass-through client {@code properties}) — the caller then leaves the table on Flink. The format
   * factory already validated {@code url} is present.
   */
  public static ConfluentSchemaRegistry fromOptions(Map<String, String> options) {
    // Format options are prefixed with the format identifier — plus "value." when the format was
    // declared as `value.format`.
    String prefix = (options.containsKey("value.format") ? "value." : "") + "avro-confluent.";
    String url = options.get(prefix + "url");
    if (url == null) {
      return null;
    }
    for (String option : UNSUPPORTED_OPTIONS) {
      if (options.containsKey(prefix + option)) {
        return null;
      }
    }
    // The url option accepts a comma-separated list of base URLs (the registry client's failover
    // form); fetches try each in order.
    return new ConfluentSchemaRegistry(url.split(","));
  }

  /**
   * Fetches the writer schema registered under {@code id}, trying each base URL in order. Fails like
   * Flink's deserializer does when the registry can't supply the schema — the record is undecodable
   * without it.
   */
  public Schema fetchWriterSchema(int id) throws IOException {
    if (client == null) {
      client = HttpClient.newHttpClient();
    }
    IOException failure = null;
    for (String base : urls) {
      String url = base.trim();
      url = (url.endsWith("/") ? url.substring(0, url.length() - 1) : url) + "/schemas/ids/" + id;
      try {
        HttpRequest request =
            HttpRequest.newBuilder(URI.create(url))
                .header("Accept", "application/vnd.schemaregistry.v1+json")
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
          throw new IOException(
              "schema registry returned " + response.statusCode() + " for " + url);
        }
        JsonNode body = new ObjectMapper().readTree(response.body());
        JsonNode type = body.get("schemaType");
        if (type != null && !"AVRO".equals(type.asText())) {
          throw new IOException("schema id " + id + " is not an Avro schema: " + type.asText());
        }
        return new Schema.Parser().parse(body.get("schema").asText());
      } catch (IOException e) {
        failure = e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted fetching schema id " + id, e);
      }
    }
    throw new IOException("Could not find schema with id " + id + " in registry", failure);
  }

  /**
   * Adds each reader record's full name as an alias on the corresponding writer record, walking the
   * two schemas in tandem (fields matched by name, null-unions unwrapped, arrays/maps descended).
   *
   * <p>Why: Avro Java deliberately skips the spec's record-name check during schema resolution (its
   * {@code Resolver} carries the check commented out for compatibility), so Flink decodes a topic
   * whose writer records are named {@code com.example.User} against a reader derived from the table
   * type (named {@code org.apache.flink.avro.generated.record}) without complaint. arrow-avro enforces
   * the check — but accepts a match through aliases, so patching the reader's names onto the writer as
   * aliases reproduces Java's leniency without touching the writer's real names (self-references in a
   * recursive schema stay valid). Mutates and returns {@code writer}, which the caller parsed fresh.
   */
  public static Schema aliasedToReader(Schema writer, Schema reader) {
    addReaderAliases(writer, reader, new HashSet<>());
    return writer;
  }

  private static void addReaderAliases(Schema writer, Schema reader, Set<String> visited) {
    Schema w = unwrapNullUnion(writer);
    Schema r = unwrapNullUnion(reader);
    if (w.getType() == Schema.Type.RECORD && r.getType() == Schema.Type.RECORD) {
      if (!visited.add(w.getFullName())) {
        return;
      }
      if (!w.getFullName().equals(r.getFullName())) {
        w.addAlias(r.getName(), r.getNamespace());
      }
      for (Schema.Field readerField : r.getFields()) {
        Schema.Field writerField = w.getField(readerField.name());
        if (writerField != null) {
          addReaderAliases(writerField.schema(), readerField.schema(), visited);
        }
      }
    } else if (w.getType() == Schema.Type.ARRAY && r.getType() == Schema.Type.ARRAY) {
      addReaderAliases(w.getElementType(), r.getElementType(), visited);
    } else if (w.getType() == Schema.Type.MAP && r.getType() == Schema.Type.MAP) {
      addReaderAliases(w.getValueType(), r.getValueType(), visited);
    }
  }

  /** The non-null branch of a nullable union, or the schema itself — field nullability wrapping. */
  private static Schema unwrapNullUnion(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema branch : schema.getTypes()) {
        if (branch.getType() != Schema.Type.NULL) {
          return branch;
        }
      }
    }
    return schema;
  }
}
