package io.github.jordepic.streamfusion.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;

/**
 * The Confluent schema registry as the native registry-Avro formats need it: the decodes ({@code
 * avro-confluent} and {@code debezium-avro-confluent}) fetch a writer schema by the id each message
 * is framed with ({@code GET /schemas/ids/<id>}), and the sink encode registers its derived writer
 * schema once at open ({@code POST /subjects/<subject>/versions}) — exactly the two calls Flink's
 * own de/serializers make through the registry client. Deliberately a plain HTTP client rather than
 * a dependency on the Confluent client library — only these two calls are used, and staying
 * dependency-free keeps the build self-contained. Registry auth/SSL options are not translated, so
 * the planner routes only tables without them (they fall back to Flink).
 *
 * <p>Serializable (the URL list travels in the operator to distributed task managers); the HTTP client
 * is created lazily on first fetch.
 */
public class ConfluentSchemaRegistry implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Confluent's own client defaults: {@code http.connect.timeout.ms} and {@code
   * http.read.timeout.ms} are both 60000 in its {@code SchemaRegistryClientConfig}, applied to
   * every request. Matching them bounds how long a hung registry can block the fetch thread or
   * sink open (a timeout is an IOException, so the multi-URL failover still tries the next URL).
   */
  private static final long DEFAULT_TIMEOUT_MILLIS = 60_000;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Registry-format options with no native translation: any of them present → fall back. */
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
  private long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;

  private transient HttpClient client;

  private ConfluentSchemaRegistry(String[] urls) {
    this.urls = urls;
  }

  ConfluentSchemaRegistry withTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
    return this;
  }

  /**
   * Builds the registry accessor from a table's options, or null when the format's registry options
   * are ones the native path doesn't translate (an explicit reader {@code schema}, auth, SSL, or
   * pass-through client {@code properties}) — the caller then leaves the table on Flink. The format
   * factory already validated {@code url} is present.
   */
  public static ConfluentSchemaRegistry fromOptions(Map<String, String> options) {
    // Format options are prefixed with the format identifier — plus "value." when the format was
    // declared as `value.format`. Both registry formats share the same option set.
    String identifier = options.getOrDefault("value.format", options.get("format"));
    if (identifier == null) {
      return null;
    }
    String prefix = (options.containsKey("value.format") ? "value." : "") + identifier + ".";
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
   * Builds the registry accessor from one format instance's prefix-stripped options (the sink
   * seam's spelling — a key format has no table-level prefix to resolve), with the same
   * untranslated-option gate as {@link #fromOptions}. The registry options with fallback keys
   * ({@code schema-registry.url}, {@code schema-registry.schema}) are honored both ways.
   */
  public static ConfluentSchemaRegistry fromFormatOptions(Map<String, String> options) {
    String url = options.getOrDefault("url", options.get("schema-registry.url"));
    if (url == null || options.containsKey("schema-registry.schema")) {
      return null;
    }
    for (String option : UNSUPPORTED_OPTIONS) {
      if (options.containsKey(option)) {
        return null;
      }
    }
    return new ConfluentSchemaRegistry(url.split(","));
  }

  /**
   * Fetches the writer schema registered under {@code id}, trying each base URL in order. Fails like
   * Flink's deserializer does when the registry can't supply the schema — the record is undecodable
   * without it.
   */
  public Schema fetchWriterSchema(int id) throws IOException {
    IOException failure = null;
    for (String base : urls) {
      String url = endpoint(base, "/schemas/ids/" + id);
      try {
        HttpRequest request = request(url).GET().build();
        HttpResponse<String> response =
            client().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
          throw new IOException(
              "schema registry returned " + response.statusCode() + " for " + url);
        }
        JsonNode body = MAPPER.readTree(response.body());
        JsonNode type = body.get("schemaType");
        if (type != null && !"AVRO".equals(type.asText())) {
          throw new IOException("schema id " + id + " is not an Avro schema: " + type.asText());
        }
        JsonNode schema = body.get("schema");
        if (schema == null) {
          throw new IOException(
              "schema registry response from " + url + " has no \"schema\" field");
        }
        return new Schema.Parser().parse(schema.asText());
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
   * Registers {@code schemaJson} under {@code subject} ({@code POST /subjects/<subject>/versions})
   * and returns the id the registry assigned — the exact call Flink's serializer makes through the
   * registry client before framing each message, made once at sink open. An already-registered
   * schema gets its existing id back; an incompatible one fails the job with the registry's
   * response (Flink surfaces the same RestClientException at the first serialized record).
   */
  public int register(String subject, String schemaJson) throws IOException {
    String body = MAPPER.createObjectNode().put("schema", schemaJson).toString();
    IOException failure = null;
    for (String base : urls) {
      String url = endpoint(base, "/subjects/" + subject + "/versions");
      try {
        HttpRequest request =
            request(url)
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response =
            client().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
          throw new IOException(
              "schema registry returned "
                  + response.statusCode()
                  + " registering subject "
                  + subject
                  + ": "
                  + response.body());
        }
        JsonNode id = MAPPER.readTree(response.body()).get("id");
        if (id == null) {
          throw new IOException("schema registry response from " + url + " has no \"id\" field");
        }
        return id.asInt();
      } catch (IOException e) {
        failure = e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted registering subject " + subject, e);
      }
    }
    throw new IOException("Could not register schema under subject " + subject, failure);
  }

  private HttpClient client() {
    if (client == null) {
      client =
          HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMillis)).build();
    }
    return client;
  }

  private HttpRequest.Builder request(String url) {
    return HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMillis))
        .header("Accept", "application/vnd.schemaregistry.v1+json");
  }

  private static String endpoint(String base, String path) {
    String url = base.trim();
    return (url.endsWith("/") ? url.substring(0, url.length() - 1) : url) + path;
  }

  /**
   * Rebuilds the writer schema onto the reader's record names, walking the two schemas in tandem
   * (fields matched by name, null-unions unwrapped, arrays/maps descended): every writer record
   * standing where the reader has one is recreated under the reader's full name, keeping the
   * writer's exact field layout (the wire contract) with writer-only subtrees carried verbatim.
   *
   * <p>Why: Avro Java deliberately skips the spec's record-name check during schema resolution (its
   * {@code Resolver} carries the check commented out for compatibility), so Flink decodes a topic
   * whose writer records are named {@code com.example.User} against a reader derived from the table
   * type (named {@code org.apache.flink.avro.generated.record}) without complaint. arrow-avro
   * enforces the check, and it also short-circuits a writer named-type <em>reference</em> to a
   * positional read of the reader's shape — Debezium's envelope references its one {@code Value}
   * record for both {@code before} and {@code after}, which would misread any payload whose layout
   * differs from the reader's. Renaming solves both at once: names match without alias quirks
   * (arrow-avro compares raw namespace attributes, so a nested alias never matches an
   * inherited-namespace reader record), and a record referenced from several reader-aligned
   * positions becomes one copy per position (reader names are position-derived, so the copies get
   * distinct names and each serializes inline for full field-by-field resolution). A writer record
   * revisited under the same reader name reuses its copy, so self-references stay valid.
   */
  public static Schema alignedToReader(Schema writer, Schema reader) {
    return align(writer, reader, new HashMap<>());
  }

  private static Schema align(Schema writer, Schema reader, Map<String, Schema> copies) {
    Schema r = unwrapNullUnion(reader);
    if (writer.getType() == Schema.Type.UNION) {
      List<Schema> branches = new ArrayList<>(writer.getTypes().size());
      boolean aligned = false;
      for (Schema branch : writer.getTypes()) {
        // Align the first non-null branch (the nullability wrapper's payload); a rare multi-branch
        // union keeps its remaining branches verbatim, matching the old per-branch leniency.
        if (branch.getType() != Schema.Type.NULL && !aligned) {
          aligned = true;
          branches.add(align(branch, r, copies));
        } else {
          branches.add(branch);
        }
      }
      return Schema.createUnion(branches);
    }
    switch (writer.getType()) {
      case RECORD:
        if (r.getType() != Schema.Type.RECORD) {
          return writer; // a shape mismatch fails resolution either way, exactly like Flink
        }
        return alignRecord(writer, r, copies);
      case ARRAY:
        return r.getType() == Schema.Type.ARRAY
            ? Schema.createArray(align(writer.getElementType(), r.getElementType(), copies))
            : writer;
      case MAP:
        return r.getType() == Schema.Type.MAP
            ? Schema.createMap(align(writer.getValueType(), r.getValueType(), copies))
            : writer;
      default:
        return writer;
    }
  }

  private static Schema alignRecord(Schema writer, Schema reader, Map<String, Schema> copies) {
    String key = writer.getFullName() + "->" + reader.getFullName();
    Schema existing = copies.get(key);
    if (existing != null) {
      return existing;
    }
    Schema copy =
        Schema.createRecord(
            reader.getName(), writer.getDoc(), reader.getNamespace(), writer.isError());
    copies.put(key, copy);
    List<Schema.Field> fields = new ArrayList<>(writer.getFields().size());
    for (Schema.Field writerField : writer.getFields()) {
      Schema.Field readerField = reader.getField(writerField.name());
      Schema fieldSchema =
          readerField == null
              ? writerField.schema() // writer-only: resolution skips it, layout kept verbatim
              : align(writerField.schema(), readerField.schema(), copies);
      fields.add(
          new Schema.Field(
              writerField.name(), fieldSchema, writerField.doc(), writerField.defaultVal()));
    }
    copy.setFields(fields);
    return copy;
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
