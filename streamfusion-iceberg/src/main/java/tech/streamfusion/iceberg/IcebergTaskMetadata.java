package tech.streamfusion.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;

/** Translates control metadata without traversing payload rows. */
final class IcebergTaskMetadata {
  private IcebergTaskMetadata() {}

  static ObjectNode translate(
      ObjectMapper mapper,
      FileScanTask task,
      Schema schema,
      List<Expression> filters,
      boolean caseSensitive,
      String nameMapping)
      throws IOException {
    var metadata = mapper.createObjectNode();
    metadata.set("partition_spec", mapper.readTree(PartitionSpecParser.toJson(task.spec())));
    metadata.set(
        "partition_values",
        mapper.readTree(
            SingleValueParser.toJson(task.spec().partitionType(), task.file().partition())));
    if (nameMapping != null) metadata.set("name_mapping", mapper.readTree(nameMapping));
    if (task.residual().op() != Expression.Operation.TRUE && filters.isEmpty()) {
      throw new IllegalArgumentException(
          "Native task residuals require the complete source filters");
    }
    var translated = predicate(mapper, schema, filters, caseSensitive);
    if (translated != null) metadata.set("predicate", translated);
    return metadata;
  }

  static JsonNode predicate(
      ObjectMapper mapper, Schema schema, List<Expression> filters, boolean caseSensitive) {
    Expression predicate = Expressions.alwaysTrue();
    for (var filter : filters) predicate = Expressions.and(predicate, filter);
    var bound = Binder.bind(schema.asStruct(), Expressions.rewriteNot(predicate), caseSensitive);
    if (bound.op() != Expression.Operation.TRUE) {
      var translated = ExpressionVisitors.visit(bound, new Visitor(mapper));
      if (translated == null)
        throw new IllegalArgumentException("Unverified native Iceberg expression");
      return translated;
    }
    return null;
  }

  private static final class Visitor extends ExpressionVisitors.ExpressionVisitor<JsonNode> {
    private final ObjectMapper mapper;

    private Visitor(ObjectMapper mapper) {
      this.mapper = mapper;
    }

    private ObjectNode operation(String op) {
      return mapper.createObjectNode().put("op", op);
    }

    private JsonNode require(JsonNode node) {
      if (node == null) throw new IllegalArgumentException("Unverified native Iceberg expression");
      return node;
    }

    @Override
    public JsonNode alwaysTrue() {
      return operation("TRUE");
    }

    @Override
    public JsonNode alwaysFalse() {
      return operation("FALSE");
    }

    @Override
    public JsonNode not(JsonNode child) {
      return operation("NOT").set("child", require(child));
    }

    @Override
    public JsonNode and(JsonNode left, JsonNode right) {
      var node = operation("AND");
      node.set("left", require(left));
      node.set("right", require(right));
      return node;
    }

    @Override
    public JsonNode or(JsonNode left, JsonNode right) {
      var node = operation("OR");
      node.set("left", require(left));
      node.set("right", require(right));
      return node;
    }

    @Override
    public <T> JsonNode predicate(BoundPredicate<T> predicate) {
      if (!(predicate.term() instanceof BoundReference)) {
        throw new IllegalArgumentException("Native predicate transforms are not admitted");
      }
      switch (predicate.term().type().typeId()) {
        case TIME:
        case BINARY:
        case FIXED:
          throw new IllegalArgumentException(
              "Native source predicates do not admit " + predicate.term().type());
        default:
      }
      var node = operation("PREDICATE");
      node.put("field_id", predicate.ref().fieldId());
      node.put("operation", predicate.op().name());
      var literals = node.putArray("literals");
      try {
        if (predicate.isLiteralPredicate()) {
          literals.add(
              mapper.readTree(
                  SingleValueParser.toJson(
                      predicate.term().type(), predicate.asLiteralPredicate().literal().value())));
        } else if (predicate.isSetPredicate()) {
          for (var literal : predicate.asSetPredicate().literalSet()) {
            literals.add(
                mapper.readTree(SingleValueParser.toJson(predicate.term().type(), literal)));
          }
        } else if (!predicate.isUnaryPredicate()) {
          throw new IllegalArgumentException("Unverified native predicate shape");
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid Iceberg predicate literal", e);
      }
      return node;
    }

    @Override
    public <T> JsonNode predicate(UnboundPredicate<T> predicate) {
      throw new IllegalArgumentException("Native predicates must first bind in Java");
    }
  }
}
