package tech.streamfusion.state;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.sequencedmultisetstate.linked.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Released stock materializer descriptors; transport is separate from their canonical file format.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class StockSinkMaterializerStateContract implements StockStateContract {
  private static final long serialVersionUID = 1L;
  private static final int CHUNK = 1024;

  public enum Strategy {
    LEGACY,
    VALUE,
    MAP,
    ADAPTIVE
  }

  private final TypeSerializer<RowData> keys;
  private final RowType keyType, input, matches, headers, candidates, indexes;
  private final Strategy strategy;
  private final long ttl;
  private final RowDataSerializer inputSerializer, matchSerializer;
  private final GeneratedRecordEqualiser keyEqualiser;
  private final GeneratedHashFunction keyHash;

  public StockSinkMaterializerStateContract(
      TypeSerializer<RowData> keys,
      RowType keyType,
      RowType input,
      int[] inputUpsertKeys,
      Strategy strategy,
      long ttl) {
    if (ttl > 0 && strategy != Strategy.LEGACY)
      throw new IllegalArgumentException("Released V2 materializers disable TTL");
    this.keys = keys.duplicate();
    this.keyType = keyType;
    this.input = input;
    this.strategy = strategy;
    this.ttl = ttl;
    int[] columns =
        inputUpsertKeys == null || inputUpsertKeys.length == 0
            ? IntStream.range(0, input.getFieldCount()).toArray()
            : inputUpsertKeys.clone();
    var matchFields = new ArrayList<RowType.RowField>();
    for (int column : columns) {
      if (column < 0 || column >= input.getFieldCount())
        throw new IllegalArgumentException("Input matching-key column missing");
      matchFields.add(input.getFields().get(column));
    }
    matches = new RowType(matchFields);
    if (strategy == Strategy.MAP || strategy == Strategy.ADAPTIVE) {
      var loader = StockSinkMaterializerStateContract.class.getClassLoader();
      keyEqualiser =
          new EqualiserCodeGenerator(matches, loader)
              .generateRecordEqualiser("CanonicalMaterializerKeyEq");
      keyHash =
          HashCodeGenerator.generateRowHash(
              new CodeGeneratorContext(new Configuration(), loader),
              matches,
              "CanonicalMaterializerKeyHash",
              IntStream.range(0, matches.getFieldCount()).toArray());
    } else {
      keyEqualiser = null;
      keyHash = null;
    }
    inputSerializer = new RowDataSerializer(input);
    matchSerializer = new RowDataSerializer(matches);
    headers =
        RowType.of(
            new LogicalType[] {
              binary(false),
              new BooleanType(false),
              new BigIntType(),
              new BigIntType(false),
              new BigIntType(false),
              new BigIntType(false),
              new BigIntType(false)
            },
            new String[] {"binary_key", "linked", "highest", "size", "written", "rows", "indexes"});
    var rowFields = new ArrayList<RowType.RowField>();
    for (int i = 0; i < input.getFieldCount(); i++)
      rowFields.add(new RowType.RowField("payload" + i, input.getTypeAt(i).copy(true)));
    rowFields.add(new RowType.RowField("binary_key", binary(false)));
    rowFields.add(new RowType.RowField("position", new BigIntType(false)));
    for (String name : List.of("previous", "next", "next_for", "sampled_at"))
      rowFields.add(new RowType.RowField(name, new BigIntType()));
    candidates = new RowType(rowFields);
    var indexFields = new ArrayList<RowType.RowField>();
    for (int i = 0; i < matches.getFieldCount(); i++)
      indexFields.add(new RowType.RowField("match" + i, matches.getTypeAt(i).copy(true)));
    indexFields.add(new RowType.RowField("binary_key", binary(false)));
    indexFields.add(new RowType.RowField("first", new BigIntType(false)));
    indexFields.add(new RowType.RowField("last", new BigIntType(false)));
    indexes = new RowType(indexFields);
  }

  private static VarBinaryType binary(boolean nullable) {
    return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
  }

  @Override
  public TypeSerializer<RowData> keySerializer() {
    return keys.duplicate();
  }

  @Override
  public Map<String, StateDescriptor.Type> stateDescriptors() {
    if (strategy == Strategy.LEGACY) return Map.of("values", StateDescriptor.Type.VALUE);
    if (strategy == Strategy.VALUE) return Map.of("list", StateDescriptor.Type.VALUE);
    var result = new HashMap<String, StateDescriptor.Type>();
    result.put("rowToSqnState", StateDescriptor.Type.MAP);
    result.put("sqnToNodeState", StateDescriptor.Type.MAP);
    result.put("highestSqnState", StateDescriptor.Type.VALUE);
    if (strategy == Strategy.ADAPTIVE) result.put("list", StateDescriptor.Type.VALUE);
    return Map.copyOf(result);
  }

  private final class States {
    final CheckpointableKeyedStateBackend<RowData> backend;
    StockValueState<List<RowData>> legacy;
    ValueState<List<Tuple2<RowData, Long>>> list;
    MapState<Long, Object> nodes;
    MapState<Object, Object> matching;
    ValueState<Object> highest;
    TypeSerializer<Object> keyObjects;
    final CompositeSerializer nodeObjects = new NodeSerializer(inputSerializer);
    final CompositeSerializer indexObjects = new RowSqnInfoSerializer();
    final CompositeSerializer highestObjects = new MetaSqnInfoSerializer();
    final TypeSerializer<Long> nullableLongs =
        NullableSerializer.wrap(LongSerializer.INSTANCE, true);

    States(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
      this.backend = backend;
      if (strategy == Strategy.LEGACY) {
        legacy =
            new StockValueState<>(
                backend, "values", new ListSerializer<>(inputSerializer), ttl > 0);
        return;
      }
      if (strategy == Strategy.VALUE || strategy == Strategy.ADAPTIVE) {
        TypeSerializer<Tuple2<RowData, Long>> tuples =
            new TupleSerializer(
                Tuple2.class, new TypeSerializer[] {inputSerializer, LongSerializer.INSTANCE});
        list = value(backend, "list", new ListSerializer<>(tuples));
      }
      if (strategy == Strategy.VALUE) return;
      var loader = StockSinkMaterializerStateContract.class.getClassLoader();
      keyObjects =
          (TypeSerializer)
              new RowDataKeySerializer(
                  matchSerializer,
                  keyEqualiser.newInstance(loader),
                  keyHash.newInstance(loader),
                  keyEqualiser,
                  keyHash);
      nodes = map(backend, "sqnToNodeState", LongSerializer.INSTANCE, nodeObjects);
      matching = map(backend, "rowToSqnState", keyObjects, indexObjects);
      highest = value(backend, "highestSqnState", highestObjects);
    }

    void clearCurrent() throws Exception {
      if (legacy != null) legacy.clear();
      if (list != null) list.clear();
      if (nodes != null) {
        nodes.clear();
        matching.clear();
        highest.clear();
      }
    }
  }

  private static <T> ValueState<T> value(
      CheckpointableKeyedStateBackend<RowData> backend, String name, TypeSerializer<T> serializer)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new ValueStateDescriptor<>(name, serializer));
  }

  private static <K, V> MapState<K, V> map(
      CheckpointableKeyedStateBackend<RowData> backend,
      String name,
      TypeSerializer<K> keys,
      TypeSerializer<V> values)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(name, keys, values));
  }

  private List<RowData> allKeys(CheckpointableKeyedStateBackend<RowData> backend) throws Exception {
    var unique = new LinkedHashMap<java.nio.ByteBuffer, RowData>();
    for (String name : stateDescriptors().keySet()) {
      try (var stream = backend.getKeys(name, VoidNamespace.INSTANCE)) {
        for (RowData key : stream.collect(Collectors.toList())) {
          var copy = keys.copy(key);
          unique.putIfAbsent(java.nio.ByteBuffer.wrap(keyBytes(copy)), copy);
        }
      }
    }
    return new ArrayList<>(unique.values());
  }

  private void current(CheckpointableKeyedStateBackend<RowData> backend, byte[] bytes) {
    var key = new BinaryRowData(keyType.getFieldCount());
    key.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
    backend.setCurrentKey(key);
  }

  private byte[] keyBytes(RowData key) {
    var binary = new RowDataSerializer(keyType).toBinaryRow(key);
    return org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
        binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
  }

  private static DataInputDeserializer encoded(TypeSerializer serializer, Object value)
      throws Exception {
    var bytes = new DataOutputSerializer(128);
    serializer.serialize(value, bytes);
    return new DataInputDeserializer(bytes.getCopyOfBuffer());
  }

  private RowData payload(RowData bridge, RowType type) {
    var result = new GenericRowData(type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++)
      result.setField(i, RowData.createFieldGetter(type.getTypeAt(i), i).getFieldOrNull(bridge));
    return new RowDataSerializer(type).copy(result);
  }

  private GenericRowData row(RowData payload, RowType type, int extra) {
    var result = new GenericRowData(type.getFieldCount() + extra);
    for (int i = 0; i < type.getFieldCount(); i++)
      result.setField(i, RowData.createFieldGetter(type.getTypeAt(i), i).getFieldOrNull(payload));
    return result;
  }

  private static Long optional(RowData row, int field) {
    return row.isNullAt(field) ? null : row.getLong(field);
  }

  @Override
  public void exportState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    clear(backend);
    var s = new States(backend);
    receive(
        handle,
        0,
        headers,
        header -> {
          current(backend, header.getBinary(0));
          if (header.getBoolean(1)) {
            if (s.highest == null || header.isNullAt(2))
              throw new IllegalArgumentException("Linked materializer cursor missing");
            s.highest.update(s.highestObjects.createInstance(header.getLong(2), header.getLong(3)));
          } else if (s.legacy != null) s.legacy.update(new ArrayList<>(), header.getLong(4));
          else {
            if (s.list == null)
              throw new IllegalArgumentException("Value representation in MAP materializer");
            s.list.update(new ArrayList<>());
          }
        });
    receive(
        handle,
        1,
        candidates,
        bridge -> {
          int n = input.getFieldCount();
          current(backend, bridge.getBinary(n));
          var payload = payload(bridge, input);
          if (s.legacy != null) {
            var stored = s.legacy.value();
            var values = new ArrayList<>(stored.getUserValue());
            if (bridge.getLong(n + 1) != values.size())
              throw new IllegalArgumentException("Candidate position changed");
            values.add(payload);
            s.legacy.update(values, stored.getLastAccessTimestamp());
          } else if (s.highest != null && s.highest.value() != null) {
            s.nodes.put(
                bridge.getLong(n + 1),
                s.nodeObjects.createInstance(
                    payload,
                    bridge.getLong(n + 1),
                    optional(bridge, n + 2),
                    optional(bridge, n + 3),
                    optional(bridge, n + 4),
                    bridge.getLong(n + 5)));
          } else {
            if (s.list == null || bridge.isNullAt(n + 5))
              throw new IllegalArgumentException("Candidate timestamp/representation missing");
            var values = s.list.value();
            values = values == null ? new ArrayList<>() : new ArrayList<>(values);
            if (bridge.getLong(n + 1) != values.size())
              throw new IllegalArgumentException("Candidate position changed");
            values.add(Tuple2.of(payload, bridge.getLong(n + 5)));
            s.list.update(values);
          }
        });
    receive(
        handle,
        2,
        indexes,
        bridge -> {
          int n = matches.getFieldCount();
          current(backend, bridge.getBinary(n));
          if (s.matching == null || s.highest.value() == null)
            throw new IllegalArgumentException("Matching index without linked group");
          var input = encoded(matchSerializer, payload(bridge, matches));
          Object key = s.keyObjects.deserialize(input);
          if (s.matching.contains(key))
            throw new IllegalArgumentException("Duplicate stock matching index");
          s.matching.put(
              key, s.indexObjects.createInstance(bridge.getLong(n + 1), bridge.getLong(n + 2)));
        });
  }

  @FunctionalInterface
  private interface Consumer {
    void accept(RowData row) throws Exception;
  }

  private void receive(long handle, int section, RowType type, Consumer consumer) {
    Native.exportSinkMaterializerCanonical(
        handle,
        section,
        (a, s) -> {
          try (var array = ArrowArray.wrap(a);
              var schema = ArrowSchema.wrap(s);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var reader = ArrowConversion.createArrowReader(root, type);
            for (int i = 0; i < root.getRowCount(); i++) consumer.accept(reader.read(i));
          } catch (Exception error) {
            throw new IllegalStateException("Cannot project stock materializer state", error);
          }
        });
  }

  private void send(long handle, int section, List<RowData> rows, RowType type) {
    try (var root = RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED,
          MapListRepresentation.forExport(root),
          NativeAllocator.DICTIONARIES,
          array,
          schema);
      if (section < 0)
        Native.bindSinkMaterializerCanonical(handle, array.memoryAddress(), schema.memoryAddress());
      else
        Native.importSinkMaterializerCanonical(
            handle, section, array.memoryAddress(), schema.memoryAddress());
    }
  }

  @Override
  public void importState(
      CheckpointableKeyedStateBackend<RowData> backend, long handle, boolean persistent)
      throws Exception {
    StockStateInventory.validate(backend, stateDescriptors(), Set.of(), keys);
    var s = new States(backend);
    var owned = allKeys(backend);
    send(handle, -1, List.of(), input);
    for (RowData key : owned) {
      backend.setCurrentKey(key);
      byte[] bytes = keyBytes(key);
      List<RowData> values = null;
      List<Tuple2<RowData, Long>> tuples = null;
      long written = 0;
      Long high = null;
      long size;
      boolean linked = false;
      long nodeCount = 0, indexCount = 0;
      if (s.legacy != null) {
        var stored = s.legacy.value();
        if (stored == null || stored.getUserValue() == null)
          throw new IllegalArgumentException("Materializer list missing");
        values = stored.getUserValue();
        written = stored.getLastAccessTimestamp();
        size = values.size();
        nodeCount = size;
      } else {
        tuples = s.list == null ? null : s.list.value();
        Object meta = s.highest == null ? null : s.highest.value();
        if (meta != null) {
          if (tuples != null && !tuples.isEmpty())
            throw new IllegalArgumentException("Both adaptive representations contain rows");
          linked = true;
          var encoded = encoded(s.highestObjects, meta);
          high = encoded.readLong();
          size = encoded.readLong();
          for (var ignored : entries(s.nodes)) nodeCount++;
          for (var ignored : entries(s.matching)) indexCount++;
        } else {
          if (s.nodes != null
              && (entries(s.nodes).iterator().hasNext()
                  || entries(s.matching).iterator().hasNext()))
            throw new IllegalArgumentException("Linked materializer metadata missing");
          if (tuples == null)
            throw new IllegalArgumentException("Materializer representation missing");
          size = tuples.size();
          nodeCount = size;
        }
      }
      send(
          handle,
          0,
          List.of(GenericRowData.of(bytes, linked, high, size, written, nodeCount, indexCount)),
          headers);
      var chunk = new ArrayList<RowData>(CHUNK);
      if (!linked) {
        int count = values != null ? values.size() : tuples.size();
        for (int i = 0; i < count; i++) {
          RowData value = values != null ? values.get(i) : tuples.get(i).f0;
          Long sample = values != null ? null : tuples.get(i).f1;
          if (value == null || values == null && sample == null)
            throw new IllegalArgumentException("NULL candidate payload/timestamp");
          var bridge = row(value, input, 6);
          int n = input.getFieldCount();
          bridge.setField(n, bytes);
          bridge.setField(n + 1, (long) i);
          bridge.setField(n + 5, sample);
          chunk.add(bridge);
          if (chunk.size() == CHUNK) {
            send(handle, 1, chunk, candidates);
            chunk.clear();
          }
        }
      } else {
        for (var entry : entries(s.nodes)) {
          var encoded = encoded(s.nodeObjects, entry.getValue());
          var value = inputSerializer.deserialize(encoded);
          long seq = encoded.readLong();
          if (seq != entry.getKey())
            throw new IllegalArgumentException("Sequence key/node changed");
          var bridge = row(value, input, 6);
          int n = input.getFieldCount();
          bridge.setField(n, bytes);
          bridge.setField(n + 1, seq);
          bridge.setField(n + 2, s.nullableLongs.deserialize(encoded));
          bridge.setField(n + 3, s.nullableLongs.deserialize(encoded));
          bridge.setField(n + 4, s.nullableLongs.deserialize(encoded));
          bridge.setField(n + 5, encoded.readLong());
          chunk.add(bridge);
          if (chunk.size() == CHUNK) {
            send(handle, 1, chunk, candidates);
            chunk.clear();
          }
        }
      }
      if (!chunk.isEmpty()) {
        send(handle, 1, chunk, candidates);
        chunk.clear();
      }
      if (linked)
        for (var entry : entries(s.matching)) {
          var match = matchSerializer.deserialize(encoded(s.keyObjects, entry.getKey()));
          var seqs = encoded(s.indexObjects, entry.getValue());
          var bridge = row(match, matches, 3);
          int n = matches.getFieldCount();
          bridge.setField(n, bytes);
          bridge.setField(n + 1, seqs.readLong());
          bridge.setField(n + 2, seqs.readLong());
          chunk.add(bridge);
          if (chunk.size() == CHUNK) {
            send(handle, 2, chunk, indexes);
            chunk.clear();
          }
        }
      if (!chunk.isEmpty()) send(handle, 2, chunk, indexes);
      Native.finishSinkMaterializerCanonicalGroup(handle);
    }
    clear(backend);
  }

  private static <K, V> Iterable<Map.Entry<K, V>> entries(MapState<K, V> state) throws Exception {
    var entries = state.entries();
    return entries == null ? List.of() : entries;
  }
}
