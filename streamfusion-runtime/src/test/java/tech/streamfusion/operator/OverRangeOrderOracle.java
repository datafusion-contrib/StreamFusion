package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Generates native RANGE iteration controls from the released SDK and the running JDK. */
public final class OverRangeOrderOracle {
  private Map<Long, Integer> map = new HashMap<>();
  private final List<String> steps = new ArrayList<>();

  private void step(String operation, long key) throws Exception {
    if (operation.equals("put")) map.put(key, 0);
    else if (operation.equals("remove")) map.remove(key);
    else if (operation.equals("savepoint")) {
      var restored = new HashMap<Long, Integer>();
      for (var entry : map.entrySet()) restored.put(entry.getKey(), entry.getValue());
      map = restored;
    } else {
      var serializer = new MapSerializer<>(LongSerializer.INSTANCE, IntSerializer.INSTANCE);
      var output = new DataOutputSerializer(128);
      serializer.serialize(map, output);
      map = serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    }
    steps.add(
        "{\"operation\":\""
            + operation
            + "\",\"key\":"
            + key
            + ",\"expected\":"
            + new ArrayList<>(map.keySet())
            + "}");
  }

  private static long collision(int value, int hash) {
    return ((long) value << 32) | Integer.toUnsignedLong(value ^ hash);
  }

  private static String fixture(String name, OverRangeOrderOracle oracle) {
    return "{\"name\":\"" + name + "\",\"steps\":[" + String.join(",", oracle.steps) + "]}";
  }

  public static void main(String[] arguments) throws Exception {
    List<String> fixtures = new ArrayList<>();
    var empty = new OverRangeOrderOracle();
    empty.step("checkpoint", 0);
    for (long key : new long[] {100, 200, 400, 2000}) empty.step("put", key);
    fixtures.add(fixture("empty_checkpoint", empty));
    var ordinary = new OverRangeOrderOracle();
    for (long key : new long[] {100, 200, 400, 2000, Long.MIN_VALUE, Long.MAX_VALUE, -1, 0})
      ordinary.step("put", key);
    ordinary.step("checkpoint", 0);
    for (long key : new long[] {400, 100, Long.MIN_VALUE, -1}) ordinary.step("remove", key);
    ordinary.step("checkpoint", 0);
    fixtures.add(fixture("ordinary_and_signed_limits", ordinary));

    for (int mode = 0; mode < 3; mode++) {
      var collisions = new OverRangeOrderOracle();
      for (int key = 1; key <= 24; key++) collisions.step("put", collision(key, 7));
      for (int i = 1; i <= 22; i++) {
        int key = mode == 0 ? i : mode == 1 ? 25 - i : (i * 11 % 24) + 1;
        collisions.step("remove", collision(key, 7));
      }
      for (int key = 25; key <= 40; key++) collisions.step("put", collision(key, 7));
      collisions.step("checkpoint", 0);
      collisions.step("savepoint", 0);
      for (int key = 25; key <= 40; key += 2) collisions.step("remove", collision(key, 7));
      fixtures.add(fixture("tree_removal_" + mode, collisions));
    }

    var signed = new OverRangeOrderOracle();
    for (int key = 1; key <= 24; key++)
      signed.step("put", collision(key, key % 2 == 0 ? 7 : 0x80000007));
    signed.step("checkpoint", 0);
    signed.step("savepoint", 0);
    for (int key = 1; key <= 20; key++)
      signed.step("remove", collision(key, key % 2 == 0 ? 7 : 0x80000007));
    fixtures.add(fixture("signed_tree_hashes", signed));
    var split = new OverRangeOrderOracle();
    for (int key = 1; key <= 24; key++) split.step("put", collision(key, key % 2 == 0 ? 7 : 71));
    for (int key = 0; key < 100; key++) split.step("put", 1000 + key);
    for (int key = 1; key <= 18; key++) split.step("remove", collision(key, key % 2 == 0 ? 7 : 71));
    split.step("checkpoint", 0);
    fixtures.add(fixture("tree_split_and_untreeify", split));

    for (int seed = 0; seed < 4; seed++) {
      var random = new OverRangeOrderOracle();
      var rng = new Random(seed);
      for (int i = 0; i < 160; i++) {
        long key = collision(1 + rng.nextInt(40), seed % 2 == 0 ? 0 : rng.nextInt(4) * 64 + 7);
        random.step(
            i % 37 == 0
                ? "checkpoint"
                : i % 61 == 0 ? "savepoint" : rng.nextInt(3) == 0 ? "remove" : "put",
            key);
      }
      fixtures.add(fixture("mixed_history_" + seed, random));
    }
    System.out.println("[" + String.join(",\n", fixtures) + "]");
  }
}
