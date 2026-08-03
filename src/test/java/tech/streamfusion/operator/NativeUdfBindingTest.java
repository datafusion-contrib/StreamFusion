package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;

/**
 * The distributed-execution mechanism for native UDFs: a {@link NativeUdf.Binding} carries
 * the (serializable) {@link ScalarFunction} and marshalling metadata into the operator, and at {@code
 * open()} registers it into <em>this</em> JVM's registry — so a task manager (a different JVM from the
 * planner, whose registry is empty) can resolve the UDF. This exercises the part the SQL harness cannot:
 * a Java-serialization round-trip (as Flink does when shipping the operator to a task), then re-resolving
 * the non-serializable {@code eval} method and registering it, with the encoded id slots patched from
 * their plan-time local indices to the task-local runtime ids.
 */
class NativeUdfBindingTest {

  /** A trivial user function; its {@code eval} is re-resolved by name+params after deserialization. */
  public static final class Exclaim extends ScalarFunction {
    public String eval(String s) {
      return s + "!";
    }
  }

  private static NativeUdf.Binding roundTrip(NativeUdf.Binding binding) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(binding);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (NativeUdf.Binding) in.readObject();
    }
  }

  @Test
  void bindingSurvivesSerializationAndRegistersOnAFreshJvm() throws Exception {
    Method eval = Exclaim.class.getMethod("eval", String.class);
    NativeUdf.Descriptor a =
        NativeUdf.Descriptor.forFunction(
            new Exclaim(), eval, new int[] {NativeUdf.TYPE_STRING}, NativeUdf.TYPE_STRING);
    NativeUdf.Descriptor b =
        NativeUdf.Descriptor.forFunction(
            new Exclaim(), eval, new int[] {NativeUdf.TYPE_STRING}, NativeUdf.TYPE_STRING);

    // Two id slots referencing the descriptors by local index in reverse, so a correct bind must use
    // the local-index -> runtime-id mapping (not treat the slot value as the id).
    int[] idSlots = {0, 1};
    long[] encodedLongs = {1, 0}; // slot 0 -> local index 1 (b); slot 1 -> local index 0 (a)
    NativeUdf.Binding binding =
        roundTrip(new NativeUdf.Binding(new NativeUdf.Descriptor[] {a, b}, idSlots));

    long[] bound = binding.bind(encodedLongs);

    // The registry assigns runtime ids in descriptor order, so a -> base, b -> base + 1; after the
    // reverse mapping, slot 0 holds b's id and slot 1 holds a's id.
    assertNotEquals(bound[0], bound[1], "the two UDFs must get distinct runtime ids");
    assertEquals(bound[0], bound[1] + 1, "slot 0 -> descriptor b (base+1), slot 1 -> descriptor a (base)");
    assertTrue(bound[0] >= 0 && bound[1] >= 0, "runtime ids are assigned");
    // The pristine encoded array is untouched (a re-open must rebind from local indices).
    assertEquals(1, encodedLongs[0]);
    assertEquals(0, encodedLongs[1]);

    binding.unbind();
  }

  @Test
  void emptyBindingIsANoOp() throws Exception {
    NativeUdf.Binding empty = roundTrip(NativeUdf.Binding.EMPTY);
    long[] longs = {7, 8, 9};
    assertEquals(longs, empty.bind(longs), "no UDFs -> the longs array is returned unchanged");
    empty.unbind();
  }
}
