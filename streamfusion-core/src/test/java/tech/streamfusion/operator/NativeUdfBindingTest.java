package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import org.apache.flink.table.functions.FunctionContext;
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

  @Test
  void sharedInstanceOwnsOneLifecycleAcrossDifferentSignatures() throws Exception {
    CountingFunction function = new CountingFunction();
    NativeUdf.Binding binding = sharedBinding(function);
    long[] ids = binding.bind(new long[] {0, 1});
    assertEquals(1, function.opens);
    assertNotEquals(ids[0], ids[1], "call sites retain their own signature registrations");
    binding.unbind();
    binding.unbind();
    assertEquals(1, function.closes);
    assertUnregistered(ids);

    binding.bind(new long[] {0, 1});
    binding.unbind();
    assertEquals(2, function.opens);
    assertEquals(2, function.closes);
  }

  @Test
  void serializationPreservesSharedLifecycleOwnership() throws Exception {
    NativeUdf.Binding binding = roundTrip(sharedBinding(new CountingFunction()));
    binding.bind(new long[] {0, 1});
    binding.unbind();
  }

  @Test
  void generatedAndDirectCallsShareDependenciesAfterSerialization() throws Exception {
    CountingFunction shared = new CountingFunction();
    NativeUdf.Binding binding = roundTrip(binding(new CompositeFunction(shared), shared));
    long[] ids = binding.bind(new long[] {0, 1});
    binding.unbind();
    assertUnregistered(ids);
  }

  @Test
  void failedGeneratedInitializationClosesItsOpenedDependencies() throws Exception {
    CountingFunction shared = new CountingFunction();
    CountingFunction generated = new CompositeFunction(shared);
    generated.failOpen = true;
    NativeUdf.Binding binding = binding(generated, shared);
    assertThrows(IllegalStateException.class, () -> binding.bind(new long[] {0, 1}));
    assertEquals(1, shared.opens);
    assertEquals(1, shared.closes);
    generated.failOpen = false;
    binding.bind(new long[] {0, 1});
    binding.unbind();
    assertEquals(2, shared.opens);
    assertEquals(2, shared.closes);
  }

  public static class CompositeFunction extends CountingFunction
      implements NativeUdf.FunctionDependencies {
    private final ScalarFunction dependency;

    CompositeFunction(ScalarFunction dependency) {
      this.dependency = dependency;
    }

    @Override
    public java.util.List<ScalarFunction> functions() {
      return java.util.List.of(dependency);
    }
  }

  @Test
  void distinctInstancesOfTheSameClassHaveSeparateLifecycles() throws Exception {
    CountingFunction first = new CountingFunction();
    CountingFunction second = new CountingFunction();
    NativeUdf.Binding binding = binding(first, second);
    binding.bind(new long[] {0, 1});
    assertEquals(1, first.opens);
    assertEquals(1, second.opens);
    binding.unbind();
    assertEquals(1, first.closes);
    assertEquals(1, second.closes);
  }

  @Test
  void failedOpenReleasesEarlierFunctionsAndCanBeRetried() throws Exception {
    CountingFunction first = new CountingFunction();
    CountingFunction second = new CountingFunction();
    second.failOpen = true;
    NativeUdf.Binding binding = binding(first, second);
    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> binding.bind(new long[] {0, 1}));
    assertEquals("open failure", failure.getCause().getMessage());
    assertEquals(1, first.closes);
    assertEquals(0, second.closes);
    binding.unbind();
    assertEquals(1, first.closes);
    second.failOpen = false;
    binding.bind(new long[] {0, 1});
    binding.unbind();
    assertEquals(2, first.closes);
    assertEquals(1, second.closes);
  }

  @Test
  void cleanupFailuresDoNotHideOpenFailure() throws Exception {
    CountingFunction first = new CountingFunction();
    first.failClose = true;
    CountingFunction second = new CountingFunction();
    second.failOpen = true;
    NativeUdf.Binding binding = binding(first, second);
    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> binding.bind(new long[] {0, 1}));
    assertEquals("open failure", failure.getCause().getMessage());
    assertEquals(1, failure.getSuppressed().length);
    assertEquals("close failure", failure.getSuppressed()[0].getCause().getMessage());
    binding.unbind();
    assertEquals(1, first.closes);
  }

  @Test
  void failedCloseStillReleasesEveryRegistrationAndFunction() throws Exception {
    CountingFunction first = new CountingFunction();
    first.failClose = true;
    CountingFunction second = new CountingFunction();
    second.failClose = true;
    NativeUdf.Binding binding = binding(first, second);
    long[] ids = binding.bind(new long[] {0, 1});
    IllegalStateException failure = assertThrows(IllegalStateException.class, binding::unbind);
    assertEquals(1, failure.getSuppressed().length);
    assertEquals(1, first.closes);
    assertEquals(1, second.closes);
    assertUnregistered(ids);
    binding.unbind();
    assertEquals(1, first.closes);
    assertEquals(1, second.closes);
  }

  @Test
  void bindingCannotBeOpenedTwiceWithoutCleanup() throws Exception {
    CountingFunction function = new CountingFunction();
    NativeUdf.Binding binding = sharedBinding(function);
    long[] ids = binding.bind(new long[] {0, 1});
    assertThrows(IllegalStateException.class, () -> binding.bind(new long[] {0, 1}));
    binding.unbind();
    assertEquals(1, function.opens);
    assertEquals(1, function.closes);
    assertUnregistered(ids);
  }

  private static NativeUdf.Binding sharedBinding(CountingFunction function) throws Exception {
    return new NativeUdf.Binding(
        new NativeUdf.Descriptor[] {
          descriptor(function),
          NativeUdf.Descriptor.forFunction(
              function,
              CountingFunction.class.getMethod("eval", String.class),
              new int[] {NativeUdf.TYPE_STRING},
              NativeUdf.TYPE_STRING)
        },
        new int[] {0, 1});
  }

  private static NativeUdf.Binding binding(CountingFunction first, CountingFunction second)
      throws Exception {
    return new NativeUdf.Binding(
        new NativeUdf.Descriptor[] {descriptor(first), descriptor(second)}, new int[] {0, 1});
  }

  private static NativeUdf.Descriptor descriptor(CountingFunction function) throws Exception {
    return NativeUdf.Descriptor.forFunction(
        function,
        CountingFunction.class.getMethod("eval", Integer.class),
        new int[] {NativeUdf.TYPE_INT},
        NativeUdf.TYPE_INT);
  }

  private static void assertUnregistered(long[] ids) {
    for (long id : ids) {
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class, () -> NativeUdf.invokeUdf((int) id, 0, 0, 0, 0));
      assertEquals("no registered UDF for id " + id, failure.getMessage());
    }
  }

  public static class CountingFunction extends ScalarFunction {
    private transient int opens;
    private transient int closes;
    private boolean failOpen;
    private boolean failClose;

    @Override
    public void open(FunctionContext context) {
      if (failOpen) {
        throw new IllegalStateException("open failure");
      }
      if (opens != closes) {
        throw new IllegalStateException("already open");
      }
      opens++;
    }

    @Override
    public void close() {
      if (opens != closes + 1) {
        throw new IllegalStateException("not open");
      }
      closes++;
      if (failClose) {
        throw new IllegalStateException("close failure");
      }
    }

    public Integer eval(Integer value) {
      return value;
    }

    public String eval(String value) {
      return value;
    }
  }
}
