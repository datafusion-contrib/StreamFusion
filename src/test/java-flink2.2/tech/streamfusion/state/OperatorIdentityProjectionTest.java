package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.Test;

class OperatorIdentityProjectionTest {
  private static final OperatorID NATIVE = new OperatorID(1, 2);
  private static final OperatorID STOCK = new OperatorID(3, 4);
  private final OperatorIdentityProjection projection =
      new OperatorIdentityProjection(Map.of(NATIVE.toString(), STOCK.toString()));

  @Test
  void preservesCoordinatorSubtasksMasterStateAndOwnershipInBothDirections() throws Exception {
    OperatorState source = state(NATIVE);
    var coordinator = new ByteStreamStateHandle("enumerator", new byte[] {1, 7, 9});
    source.setCoordinatorState(coordinator);
    var keyed =
        new KeyGroupsStateHandle(
            new KeyGroupRangeOffsets(0, 0, new long[] {0}),
            new ByteStreamStateHandle("keyed", new byte[] {2, 3}));
    var subtask =
        OperatorSubtaskState.builder()
            .setManagedKeyedState(new StateObjectCollection<>(List.of(keyed)))
            .build();
    source.putState(0, subtask);
    MasterState master = new MasterState("user-hook", new byte[] {9, 8}, 1);
    CheckpointMetadata original =
        metadata(List.of(source), List.of(master), SavepointFormatType.CANONICAL);
    CheckpointMetadata exported = projection.exportMetadata(original);
    OperatorState target = exported.getOperatorStates().iterator().next();
    assertEquals(STOCK, target.getOperatorID());
    assertSame(coordinator, target.getCoordinatorState());
    assertSame(subtask, target.getState(0));
    assertSame(master, exported.getMasterStates().iterator().next());
    assertEquals(NATIVE, source.getOperatorID());
    assertEquals(source, projection.importMetadata(exported).getOperatorStates().iterator().next());
    assertEquals(11L, exported.getCheckpointId());
    assertSame(original.getCheckpointProperties(), exported.getCheckpointProperties());
  }

  @Test
  void preservesFinishedOperatorsAndFinishedSubtasks() throws Exception {
    var finished = new FullyFinishedOperatorState("source", "uid", NATIVE, 2, 128);
    var exported =
        projection.exportMetadata(
            metadata(List.of(finished), List.of(), SavepointFormatType.CANONICAL));
    var actual = exported.getOperatorStates().iterator().next();
    assertTrue(actual.isFullyFinished());
    assertEquals(2, actual.getParallelism());
    assertEquals(128, actual.getMaxParallelism());
    assertEquals(STOCK, actual.getOperatorID());
    var partial = state(NATIVE);
    partial.putState(0, org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState.INSTANCE);
    assertTrue(
        projection
            .exportMetadata(metadata(List.of(partial), List.of(), SavepointFormatType.CANONICAL))
            .getOperatorStates()
            .iterator()
            .next()
            .getState(0)
            .isFinished());
  }

  @Test
  void dropsOnlyUnmappedEmptyOperatorsAndRejectsEvenZeroByteCoordinatorState() throws Exception {
    var extra = state(new OperatorID(8, 9));
    extra.putState(0, OperatorSubtaskState.builder().build());
    assertTrue(
        projection
            .exportMetadata(metadata(List.of(extra), List.of(), SavepointFormatType.CANONICAL))
            .getOperatorStates()
            .isEmpty());
    extra.setCoordinatorState(new ByteStreamStateHandle("empty-but-owned", new byte[0]));
    assertThrows(
        IOException.class,
        () ->
            projection.exportMetadata(
                metadata(List.of(extra), List.of(), SavepointFormatType.CANONICAL)));
  }

  @Test
  void refusesUnknownStateDuringImportAndDuplicateMappings() {
    var extra = state(new OperatorID(8, 9));
    extra.setCoordinatorState(new ByteStreamStateHandle("coordinator", new byte[] {1}));
    assertThrows(
        IOException.class,
        () ->
            projection.importMetadata(
                metadata(List.of(extra), List.of(), SavepointFormatType.CANONICAL)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OperatorIdentityProjection(
                Map.of(
                    NATIVE.toString(),
                    STOCK.toString(),
                    extra.getOperatorID().toString(),
                    STOCK.toString())));
  }

  @Test
  void unmappedFinishedStateCannotBeSilentlyDropped() {
    var fullyFinished =
        new FullyFinishedOperatorState("source", "uid", new OperatorID(8, 9), 1, 128);
    assertThrows(
        IOException.class,
        () ->
            projection.exportMetadata(
                metadata(List.of(fullyFinished), List.of(), SavepointFormatType.CANONICAL)));
    var partial = state(new OperatorID(8, 9));
    partial.putState(0, org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState.INSTANCE);
    assertThrows(
        IOException.class,
        () ->
            projection.importMetadata(
                metadata(List.of(partial), List.of(), SavepointFormatType.CANONICAL)));
  }

  @Test
  void knownEmptyNativeHelpersMayOmitFinishedMarkersOnlyDuringExport() throws Exception {
    OperatorID helper = new OperatorID(8, 9);
    var projection =
        new OperatorIdentityProjection(
            Map.of(NATIVE.toString(), STOCK.toString()), java.util.Set.of(helper.toString()));
    var finished = new FullyFinishedOperatorState("transpose", null, helper, 2, 128);
    var source = new FullyFinishedOperatorState("source", "uid", NATIVE, 2, 128);
    var result =
        projection.exportMetadata(
            metadata(List.of(source, finished), List.of(), SavepointFormatType.CANONICAL));
    assertEquals(1, result.getOperatorStates().size());
    assertTrue(result.getOperatorStates().iterator().next().isFullyFinished());
    assertEquals(STOCK, result.getOperatorStates().iterator().next().getOperatorID());
    var partial = state(helper);
    partial.putState(0, org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState.INSTANCE);
    assertTrue(
        projection
            .exportMetadata(metadata(List.of(partial), List.of(), SavepointFormatType.CANONICAL))
            .getOperatorStates()
            .isEmpty());
    assertThrows(
        IOException.class,
        () ->
            projection.importMetadata(
                metadata(List.of(finished), List.of(), SavepointFormatType.CANONICAL)));
  }

  @Test
  void helperDeclarationsCannotHideDurableStateOrReplaceLogicalBindings() throws Exception {
    OperatorID helper = new OperatorID(8, 9);
    var projection =
        new OperatorIdentityProjection(
            Map.of(NATIVE.toString(), STOCK.toString()), java.util.Set.of(helper.toString()));
    var state = state(helper);
    state.setCoordinatorState(new ByteStreamStateHandle("empty-owned", new byte[0]));
    assertThrows(
        IOException.class,
        () ->
            projection.exportMetadata(
                metadata(List.of(state), List.of(), SavepointFormatType.CANONICAL)));
    var keyedState = state(helper);
    keyedState.putState(
        0,
        OperatorSubtaskState.builder()
            .setManagedKeyedState(
                new StateObjectCollection<>(
                    List.of(
                        new KeyGroupsStateHandle(
                            new KeyGroupRangeOffsets(0, 0, new long[] {0}),
                            new ByteStreamStateHandle("empty-owned-keyed", new byte[0])))))
            .build());
    assertThrows(
        IOException.class,
        () ->
            projection.exportMetadata(
                metadata(List.of(keyedState), List.of(), SavepointFormatType.CANONICAL)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OperatorIdentityProjection(
                Map.of(NATIVE.toString(), STOCK.toString()), java.util.Set.of(NATIVE.toString())));
  }

  @Test
  void importPropagatesFullyFinishedStateThroughNewNativeBoundaries() throws Exception {
    OperatorID entry = new OperatorID(8, 9);
    OperatorID exit = new OperatorID(10, 11);
    var projection =
        new OperatorIdentityProjection(
            Map.of(NATIVE.toString(), STOCK.toString()),
            Map.of(
                entry.toString(),
                new OperatorIdentityProjection.EmptyHelper(NATIVE.toString(), 2, 128, true),
                exit.toString(),
                new OperatorIdentityProjection.EmptyHelper(entry.toString(), 2, 128, true)));
    var source = new FullyFinishedOperatorState("source", "uid", STOCK, 2, 128);
    var imported =
        projection.importMetadata(
            metadata(List.of(source), List.of(), SavepointFormatType.CANONICAL));
    assertEquals(3, imported.getOperatorStates().size());
    assertTrue(imported.getOperatorStates().stream().allMatch(OperatorState::isFullyFinished));
    var exported = projection.exportMetadata(imported);
    assertEquals(1, exported.getOperatorStates().size());
    assertEquals(STOCK, exported.getOperatorStates().iterator().next().getOperatorID());
    assertTrue(source.isFullyFinished());
  }

  @Test
  void partialFinishedFlagsFollowOnlyPointwiseBoundariesWithTheSameParallelism() throws Exception {
    OperatorID entry = new OperatorID(8, 9);
    OperatorID shuffled = new OperatorID(10, 11);
    OperatorID rescaled = new OperatorID(12, 13);
    var projection =
        new OperatorIdentityProjection(
            Map.of(NATIVE.toString(), STOCK.toString()),
            Map.of(
                entry.toString(),
                new OperatorIdentityProjection.EmptyHelper(NATIVE.toString(), 2, -1, true),
                shuffled.toString(),
                new OperatorIdentityProjection.EmptyHelper(entry.toString(), 2, 128, false),
                rescaled.toString(),
                new OperatorIdentityProjection.EmptyHelper(entry.toString(), 3, 128, true)));
    var source = new OperatorState("source", "uid", STOCK, 2, 128);
    source.putState(0, org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState.INSTANCE);
    source.putState(1, OperatorSubtaskState.builder().build());
    var imported =
        projection.importMetadata(
            metadata(List.of(source), List.of(), SavepointFormatType.CANONICAL));
    assertEquals(2, imported.getOperatorStates().size());
    var boundary =
        imported.getOperatorStates().stream()
            .filter(state -> state.getOperatorID().equals(entry))
            .findFirst()
            .orElseThrow();
    assertTrue(boundary.getState(0).isFinished());
    assertFalse(boundary.getState(1).isFinished());
    assertEquals(128, boundary.getMaxParallelism());
  }

  @Test
  void unionBoundaryFinishesOnlyWhereEveryUpstreamInputHasFinished() throws Exception {
    OperatorID otherStock = new OperatorID(21, 22), otherNative = new OperatorID(23, 24);
    OperatorID helper = new OperatorID(25, 26);
    var projection =
        new OperatorIdentityProjection(
            Map.of(
                NATIVE.toString(), STOCK.toString(), otherNative.toString(), otherStock.toString()),
            Map.of(
                helper.toString(),
                new OperatorIdentityProjection.EmptyHelper(
                    List.of(NATIVE.toString(), otherNative.toString()), 2, 128, true)));
    var first = new OperatorState("first", "first", STOCK, 2, 128);
    first.putState(0, org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState.INSTANCE);
    first.putState(1, OperatorSubtaskState.builder().build());
    var second = new OperatorState("second", "second", otherStock, 2, 128);
    second.putState(0, OperatorSubtaskState.builder().build());
    second.putState(1, org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState.INSTANCE);
    assertEquals(
        2,
        projection
            .importMetadata(
                metadata(List.of(first, second), List.of(), SavepointFormatType.CANONICAL))
            .getOperatorStates()
            .size());
    var completed = new FullyFinishedOperatorState("second", "second", otherStock, 2, 128);
    var imported =
        projection.importMetadata(
            metadata(List.of(first, completed), List.of(), SavepointFormatType.CANONICAL));
    var boundary =
        imported.getOperatorStates().stream()
            .filter(state -> state.getOperatorID().equals(helper))
            .findFirst()
            .orElseThrow();
    assertFalse(boundary.isFullyFinished());
    assertTrue(boundary.getState(0).isFinished());
    assertFalse(boundary.getState(1).isFinished());
  }

  @Test
  void leavesNativeSavepointAndCheckpointMetadataUntouched() throws Exception {
    var nativeSavepoint = metadata(List.of(state(NATIVE)), List.of(), SavepointFormatType.NATIVE);
    assertSame(nativeSavepoint, projection.exportMetadata(nativeSavepoint));
    assertSame(nativeSavepoint, projection.importMetadata(nativeSavepoint));
    var checkpoint =
        new CheckpointMetadata(
            12,
            List.of(state(NATIVE)),
            List.of(),
            CheckpointProperties.forCheckpoint(
                org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy
                    .NEVER_RETAIN_AFTER_TERMINATION));
    assertSame(checkpoint, projection.exportMetadata(checkpoint));
    assertSame(checkpoint, projection.importMetadata(checkpoint));
  }

  private static OperatorState state(OperatorID id) {
    return new OperatorState("operator", "uid", id, 1, 128);
  }

  private static CheckpointMetadata metadata(
      List<OperatorState> states, List<MasterState> masters, SavepointFormatType format) {
    return new CheckpointMetadata(
        11, states, masters, CheckpointProperties.forSavepoint(false, format));
  }
}
