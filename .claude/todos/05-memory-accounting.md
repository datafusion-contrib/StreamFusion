# Account native memory against Flink's MemoryManager

**Status:** open
**Source:** research findings §4 (open question #1)

## Problem
Arrow buffers and DataFusion accumulators allocate off-heap in native code,
invisible to Flink's `MemoryManager`. A task can blow past its managed-memory
budget and get OOM-killed by the container rather than spilling or
backpressuring, because Flink has no idea the native side is holding memory.

## Goal
Wire native allocations through a memory pool that reserves/releases against
Flink's `MemoryManager`, mirroring Comet's `MemoryPool` → JNI →
`CometTaskMemoryManager` pattern, retargeted at Flink's
`MemoryManager.reserveMemory`/`releaseMemory` with a managed-memory use case.

## Acceptance criteria
- Native allocations reserve managed memory and release it on operator close.
- A window operator holding many open windows reports its footprint to Flink.
- Exceeding the budget produces a clear failure/backpressure, not a silent
  container OOM.

## Pointers
- Comet `native/core/src/execution/memory_pools/` (`CometUnifiedMemoryPool`)
  and `CometTaskMemoryManager.java`.
- Flink `MemoryManager.reserveMemory/releaseMemory`, `ManagedMemoryUseCase`.
- The spill gap (research §4): DataFusion can spill to disk/object store; Flink
  expects managed memory — decide the disaggregated/object-store state story.
