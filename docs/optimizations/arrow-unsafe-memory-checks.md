# Disable Arrow Java's per-accessor safety checks

**Applies to:** the RowData↔Arrow transpose at every native island's edges

## What it is

Two JVM flags — `arrow.enable_unsafe_memory_access` and `arrow.enable_null_check_for_get` — set the
same way Comet/Spark set them, removing Arrow Java's per-accessor bounds and refcount checks
(`5454540`).

## Why it works

Every vector read and write in the entry transpose was paying a bounds check and a refcount check
per accessor call. Those checks accounted for roughly a third of native-side CPU on q0.

## Measured

Cut the entry transpose from ~21% to ~12% of CPU.
