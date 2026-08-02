# One shared FFI allocator

**Applies to:** every buffer crossing the JNI boundary

Per-operator Arrow allocators created and closed per operator were replaced with a single
long-lived allocator (Comet's pattern) for buffers crossing the boundary; Arrow reference counting
reclaims batches as they release.
