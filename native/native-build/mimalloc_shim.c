// Checked deallocation shims for the library-scoped mimalloc rebinding (see build.rs and
// divergences/19). Aliasing `free` straight to `mi_free` breaks any libc API that returns memory
// it allocated internally with the SYSTEM allocator — `realpath(3)` with a NULL buffer is the one
// this library actually hits (`std::fs::canonicalize` inside the object-store layer's builder).
// Whether that foreign free crashes depends on where the system allocation lands relative to
// mimalloc's segment map, which is why it surfaced only under particular host classpaths (the
// address-space layout shifts with what the JVM has loaded). `free` and `realloc` therefore land
// here instead of on mi_* directly: mimalloc-owned pointers take the fast path
// (mi_is_in_heap_region consults only the global segment map — it never dereferences near the
// pointer), and anything else is returned to the allocator that owns it.
#include <stdbool.h>
#include <stddef.h>

extern bool mi_is_in_heap_region(const void* p);
extern void mi_free(void* p);
extern void* mi_realloc(void* p, size_t newsize);

#if defined(__APPLE__)
#include <malloc/malloc.h>

void sf_free(void* p) {
  if (p == NULL) {
    return;
  }
  if (mi_is_in_heap_region(p)) {
    mi_free(p);
    return;
  }
  malloc_zone_t* zone = malloc_zone_from_ptr(p);
  if (zone != NULL) {
    malloc_zone_free(zone, p);
    return;
  }
  mi_free(p);
}

void* sf_realloc(void* p, size_t newsize) {
  if (p != NULL && !mi_is_in_heap_region(p)) {
    malloc_zone_t* zone = malloc_zone_from_ptr(p);
    if (zone != NULL) {
      return malloc_zone_realloc(zone, p, newsize);
    }
  }
  return mi_realloc(p, newsize);
}

#elif defined(__linux__)
extern void __libc_free(void* p);
extern void* __libc_realloc(void* p, size_t newsize);

void sf_free(void* p) {
  if (p == NULL) {
    return;
  }
  if (mi_is_in_heap_region(p)) {
    mi_free(p);
    return;
  }
  __libc_free(p);
}

void* sf_realloc(void* p, size_t newsize) {
  if (p != NULL && !mi_is_in_heap_region(p)) {
    return __libc_realloc(p, newsize);
  }
  return mi_realloc(p, newsize);
}

#else
#error "the mimalloc feature has no checked-free mapping for this OS"
#endif
