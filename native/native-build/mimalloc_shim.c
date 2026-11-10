// Library-scoped mimalloc aliases must also accept buffers allocated inside host APIs. Checked
// free/realloc preserve libc or macOS zone ownership; Linux realpath additionally resolves into
// caller-owned storage so a third allocator in the host cannot supply its returned buffer.
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
#include <limits.h>

extern void __libc_free(void* p);
extern void* __libc_realloc(void* p, size_t newsize);
extern char* __realpath_chk(const char* path, char* resolved, size_t resolved_size);
extern char* mi_strdup(const char* s);

// A host library may supply realpath's allocated result from a third allocator. Resolve into a
// caller-owned buffer, then return a copy owned by this DSO. __realpath_chk avoids our local alias.
char* sf_realpath(const char* path, char* resolved) {
  if (resolved != NULL) {
    return __realpath_chk(path, resolved, (size_t)-1);
  }
  char buffer[PATH_MAX];
  if (__realpath_chk(path, buffer, sizeof(buffer)) == NULL) {
    return NULL;
  }
  return mi_strdup(buffer);
}

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
