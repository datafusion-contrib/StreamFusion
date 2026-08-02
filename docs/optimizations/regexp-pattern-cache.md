# Host-exact REGEXP_EXTRACT compiles its pattern once

**Applies to:** REGEXP_EXTRACT (host-exact upcall path)

Flink's own `SqlFunctionUtils.regexpExtract` calls `Pattern.compile` on *every invocation*. The
upcall now routes to a byte-identical reimplementation that caches the compiled `Pattern` per regex
string instead — it's the same `java.util.regex` engine, so the output cannot differ, and
compilation is pure, so caching it changes nothing observable.

A CPU profile put the per-call compile at ~13% of q21's total. Caching it lifted the whole query
+12.5% (96 → 108 profile-loop iterations), with the compile subtree measuring zero afterward. Stock
Flink pays the recompile cost on every REGEXP_EXTRACT row; we no longer do.
