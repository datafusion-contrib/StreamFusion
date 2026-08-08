# 29 — Native logs bridge into SLF4J instead of Comet's log4rs console appender

## Reference pattern

DataFusion Comet initializes `log4rs` from an explicit JNI init call (optionally reading a log4rs
config file) and writes native log records to stderr. One native library, one Java-driven init,
console output.

## StreamFusion decision

Each StreamFusion native library installs, from its own `JNI_OnLoad`, a Rust `log`-facade logger
that upcalls a small static Java receiver, which forwards to SLF4J. The receiver's class and method
handles are cached as a global ref/`JStaticMethodID` at load time, and threads that are not JVM
threads are attached as daemons on first use. Without a JVM — the
standalone `cargo test`/Criterion binaries — no logger is installed and a failed upcall falls back
to stderr, so the DSO never depends on the JVM to log.

## Why deviate

- Flink operations read logs through the TaskManager log files and web UI, which are fed by the
  deployment's log4j/SLF4J configuration; stderr lands in `.out` files outside rotation and
  per-logger level control. Routing through SLF4J gives native operator logs the same knobs as every
  other Flink logger.
- StreamFusion ships several native libraries (core plus per-connector/format extensions), each
  with its own copy of the `log` statics. `JNI_OnLoad` is the one per-library hook that runs
  exactly once per loaded DSO with the right class loader in scope, without adding a Java-side
  init entry point to every extension class — Comet's single explicit init assumes one library.
- Resolving the receiver at load time matters: log events fire on non-JVM threads where
  `FindClass` uses the system class loader and would miss classes a child loader owns.
