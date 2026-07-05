# Byte-parity upcall builtins: hand off UTF-8 bytes, not java.lang.String

**Status:** TODO (follow-on to the 2026-07-04 round's q21 work — the regex-compile cache took the
parity path 0.75x → 0.86x on the generator; this is the remaining identified deficit).

**Measured residue:** q21-native's top JVM leaves are `String.<init>` (~5%), `StringLatin1.
toLowerCase` (~4%), and `StringUTF16.checkIndex` (~4%) — the upcall materializes a `java.lang.
String` per row from the Arrow column, and `NativeBuiltinFunctions.lower` then round-trips it
`String → BinaryStringData → toLowerCase → String` before the result is re-encoded to Arrow.

**Fix:** the upcall boundary should stay in bytes. Read the UTF-8 bytes off the Arrow
`VarCharVector` into `BinaryStringData.fromBytes` directly (no `String`), run Flink's own
`BinaryStringData.toLowerCase/toUpperCase` — whose ASCII fast path operates on the byte array and
never decodes — and write the result bytes straight back into the output vector. Byte-identical by
construction (it is Flink's code operating on the same bytes); the `String` materialization
survives only on the non-ASCII slow path, where Flink itself materializes. `REGEXP_EXTRACT` needs
the `String` for `java.util.regex` (keep it), but its pattern is already cached and its input
string could at least skip the UTF-16 re-validation (`fromBytes` + lazy `toString` only when the
regex actually runs — measure whether that pays).

Expected: most of the ~9-10% string-materialization share on q21's parity path; also trims every
LTZ datetime upcall (q10/q14/q15/q16/q17 on Kafka) whose inputs take the same boundary.

Acceptance: q21 parity-path profile shows the `String.<init>`/`toLowerCase` leaves gone; upcall
parity tests (incl. non-ASCII inputs) green; q21 re-measured on the profile loop.
