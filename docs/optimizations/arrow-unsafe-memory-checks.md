# Disable Arrow Java's per-accessor safety checks

**Applies to:** the RowData↔Arrow transpose at every native island's edges

## What it is

StreamFusion defaults `arrow.enable_unsafe_memory_access` to `true` and
`arrow.enable_null_check_for_get` to `false` before Arrow conversion, allocation or native library
initialization. This follows [Comet's initialization pattern](https://github.com/apache/datafusion-comet/blob/4de6704391c9a7832a2079fc4e8d80fc09e9b209/spark/src/main/java/org/apache/comet/NativeBase.java#L226).
Explicit system properties, Arrow environment variables and the legacy unsafe-access property
remain authoritative; no additional StreamFusion switch is introduced. See
[configuration](../configuration.md#arrow-java-access-checks) for precedence and debugging overrides.

## Why it works

Unsafe access skips ArrowBuf's per-access bounds and reference-count liveness checks. Disabling
getter NULL checks separately avoids a redundant validity lookup in primitive getters after the
caller has already checked `isNullAt`. NULL validity bitmaps and object getters retain their
semantics, and safe setters still grow vector capacity. Allocation budgeting and retain/release
are unaffected. Correct row indices, nullable-value handling and buffer lifetimes remain required;
an ownership or bounds bug can become memory corruption instead of a Java access exception.

Arrow stores both settings in static-final fields, so defaults must precede their first use.
Initialization logs the effective checks and warns when earlier Arrow initialization prevents
the requested configuration from taking effect. It never attempts to rewrite initialized fields.
Properties affect the whole JVM, including other Arrow components; startup `-D` options select
checks reliably even when another component initializes Arrow first.

## Measured

The historical q0 profile (`5454540`) recorded the entry transpose falling from approximately
21% to 12% of CPU after the explicit JVM options removed repeated checks. This is historical
CPU sampling evidence, not a current-main throughput measurement or a separately measured gain
for each property. Applying the defaults makes that existing deployment configuration automatic;
it does not change the native kernels.
