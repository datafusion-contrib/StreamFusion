# Concrete List argument bridges on released Flink

Decision: preserve released planning rejection for concrete/custom List argument conversion
classes. This is a scoped outcome within #237; the issue remains open for other contracts.
Concrete List result conversions are a separate, valid direction, with scalar ArrayList
results covered in the Correlate operator page.

The released `flink-table-common` source artifacts for 1.18.1 and 2.2.1 contain identical
`org.apache.flink.table.types.logical.ArrayType` implementations. `supportsInputConversion`
accepts classes assignable to `java.util.List`. `supportsOutputConversion` accepts the exact
`List` and `ArrayData` classes or supported Java arrays; it does not accept List subclasses.
Arguments pass from Flink's internal data to the external function and require that output
conversion. Results pass from the external function back to internal data and require the
input conversion.

`ArrayListConverter.toExternal` does construct an ArrayList, and its registry recognizes List
subclasses. Neither fact bypasses the logical type's conversion validation. Do not widen the
argument whitelist based on runtime materialization alone or patch an unreleased Flink API.

The independent ArrayList and LinkedList controls in
`concreteListArgumentsKeepReleasedPlanningRejection` exercise actual released SQL planning on
both supported lines. They require matching `ValidationException` causes, the output-conversion
diagnostic and planning phase, zero substitutions and no consumed native operators. The
native-enabled run records explicit fallback before the released validator rejects the job;
this is not an execution failure to turn into a native feature.

Reconsider this boundary when a released Flink version changes its argument conversion rule.
Other concrete/custom List result classes, alternate nested bridges and Lists of containers
remain tracked by https://github.com/datafusion-contrib/StreamFusion/issues/237.
