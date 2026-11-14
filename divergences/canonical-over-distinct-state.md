# Released OVER DISTINCT membership projection

Arroyo does not provide Flink's independent DISTINCT MapView schema. Reuse StreamFusion's existing
unbounded native DISTINCT fold and membership visitors, and bind them to the released Flink
accumulator, map view and external serializers. Comet's execution utilities and ArrowImporter
supply the moved C Data ownership pattern; imported/exported roots are scoped before errors
propagate. No new JNI entry point or native checkpoint format is needed.

The initial Java binding is deliberately limited to required INT/BIGINT COUNT DISTINCT in
TTL-disabled unbounded processing-time ROWS/RANGE frames. Nullable arguments need stock's NULL
membership marker in native state; TTL needs independent scalar/view lifetimes. Those existing
native protocols and the wider event/bounded contracts require separate qualification under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).

The stock scalar and membership views validate together before any native insertion, retaining
source state for retry. Canonical capture visits live/frozen native state and does not create an
intermediate native checkpoint. The new both-release state/planner/process controls are authored
but remain unexecuted until the final local testing window; benchmarking is deferred.
