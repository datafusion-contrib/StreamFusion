# Preserve Flink string bytes at Arrow ingress

[StreamFusion #622](https://github.com/datafusion-contrib/StreamFusion/issues/622)
shows that a logical STRING can carry arbitrary bytes. Byte-oriented Flink functions can observe
these bytes directly. A row containing `FF` concatenated with `!` yields `FF21`.

[Comet's JVM-to-native import](https://github.com/apache/datafusion-comet/pull/5310) validates
imported strings and repairs malformed values with its replacement decoder. StreamFusion checks
the same invariant before constructing typed arrays and preserves arbitrary bytes using its
marked Binary representation. The planner selects Flink when a consumer requires a string
representation that the available producer and transport cannot guarantee.

The Arrow bridge checks the string leaves of imported data, including nested containers and
dictionary values. Arrow validates offsets and element boundaries, so adjacent byte slices each
have to be valid strings even when their combined buffer is valid UTF-8. Conforming buffers retain
their existing ownership and storage.
