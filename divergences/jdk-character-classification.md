# Worker-JDK character classification in native expressions

Flink's string IS_ALPHA delegates to Commons Lang, which tests Java UTF-16 `char` values
with `Character.isLetter(char)`. Rust's Unicode properties can use a different Unicode
release and classify supplementary code points as letters even though Java's two surrogate
units both fail this function. A Rust alphabetic predicate is therefore not a compatible
replacement.

Comet's JNI bridge initializes JVM metadata once, confines attached-thread calls to a local
reference frame, and retains only the state needed by native execution. We use that lifecycle
for a process-local immutable property bitmap. During expression compilation, one bounded
JNI frame calls a helper in the worker JVM and copies its 65,536 letter bits into Rust.
The cached value contains no Java object, class or task/classloader reference. The planner's
schema probe may initialize its own bitmap; workers independently initialize theirs, so a
planner/worker JDK difference never serializes the wrong Unicode table into the expression.
There is no new JNI entry point or batch/row callback.

The native kernel reads Arrow UTF-8, accepts ASCII letters through a byte fast path, and
looks up other BMP characters in that bitmap. Supplementary characters fail immediately,
matching the UTF-16 loop. An isolated surrogate encoded as `?` also remains nonalphabetic;
the normalization cannot turn a failed string into an accepted one. Arbitrary byte-backed
STRING producers retain the existing fused-expression/boundary rules. Numeric and other
noncharacter overloads keep their generated implementation.

This differs from relying on an engine-bundled Unicode property table because Flink's
contract belongs to the JDK running that task. Correctness tests compare every BMP unit
with the released stock function on each supported Flink/JDK pair, alongside supplementary,
NULL, empty, conditional and multi-batch cases. Performance measurements and remaining
classification/casing gaps live in the Calc and scalar benchmark pages.
