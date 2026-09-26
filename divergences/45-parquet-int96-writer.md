# INT96 columns beside Arrow column encoders

Comet uses parquet-rs' ArrowWriter for columnar Parquet output. StreamFusion keeps that path for
INT64 files. The released parquet-rs ArrowWriter cannot encode INT96, which is the default timestamp
encoding of Flink 1.18's filesystem writer. Requiring a different format option leaves otherwise
supported legacy jobs on Flink.

For INT96 files we use the released SerializedFileWriter and ArrowRowGroupWriterFactory APIs.
Ordinary leaves keep the standard Arrow column encoders; only timestamp leaves use the typed
INT96 writer. A recursive timestamp walker supplies definition and repetition levels for nullable
structs, lists and maps. Flink's millisecond/fraction pair converts directly to the twelve physical
bytes using Java's division/remainder behavior, including dates before the epoch. No fork, private
Arrow API, Java row conversion or replacement filesystem/commit protocol is introduced.

The adapter bounds row groups by the configured byte threshold, including pending timestamp values
and levels, checked every 1,024 rows. JVM output ownership and the existing bounded JNI output bridge
are unchanged. Local-time INT96 uses one JVM callback per timestamp column for Flink's exact
calendar conversion. The callback captures the defining Parquet class in a JNI global reference,
borrows the calling JVM thread and bounds its local references with a local frame, following
Comet's JNI ownership separation. The Rust encoder owns the resulting physical bytes; Java keeps
filesystem ownership. Local-time INT64 remains outside admission.
