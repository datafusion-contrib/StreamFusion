package tech.streamfusion;

/**
 * The Nexmark state-backend comparison, run from the one module whose classpath holds both the
 * Paimon backend and its Java table maintainer. With the compactor deployed, state tables carry
 * deletion vectors and compact synchronously at every barrier — the production configuration the
 * comparison claims to measure; the runtime-module copy of this benchmark refuses to run without
 * it. Enable with {@code SF_BENCHMARK=true SF_MATRIX_STATE_BACKENDS=true} and select
 * {@code NexmarkStateBackendBenchmark#stateBackendComparison}.
 */
class NexmarkStateBackendBenchmark extends NexmarkMatrixBenchmark {}
