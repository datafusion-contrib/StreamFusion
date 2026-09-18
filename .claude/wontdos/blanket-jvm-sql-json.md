# Replace every native SQL/JSON Calc with JVM evaluation

Rejected after the 2026-09-18 release experiment: whole-Calc JVM evaluation took
2.09–3.02 times the existing Rust route across simple and wildcard JSON_VALUE/JSON_EXISTS
workloads, including both row/Arrow transposes. Retain verified native encoding first;
use the batch JVM bridge for JSON coverage that native encoding declines.

The [benchmark report](../../docs/benchmarks/scalar-functions.md#sqljson-jvm-bridge-prototype-2026-09-18)
contains paired Flink controls, raw trials and reproduction steps. This decision does
not reject JVM coverage or finish issue #91. Revisit blanket routing only with measured
bridge improvements or a materially different workload.
