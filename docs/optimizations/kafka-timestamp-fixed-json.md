# Kafka timestamps: write fixed JSON syntax directly

**Applies to:** native JSON Kafka sink, `TIMESTAMP_LTZ` encoding

The exactly-once Kafka q19 profile showed `StrftimeItems::next` and `DelayedFormat::fmt` beneath the
native JSON serializer: every `TIMESTAMP_LTZ` row reparsed two fixed chrono patterns and paid the
generic formatting machinery for syntax that never changes.

## First stage: direct digit writes

The Kafka encoder now converts the epoch value to UTC components once and writes the fixed-width
date and time digits directly into arrow-json's reused output buffer. Fraction trimming and the
SQL/ISO separator remain byte-identical to the old formatter, pinned by a parity sweep across the
full nanosecond timestamp range, precisions 0–12, and both formats.

Measured with Criterion (`kafka_timestamp_sink/*`, 4096 values): 935.6 µs → 174.3 µs per batch,
**5.37× throughput** (4.38 → 23.49 Melem/s) and 81.4% less formatter time. The 50K-row
exactly-once Kafka profile loop improved q9 from 50 → 53 completed jobs in 60 seconds (+6.0%); q19
remained 27 jobs in both repeat runs, so no q19 end-to-end gain is claimed.

## Second stage: integer calendar arithmetic

A fresh profile after that change showed the remaining timestamp frame was chrono's epoch-to-calendar
conversion. The second stage replaces that conversion with integer Gregorian calendar arithmetic
before writing the same components. The retained three-way Criterion A/B measures generic chrono
formatting at 944.3 µs, chrono components plus direct digits at 172.9 µs, and integer calendar plus
direct digits at 116.1 µs per 4096 values. The second stage is another **1.49× throughput gain**
(32.8% less timestamp time); its q9 guard remained neutral at 53 jobs in 60 seconds.

See [Benchmarks](../benchmarks.md) for the profile-loop methodology behind the q9/q19 job counts.
