# SQL/JSON parsing

The native techniques below remain the first choice for verified expressions. SQL/JSON
Calcs that exceed native admission use Flink-generated evaluation through the batch JVM
bridge. The [comparison](../benchmarks/scalar-functions.md#sqljson-jvm-bridge-prototype-2026-09-18)
found a 2.09–3.02× elapsed-time cost for replacing the measured native fast paths, so they
are retained. See [coverage and bridge limits](../operators/calc-filter.md#sqljson-evaluation).

JSON_VALUE with non-throwing policies, non-throwing JSON_EXISTS policies and IS JSON use a shared
native reader with two parsing paths. The streaming
path borrows selected tokens and validates the first JSON document with Flink/Jackson rules.
The SIMD path uses the existing `simd-json` dependency for documents containing many short
members, where repeatedly scanning individual keys and values costs more than building a tape.

The reader reuses the mutable input buffer, structural scratch and tape across rows in a
batch. Selected decoded strings are copied directly into the Arrow builder. Object lookup
visits all members and retains the last match before descending, including duplicate ancestors.
The entire document is validated even when the requested member appears near the beginning.

A SIMD attempt requires eight colons within the first 256 bytes and fewer than 50,000 total
input bytes. A successful tape must contain at most 1000 nodes, with no floating-point values;
Unicode escapes and selected numbers use the streaming path. Those bounds preserve Jackson's
resource limits, BigDecimal spelling and UTF-16 escape behavior. Invalid SIMD input also goes
through the streaming parser, retaining Flink's first-document and trailing-content behavior.
These functions are admitted by default for their verified SQL shapes.
Consumers of STRING JSON_VALUE results use a fused Flink expression to preserve intermediate
UTF-16 identity; the native reader described here remains the direct-projection path. JSON-derived
strings crossing operator boundaries fall back as described in [Calc/filter](../operators/calc-filter.md#json_value).
A document rejected after tape construction is parsed again by the streaming path. The
multi-member measurements use string members; they do not establish an improvement for
workloads dominated by floating-point members or numeric selections.

Jackson's recycled input-buffer capacity is acquired once per batch through its released
Java API. Native parsing tracks the same UTF-16 input-length growth and returns the resulting
capacity to the recycler, including when a policy fails. This preserves Jackson's numeric
limit behavior at buffer crossings without sending documents or results through JNI. The
SIMD path updates this state too, so subsequent native or Flink parsing sees the same capacity.

Long padding strings and documents with many fields are separate benchmark workloads. SIMD
is useful for the latter; a large byte count alone does not predict a benefit. The benchmark
parameter `scalar.json.fields` adds that many short string members without changing the selected
path. Each JSON function is measured independently against Flink, including both transposes.
Final measurements and reproduction commands are on the
[scalar benchmark page](../benchmarks/scalar-functions.md#sqljson-measurements).

Comet's streaming `get_json_object` and `datafusion-functions-json`'s jiter lookup informed
the comparison. Their path selection and validation contracts differ from SQL/JSON in Flink;
see the [semantic note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/32-sql-json-definite-paths.md).

## Serialize selected JSON_QUERY containers

Verified final JSON_QUERY projections reuse the reader rather than materializing the entire
document in a JVM JSON tree. Compatible wide documents write the selected container directly
from the recycled SIMD tape. Documents outside that compatibility gate use the exact reader
to validate the first document, then retain only the selected subtree in a flat arena with
child/sibling indexes. Ordinary keys borrow their input span; escaped keys retain decoded
UTF-16 identity until serialization. Stable grouping keeps the last duplicate value and the
released provider's order: first insertion positions on 1.18, Java UTF-16 order on 2.2.

The initial recursive-tree implementation lost on wide and deep objects. Profiling exposed
per-container allocations, deduplication hashing and recursive destruction. The flat arena
removed those costs, but a 1.18 wide case still lost; a second profile showed duplicate
validation/parsing, sorting and arena growth. Reusing the compatible SIMD tape avoids that
second parse and tree construction. Both slower cohorts remain in the measurement record.
For a 1,024-row 2.2 wide-object allocation probe, Rust allocation calls fall from 104,509 to
1,170 and cumulative requested bytes from 12,020,122 to 2,200,356. This untimed diagnostic
excludes JVM allocations and does not measure retained memory.

The final release matrix improves all 40 prepared bulk cells over stock Flink by
1.35-3.79x and actual preceding execution by 1.56-3.78x; all ten tiny controls improve too.
Six complete million-row jobs retain JNI and both row/Arrow transposes. Native/stock elapsed
ranges from 0.757 to 0.960, and native/previous from 0.685 to 0.896. These results apply to
the measured workloads, rather than every JSON_QUERY shape. See the
[complete method, slower cohorts and raw observations](../benchmarks/scalar-functions.md#definite-json_query-2026-11-06).

No JVM JSON evaluator handles admitted documents/results, although per-batch JNI recycler
metadata synchronization remains. The [coverage gate](../operators/calc-filter.md#json_query)
retains generated evaluation for unverified paths, consumers, types and shared-reader
combinations. The shared-reader state mechanism below addresses the successful-value mismatch
tracked in [#608](https://github.com/datafusion-contrib/StreamFusion/issues/608).

## Share reader metadata without reordering parser history

Multiple unconditional reader projections over direct fields/literals acquire Jackson's
metadata once per Calc batch. Each reader receives the buffer capacity that Flink's
row/projection order would expose, calculated from the original UTF-16 document lengths.
The actual JSON kernels still evaluate Arrow columns. SQL NULLs and rejected rows do not
grow the buffer; empty survivor batches do not enter the metadata scope. A thread-local
guard clears the schedule during normal completion and unwinding, and the common owner
publishes the final capacity for later parsing.

Ordinary short documents cannot grow the recycled buffer. That common path checks their
stored byte widths and avoids UTF-16 rescanning and per-row capacity-table allocation.
Growing documents use one capacity vector per reader. This preserves the released numeric
token-limit behavior without invoking a JVM JSON evaluator or creating a JSON DOM for
the scheduling step. Computed/conditional readers, parser calls in the condition and JVM
evaluator combinations retain generated row-major evaluation on 2.2. The 1.18 profile lacks
the later token-limit interaction and retains its prior admission outside this native scope.

On the release comparison, 32 bulk cells beat stock by 1.56-9.02x and eight tiny cells by
1.21-3.25x. Bulk comparisons against the actual preceding implementation span 0.936-2.330x:
some previously native cases pay up to 6.9% more time for corrected history, while 2.2 QUERY
pairs remove generated evaluation. Six conversion-inclusive jobs all beat stock, but five
lose 0.2-2.3% to the preceding control. Keep these costs with the correctness motivation;
the technique is not a blanket throughput improvement. See the
[complete method, variability and raw observations](../benchmarks/scalar-functions.md#shared-json-reader-history-2026-11-06).
