# String (and general-typed) grouping keys

**Status:** open — design ready
**Source:** ticket 04; the highest-value remaining grouping gap

## Why
`GROUP BY` on string dimensions (category, region, status, …) is ubiquitous.
Keys are currently bigint/int, carried as an `i64` composite. Strings don't fit
that, so this generalizes the key to a typed value.

## Native
Generalize the composite group key from `Vec<i64>` to **`Vec<ScalarValue>`** — the
same DataFusion scalar used everywhere else, which is `Hash + Eq` (usable as a
`HashMap` key) and covers int/bigint/string (and more later) uniformly.

- Replace the stored `key_arity: usize` with `key_types: Vec<DataType>` (the Arrow
  types of the key columns), captured on the first update from the gathered key
  arrays. `flush`/`flush_partial`/`snapshot` build the `key0..key{n-1}` columns
  with those types; `restore`/`update_partial` recover them from the batch.
- `key_arrays` gathers the key columns as generic `ArrayRef`; `read_key` reads one
  row's key as `Vec<ScalarValue>` via `ScalarValue::try_from_array`; `key_columns`
  transposes back, one typed array per position (`iter_to_array`, empty case via
  the stored type).
- Sorting in `flush` for determinism becomes a best-effort `partial_cmp` (the
  parity harness sorts results anyway).

## JVM
- Extend the key-type codes with `TYPE_STRING`. `keyTypes(...)` maps a `VARCHAR`
  output column to it.
- The key vectors become mixed: `updateRaw` builds a `VarCharVector` for a string
  key (from `RowData.getString(col).toBytes()`) and a `BigIntVector` for an
  int/bigint key (widened). `emitFinal` / the local emit read each key vector by
  type and box it — `StringData.fromBytes(...)` for string, Long/Integer for the
  integer family.
- The global operator reads the intermediate's string key columns likewise
  (`getString`), and rebuilds them for the native merge input.
- Matcher: accept `VARCHAR`/`CHAR` grouping keys in addition to bigint/int.

## Acceptance criteria
- `GROUP BY s, window_start, window_end` (string key) and a mixed
  `GROUP BY s, k` (string + bigint) route to native with identical results to the
  host, on tumbling, session, and two-phase tumbling.
- Existing bigint/int-keyed and unkeyed queries stay green (they are the
  integer-family case of the same path).

## Notes
- `Vec<ScalarValue>` subsumes the current `Vec<i64>`: int/bigint keys become
  `ScalarValue::Int64`/`Int32`-ish entries. This is the general key the
  byte-encoded alternative would also serve, but typed scalars keep the emit
  round-trip simple (each value knows how to rebuild its column).
- Decimal/other key types then come almost for free (matcher gate + JVM vector),
  but are out of scope here.
