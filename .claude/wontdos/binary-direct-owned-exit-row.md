# Direct allocation of an owned binary exit row

Reject this as a strategy for eliminating the final output copy. This is a
released-API inspection, not a measured implementation or a closure of #235.

The existing profile exposes both Arrow fixed-binary getter arrays and
`BinaryRowData.copy`. Allocating a fresh final binary row and copying Arrow
payload directly into its storage initially appears to remove both copies.
However, released Flink 2.2.1 and 1.18.1 `CopyingChainingOutput.pushToOperator`
invoke `serializer.copy(record.getValue())` for every record. Existing ownership
does not bypass that copy when object reuse is disabled. A newly allocated row
would therefore still be copied at the host boundary.

The retained generated-segment experiment already removes the temporary getter
array while preserving the reusable writer and host ownership copy. A fresh
owned-row allocation would not eliminate a further payload copy under this
contract, and adds an allocation before Flink's copy. No fresh implementation
or timing result is claimed. Comet's native row converter returns borrowed rows
valid only until the next conversion; that lifetime cannot replace our owned
host-output contract.

Revisit only with new evidence for an ownership-aware host boundary that
preserves retained-row semantics and the normal production configuration.
Changing object-reuse settings or bypassing Flink's chained output is not proof
for the original workload. Fixed-length BINARY support remains pending under
[#235](https://github.com/datafusion-contrib/StreamFusion/issues/235).
