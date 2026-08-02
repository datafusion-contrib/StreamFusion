# Watermark assigner

**Status:** native whenever it can help.

The watermark assigner has no admission conditions of its own beyond one placement rule: it is
substituted only when its input is **already** a columnar producer.

If the input is still row-wise, the assigner is left on the host on purpose — substituting it there
would just insert a transpose immediately followed by another transpose back, a pure round-trip
with no work done natively in between. That's a no-op, not a real fallback: nothing about the
watermark logic itself is unsupported, and the moment an upstream operator in the same query starts
producing Arrow batches, the assigner joins the native island with it.
