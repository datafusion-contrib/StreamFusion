# All native operators are columnar; changelog operators converted

**Applies to:** GROUP BY aggregate, updating join, Top-N

A row-fed native operator forces a transpose on every batch even inside an all-native chain, so
Arrow-in/Arrow-out is the standing rule and the row-fed GROUP BY / updating join / Top-N variants
were deleted once columnar ones existed.

The whole-query all-or-nothing gate then guarantees no interior row↔Arrow round-trip ever survives
planning: a query accelerates as one columnar island or runs as stock Flink.
