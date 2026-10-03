# Generated binary-row projection for Boolean arrays

Rejected on 2026-10-02 for the measured width-eight ARRAY_DISTINCT boundary. The
prototype adds ARRAY<BOOLEAN> to the existing generated exit projection and keeps
all ownership copies. It passes 26 Flink 2.2.1 ownership/SQL checks, but ten million
rows take 2.362853s versus stock 2.281326s and previous fallback 2.245811s.
The lower absolute time than an earlier candidate is accompanied by lower stock
controls and does not prove a win. All trials remain in the scalar-kernel ledger.

Code and prototype-only tests were removed. Do not repeat this exact projection
extension without a different measured cost reduction. Comet's generated
columnar-to-row projection was consulted before design. The rejection does not
exclude Boolean arrays, different boundary optimizations or the current exact-bit
kernel; issue #234 remains pending:
https://github.com/datafusion-contrib/StreamFusion/issues/234.
