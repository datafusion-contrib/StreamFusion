# Superseded canonical-export diagnostic

This interrupted sweep retains all 22 complete cells (88 validated pairs) measured
before profiling the q4 regression. It used two canonical savepoint exports per
trial. It is not the final Nexmark matrix and none of its cells is selected into
the final aggregates. The entire cohort is superseded by the uniform configured
checkpoint / draining native-savepoint protocol documented in the connector page.

The corresponding CPU, wall, GC and kernel-wait evidence is retained in
[the diagnostic directory](../iceberg-profile-20261017/README.md). All warmups,
unfavorable observations, plans and metadata remain available here.
