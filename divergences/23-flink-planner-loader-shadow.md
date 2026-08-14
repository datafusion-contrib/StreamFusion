# 23 — A temporary planner-loader shadow, not a new Flink planner fork

## What Comet and Flink normally do

Comet uses Spark's public extension points to add its optimizer rules after Spark has assembled the
session planner. Flink 2.2 has no equivalent public hook for an external distribution to append a
planner program stage. Its `PlannerModule` loads the private planner JAR through an isolated
component classloader and discovers the stock `DefaultPlannerFactory`.

## What we do instead

The StreamFusion distribution places a small JAR first in `$FLINK_HOME/lib` that shadows only
`org.apache.flink.table.planner.loader.PlannerModule`. It preserves Flink's component-classloader
model, extracts Flink's original planner JAR, and adds an embedded StreamFusion runtime payload in
front of it. The shim instantiates `StreamFusionPlannerFactory`; that factory adds the native
physical-plan scan stage for streaming mode and immediately delegates construction to Flink's own
`DefaultPlannerFactory`.

The runtime payload is also installed as a normal Flink library. Task code therefore sees the same
native runtime package as the planner component, while the payload embedded in the loader keeps the
planner classloader self-contained.

## Why deviate

This is a deployment bridge while the upstream extension API is unavailable. It is much narrower
than a planner fork: StreamFusion does not copy Flink's planner, execution nodes, or optimizer;
Flink still performs all standard planning and execution. The only injected behavior is the native
scan stage, after which unsupported plan shapes retain their normal Flink nodes.

The cost is a version-sensitive private-class seam. The shim supports exactly the tested Flink
**2.2.0 and 2.2.1** planner ABIs and fails closed for unknown or unversioned artifacts. It
rejects incompatible packaged versions at startup. A public upstream planner-extension API would
replace this file and remove the class-name shadow.
