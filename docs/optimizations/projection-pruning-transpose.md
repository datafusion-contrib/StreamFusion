# Projection pruning into the entry transpose

**Applies to:** the RowData→Arrow entry transpose

## What it is

When a native calc reads only a few columns or nested struct fields of a wide row, the planner
narrows the entry transpose to exactly those leaves and remaps the calc accordingly. The unread
person/auction structs of the Nexmark wide event are never materialized into Arrow at all
(`8523187`).

## The pass-through bug

Pass-through columnar nodes must not hide the rowwise input from this pruning pass. The mini-batch
assigner sitting between a calc and the source silently disabled it: an unpruned transpose measured
at 7x the transpose work, and native q3 ran 2.4x slower with mini-batch on. The fix pushes the
pruning through the assigner (`ddc4f25`); a planner test pins the pruned arity.

## General rule

Any future pass-through columnar rel needs the same treatment — it must not opaquely block
projection pruning from reaching the transpose on its far side.
