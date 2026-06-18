# Route SQL filters to the native filter operator

**Status:** open (low priority)
**Source:** loose end — the native filter exists but is not reachable from SQL

## Problem
A native filter operator exists and is proven via JNI, but nothing in the
planner routes a SQL `WHERE` to it; it is dead weight relative to the SQL
path. The projection substitution shows the pattern; filter is the obvious
companion.

## Goal
Match a stream-physical filter (or the filter half of a Calc) and substitute a
native filter operator, with fallback for unsupported predicates.

## Acceptance criteria
- `SELECT ... WHERE value > k` routes its filter to native with identical
  results; other predicates fall back.

## Notes
- The current native filter is hard-coded to `> threshold` on a single int
  column; generalizing the predicate is part of this (extract the constant and
  column from the `RexProgram` condition, like the doubling-calc matcher does).
- Lower value than the aggregate work; filters are rarely the bottleneck. Do
  after the aggregation tickets unless a concrete need appears.
