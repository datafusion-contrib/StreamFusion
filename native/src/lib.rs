pub(crate) use arrow::array::builder::{BooleanBuilder, PrimitiveBuilder, StringBuilder};
pub(crate) use arrow::array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampNanosecondType,
};
pub(crate) use arrow::array::{
    make_array, new_empty_array, new_null_array, Array, ArrayRef, BooleanArray, Decimal128Array,
    Float32Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt32Array,
};
pub(crate) use arrow::array::NullBufferBuilder;
pub(crate) use arrow::buffer::{OffsetBuffer, ScalarBuffer};
pub(crate) use arrow::compute::{concat_batches, filter_record_batch, take, SortOptions};
pub(crate) use arrow::datatypes::ArrowPrimitiveType;
pub(crate) use arrow::row::{OwnedRow, Row, RowConverter, Rows, SortField};
pub(crate) use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
pub(crate) use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
pub(crate) use datafusion::catalog::memory::MemorySourceConfig;
pub(crate) use datafusion::common::{DFSchema, DataFusionError, JoinSide, JoinType, NullEquality};
pub(crate) use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
pub(crate) use datafusion::execution::runtime_env::RuntimeEnvBuilder;
pub(crate) use datafusion::execution::TaskContext;
pub(crate) use datafusion::functions_aggregate::count::count_udaf;
pub(crate) use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
pub(crate) use datafusion::functions_aggregate::sum::sum_udaf;
pub(crate) use datafusion::logical_expr::execution_props::ExecutionProps;
pub(crate) use datafusion::logical_expr::{Accumulator, AggregateUDF, Operator};
pub(crate) use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
pub(crate) use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
pub(crate) use datafusion::physical_expr::expressions::{binary, col, lit, Column};
pub(crate) use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
pub(crate) use datafusion::physical_plan::collect;
pub(crate) use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
pub(crate) use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
pub(crate) use datafusion::prelude::{col as logical_col, lit as logical_lit, SessionContext};
pub(crate) use datafusion::scalar::ScalarValue;
pub(crate) use futures::StreamExt;
pub(crate) use jni::objects::{
    JByteArray, JClass, JDoubleArray, JIntArray, JLongArray, JObjectArray, JString,
};
pub(crate) use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
pub(crate) use jni::JNIEnv;
// ahash, not std's SipHash: every keyed hot loop in the crate hashes through these aliases, and
// the CPU profiles showed SipHash as a top cost wherever an operator missed the explicit swap
// (q18's keep-last dedup spent ~35% of its time in it). DoS-hardness is irrelevant for internal
// operator state, so the fast hash is the right crate-wide default.
pub(crate) use ahash::{HashMap, HashSet};
pub(crate) use std::collections::BTreeMap;
pub(crate) use std::sync::{Arc, Mutex, OnceLock};
pub(crate) use tokio::runtime::Runtime;

mod aggregates;
mod bridge;
mod calc;
mod changelog;
mod csv;
mod dedup;
mod exchange;
mod expr;
mod files;
mod flatten;
mod flink_text;
mod formats;
mod group_agg;
mod interval_join;
mod ipc;
mod join_common;
mod json;
mod kafka;
mod keys;
mod memory;
mod normalizer;
mod over_agg;
mod session_agg;
mod sorter;
mod temporal_join;
mod topn;
mod updating_join;
mod window_agg;
mod window_join;

// Flatten the crate namespace: every module starts with `use crate::*;`, so items cross module
// boundaries (and reach the tests via `super::*`) without per-module import lists. A self-contained
// operator's re-export is "unused" outside `cfg(test)`, hence the allow.
#[allow(unused_imports)]
pub(crate) use {
    aggregates::*, bridge::*, calc::*, changelog::*, dedup::*, exchange::*, expr::*, files::*,
    flatten::*, formats::*, group_agg::*, interval_join::*, ipc::*, join_common::*, json::*,
    kafka::*, keys::*, memory::*, normalizer::*, over_agg::*, session_agg::*, sorter::*,
    temporal_join::*, topn::*, updating_join::*, window_agg::*, window_join::*,
};

/// Thin wrappers exposing the engine hot paths to the Criterion benchmark harness, without leaking
/// the JNI internals or the Arrow-FFI plumbing. Not used by the JVM bridge.
pub mod bench;

#[cfg(test)]
mod tests;
