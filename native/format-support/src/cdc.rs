use crate::*;

/// Which CDC changelog JSON dialect an envelope is in. The wire codec is plain JSON either way; the
/// dialect fixes the two image fields, the operation field, the op-code → action mapping, and how the
/// pre-image is recovered (see `CdcShape`). Debezium/OGG and Maxwell are scalar (one image per message);
/// Canal (`data`/`old` arrays — a message fans out per element) is a follow-up.
#[derive(Clone, Copy)]
pub enum CdcDialect {
    /// Debezium JSON: `{before, after, op}`, op ∈ {`c`/`r` → insert, `u` → update, `d` → delete}.
    /// Mirrors `DebeziumJsonDeserializationSchema` (`r` is a snapshot read, treated as an insert).
    Debezium,
    /// Oracle GoldenGate JSON: `{before, after, op_type}`, op ∈ {`I` → insert, `U` → update,
    /// `D` → delete, `T` truncate → skipped}. Mirrors `OggJsonDeserializationSchema`.
    Ogg,
    /// Maxwell JSON: `{data, old, type}`, type ∈ {`insert`, `update`, `delete`}. `data` is the full
    /// post-image, `old` a *partial* pre-image (only changed fields); delete carries the row in `data`.
    /// Mirrors `MaxwellJsonDeserializationSchema`.
    Maxwell,
    /// Canal JSON: `{data, old, type}` where `data`/`old` are *arrays* of rows (one message fans out
    /// per element), type ∈ {`INSERT`, `UPDATE`, `DELETE`, `CREATE` (DDL → skipped)}. Same partial-`old`
    /// merge as Maxwell, applied per element pair. Mirrors `CanalJsonDeserializationSchema`.
    Canal,
}

/// A CDC envelope's change action, before fanning out to physical rows. An update emits two rows
/// (UPDATE_BEFORE + UPDATE_AFTER); insert/delete emit one.
pub enum CdcAction {
    Insert,
    Update,
    Delete,
}

impl CdcAction {
    pub fn name(&self) -> &'static str {
        match self {
            CdcAction::Insert => "INSERT",
            CdcAction::Update => "UPDATE",
            CdcAction::Delete => "DELETE",
        }
    }
}

/// What to do with one envelope row's operation. `Skip` is a deliberate no-op Flink also drops (Canal's
/// `CREATE` DDL); `Unknown` is an unrecognized op, which Flink *fails the job* on by default — we match
/// that (rather than silently dropping the row) so the result is identical, and only the planner's
/// fallback gate lets `ignore-parse-errors` tables (which skip) run on Flink instead.
pub enum CdcOp {
    Change(CdcAction),
    Skip,
    Unknown,
}

/// How a dialect lays out its pre/post images, which determines how UPDATE_BEFORE and DELETE rows are
/// built.
#[derive(Clone, Copy, PartialEq)]
pub enum CdcShape {
    /// Debezium/OGG: `before` is the full pre-image and `after` the full post-image. DELETE reads
    /// `before`; an update's UPDATE_BEFORE is `before` verbatim — a null `before` skips the record
    /// (Flink throws; we match `ignore-parse-errors`).
    BeforeAfter,
    /// Maxwell/Canal: `data` is the full post-image and `old` a *partial* pre-image (only changed
    /// fields). DELETE reads `data` (it holds the deleted row); an update's UPDATE_BEFORE reads a
    /// field from `old` when its KEY is present there — at ANY depth, since Flink's `findValue`
    /// searches the whole subtree: an explicit top-level null (a field changed *to* null) keeps
    /// the null, and so does a key found only inside a nested container (the top-level decode saw
    /// nothing) — and copies `data` only when the key appears nowhere. Reproduced by a per-message
    /// key scan of the raw `old` JSON (the decoded image alone can't distinguish present-null from
    /// absent).
    DataOld,
}

/// The fixed per-dialect envelope layout the decoder reads.
pub struct CdcSpec {
    /// JSON field holding the pre-image (`before` / `old`) — envelope column 0.
    pub before_field: &'static str,
    /// JSON field holding the post-image (`after` / `data`) — envelope column 1.
    pub after_field: &'static str,
    /// JSON field holding the operation — envelope column 2.
    pub op_field: &'static str,
    pub shape: CdcShape,
    /// Whether the images are JSON *arrays* of rows (Canal) rather than single rows: one message then
    /// fans out per element, pairing `data[i]` with `old[i]`.
    pub arrays: bool,
}

impl CdcDialect {
    /// The dialect a CDC format code selects — the only thing that differs between the CDC arms of
    /// [`MessageDecoder::new`].
    pub fn for_format(format: i32) -> CdcDialect {
        match format {
            FORMAT_DEBEZIUM_JSON => CdcDialect::Debezium,
            FORMAT_OGG_JSON => CdcDialect::Ogg,
            FORMAT_MAXWELL_JSON => CdcDialect::Maxwell,
            FORMAT_CANAL_JSON => CdcDialect::Canal,
            other => panic!("format code {other} is not a CDC format"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            CdcDialect::Debezium => "Debezium",
            CdcDialect::Ogg => "Ogg",
            CdcDialect::Maxwell => "Maxwell",
            CdcDialect::Canal => "Canal",
        }
    }

    pub fn spec(self) -> CdcSpec {
        match self {
            CdcDialect::Debezium => CdcSpec {
                before_field: "before",
                after_field: "after",
                op_field: "op",
                shape: CdcShape::BeforeAfter,
                arrays: false,
            },
            CdcDialect::Ogg => CdcSpec {
                before_field: "before",
                after_field: "after",
                op_field: "op_type",
                shape: CdcShape::BeforeAfter,
                arrays: false,
            },
            CdcDialect::Maxwell => CdcSpec {
                before_field: "old",
                after_field: "data",
                op_field: "type",
                shape: CdcShape::DataOld,
                arrays: false,
            },
            CdcDialect::Canal => CdcSpec {
                before_field: "old",
                after_field: "data",
                op_field: "type",
                shape: CdcShape::DataOld,
                arrays: true,
            },
        }
    }

    /// Classifies an op string. An unrecognized op is `Unknown` (Flink throws on it by default — see
    /// `CdcOp`); Canal's `CREATE` is a `Skip` (Flink drops DDL). Mirrors each `*JsonDeserializationSchema`.
    pub fn classify(self, op: &str) -> CdcOp {
        match self {
            CdcDialect::Debezium => match op {
                "c" | "r" => CdcOp::Change(CdcAction::Insert),
                "u" => CdcOp::Change(CdcAction::Update),
                "d" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown,
            },
            CdcDialect::Ogg => match op {
                "I" => CdcOp::Change(CdcAction::Insert),
                "U" => CdcOp::Change(CdcAction::Update),
                "D" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown, // including "T" truncate, which Flink treats as an unknown op
            },
            CdcDialect::Maxwell => match op {
                "insert" => CdcOp::Change(CdcAction::Insert),
                "update" => CdcOp::Change(CdcAction::Update),
                "delete" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown,
            },
            CdcDialect::Canal => match op {
                "INSERT" => CdcOp::Change(CdcAction::Insert),
                "UPDATE" => CdcOp::Change(CdcAction::Update),
                "DELETE" => CdcOp::Change(CdcAction::Delete),
                "CREATE" => CdcOp::Skip, // a DDL change event Flink drops
                _ => CdcOp::Unknown,
            },
        }
    }
}

/// Appends the output row(s) for one change-event unit (one envelope row, or one array element for
/// Canal): `before_idx`/`after_idx` are the rows to read in the pre/post-image struct arrays (equal for
/// scalar dialects; distinct flattened indices for Canal). An update fans out to UPDATE_BEFORE +
/// UPDATE_AFTER. A null image where the dialect requires one fails the job, exactly where Flink's
/// deserializer hits a NullPointerException and reports a corrupt message: a null pre-image on a
/// `BeforeAfter` update/delete (the REPLICA IDENTITY case), a null `old` on a `DataOld` update, and a
/// null row wherever the emitted row would read from.
#[allow(clippy::too_many_arguments)]
pub fn cdc_emit(
    action: &CdcAction,
    before_idx: usize,
    after_idx: usize,
    shape: CdcShape,
    old_presence: u128,
    before: &StructArray,
    after: &StructArray,
    out: &mut Vec<(i8, usize, usize, RowSource)>,
) {
    let require = |image: &StructArray, idx: usize, name: &str| {
        if !image.is_valid(idx) {
            panic!(
                "CDC {action} has a null {name} image",
                action = action.name()
            );
        }
    };
    match action {
        CdcAction::Insert => {
            require(after, after_idx, "post");
            out.push((0, before_idx, after_idx, RowSource::After));
        }
        CdcAction::Update => {
            let before_source = match shape {
                CdcShape::BeforeAfter => {
                    require(
                        before,
                        before_idx,
                        "\"before\"/pre (REPLICA IDENTITY not FULL?)",
                    );
                    RowSource::Before
                }
                CdcShape::DataOld => {
                    require(before, before_idx, "\"old\"/pre");
                    RowSource::Coalesce(old_presence)
                }
            };
            require(after, after_idx, "post");
            out.push((1, before_idx, after_idx, before_source));
            out.push((2, before_idx, after_idx, RowSource::After));
        }
        CdcAction::Delete => match shape {
            CdcShape::BeforeAfter => {
                require(
                    before,
                    before_idx,
                    "\"before\"/pre (REPLICA IDENTITY not FULL?)",
                );
                out.push((3, before_idx, after_idx, RowSource::Before));
            }
            // Maxwell/Canal: the deleted row lives in the post-image (`data`).
            CdcShape::DataOld => {
                require(after, after_idx, "post");
                out.push((3, before_idx, after_idx, RowSource::After));
            }
        },
    }
}

/// Which image an output row reads its columns from. `Coalesce` (Maxwell/Canal UPDATE_BEFORE) reads a
/// field from the pre-image when its key appears anywhere under the message's `old` (Flink's
/// recursive `findValue` presence rule — an explicit null is present, a key absent from the whole
/// subtree copies the post-image) — a per-field choice, so it can't share one gather index across
/// columns. The bitmask holds bit `i` for physical field `i` present in `old` (the arity is capped
/// at 128, enforced at construction).
#[derive(Clone, Copy)]
pub enum RowSource {
    /// The pre-image (`before` / `old`), envelope column 0.
    Before,
    /// The post-image (`after` / `data`), envelope column 1.
    After,
    /// Per field: pre-image where its key is present in `old`, else post-image.
    Coalesce(u128),
}

/// message's `old` — so the gather index is built per field.
pub fn gather_cdc_batch(
    out_rows: &[(i8, usize, usize, RowSource)],
    before: &StructArray,
    after: &StructArray,
    arity: usize,
    output: &SchemaRef,
) -> RecordBatch {
    const BEFORE: usize = 0;
    const AFTER: usize = 1;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arity + 1);
    for field in 0..arity {
        let before_child = before.column(field);
        let after_child = after.column(field);
        let indices: Vec<(usize, usize)> = out_rows
            .iter()
            .map(|&(_, before_idx, after_idx, source)| match source {
                RowSource::Before => (BEFORE, before_idx),
                RowSource::After => (AFTER, after_idx),
                RowSource::Coalesce(present) => {
                    if present & (1u128 << field) != 0 {
                        (BEFORE, before_idx)
                    } else {
                        (AFTER, after_idx)
                    }
                }
            })
            .collect();
        let sources = [before_child.as_ref(), after_child.as_ref()];
        columns.push(
            arrow::compute::interleave(&sources, &indices).expect("failed to gather CDC column"),
        );
    }
    columns.push(Arc::new(Int8Array::from(
        out_rows
            .iter()
            .map(|&(kind, _, _, _)| kind)
            .collect::<Vec<i8>>(),
    )));
    RecordBatch::try_new(output.clone(), columns).expect("failed to build CDC batch")
}
