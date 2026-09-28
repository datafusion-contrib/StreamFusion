//! Paimon level-0 reducers. Like paimon-rust's sort_merge and pick-value aggregators, unchanged
//! cells retain indices into Arrow columns. Only arithmetic and concatenation materialize values.
//! The reducer shortcut and field iteration follow released Java Paimon 2.0.
//! The legacy adapter selects released 1.0 numeric overflow and retraction semantics.
use super::*;
#[cfg(test)]
use arrow::array::Float64Array;
use arrow::compute::interleave;
use serde::Deserialize;
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Engine {
    #[default]
    Deduplicate,
    FirstRow,
    PartialUpdate,
    Aggregation,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub(super) struct Options {
    pub engine: Engine,
    pub sequence_columns: Vec<usize>,
    pub sequence_ascending: bool,
    pub rowkind_column: Option<usize>,
    pub ignore_update_before: bool,
    pub fields: Vec<FieldOptions>,
    pub remove_on_delete: bool,
    pub remove_on_sequence_group: Vec<usize>,
    #[serde(skip)]
    pub host_aggregates: Vec<Option<jni::objects::GlobalRef>>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            engine: Engine::Deduplicate,
            sequence_columns: vec![],
            sequence_ascending: true,
            rowkind_column: None,
            ignore_update_before: false,
            fields: vec![],
            remove_on_delete: false,
            remove_on_sequence_group: vec![],
            host_aggregates: vec![],
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FieldOptions {
    function: Option<String>,
    ignore_retract: bool,
    sequence_group: Vec<usize>,
    delimiter: String,
    #[serde(default)]
    distinct: bool,
    #[serde(default)]
    host: bool,
    #[serde(default)]
    legacy_numeric: bool,
}

impl Options {
    pub fn row_kinds(&self, batch: RecordBatch, kind_column: usize) -> RecordBatch {
        let Some(column) = self.rowkind_column else {
            return batch;
        };
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("rowkind.field string");
        let kinds: Int8Array = values
            .iter()
            .map(|value| match value {
                Some("+I") => 0,
                Some("-U") => 1,
                Some("+U") => 2,
                Some("-D") => 3,
                Some(other) => panic!("Unsupported row kind: {other}"),
                None => panic!("Row kind cannot be null."),
            })
            .collect();
        let mut columns = batch.columns().to_vec();
        columns[kind_column] = Arc::new(kinds);
        RecordBatch::try_new(batch.schema(), columns).expect("replace row kinds")
    }

    pub fn user_sequences(&self, batch: &RecordBatch) -> Option<Rows> {
        if self.sequence_columns.is_empty() {
            return None;
        }
        let columns: Vec<ArrayRef> = self
            .sequence_columns
            .iter()
            .map(|&i| streamfusion_bridge::ordering::canonical_ordering_column(batch.column(i)))
            .collect();
        let converter = RowConverter::new(
            columns
                .iter()
                .map(|column| {
                    SortField::new_with_options(
                        column.data_type().clone(),
                        arrow::compute::SortOptions {
                            descending: !self.sequence_ascending,
                            nulls_first: true,
                        },
                    )
                })
                .collect(),
        )
        .expect("sequence comparator");
        Some(
            converter
                .convert_columns(&columns)
                .expect("encode user sequence"),
        )
    }
}

#[derive(Clone, Debug)]
enum Cell {
    Null,
    Input(usize),
    Value(ScalarValue),
    Host(usize),
}

impl Cell {
    fn input(column: &ArrayRef, row: usize) -> Self {
        if column.is_null(row) {
            Self::Null
        } else {
            Self::Input(row)
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null) || matches!(self, Self::Value(value) if value.is_null())
    }

    fn scalar(&self, column: &ArrayRef) -> ScalarValue {
        match self {
            Self::Null => ScalarValue::try_from(column.data_type()).expect("typed null"),
            Self::Input(row) => ScalarValue::try_from_array(column, *row).expect("aggregate input"),
            Self::Value(value) => value.clone(),
            Self::Host(_) => panic!("host aggregate cannot be used as a sequence field"),
        }
    }
}

pub(super) struct Reducer<'a> {
    options: &'a Options,
    batch: &'a RecordBatch,
    kind_column: usize,
    cells: Vec<Vec<Cell>>,
    kinds: Vec<i8>,
    host_programs: Vec<Vec<i32>>,
}

impl<'a> Reducer<'a> {
    pub fn new(options: &'a Options, batch: &'a RecordBatch, kind_column: usize) -> Self {
        Self {
            options,
            batch,
            kind_column,
            cells: vec![vec![]; kind_column],
            kinds: vec![],
            host_programs: vec![vec![]; kind_column],
        }
    }

    pub fn add_group(&mut self, rows: &[usize]) {
        let kinds = self
            .batch
            .column(self.kind_column)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("row kinds");
        // Paimon's ReducerMergeFunctionWrapper passes a singleton through, including its row kind.
        if rows.len() == 1 {
            for (i, cells) in self.cells.iter_mut().enumerate() {
                cells.push(Cell::input(self.batch.column(i), rows[0]));
            }
            self.kinds.push(kinds.value(rows[0]));
            return;
        }
        let mut current = vec![Cell::Null; self.kind_column];
        let mut initialized = vec![false; self.kind_column];
        let mut meet_insert = false;
        let mut filled = false;
        let mut deleted = false;
        let partial = self.options.engine == Engine::PartialUpdate;
        let grouped = self
            .options
            .fields
            .iter()
            .any(|f| !f.sequence_group.is_empty());
        for &row in rows {
            let kind = kinds.value(row);
            let retract = is_retract(kind);
            deleted = false;
            if partial && retract && !filled {
                self.copy_row(&mut current, row);
                filled = true;
            }
            if self.options.remove_on_delete && kind == DELETE {
                deleted = true;
                self.copy_row(&mut current, row);
                continue;
            }
            if partial && retract && !grouped {
                assert!(
                    self.options.remove_on_delete,
                    "Partial update cannot accept delete records without ignore-delete, remove-record-on-delete or sequence groups"
                );
                continue;
            }
            let mut processed = vec![false; self.kind_column];
            for i in 0..self.kind_column {
                let field = &self.options.fields[i];
                let input = Cell::input(self.batch.column(i), row);
                if partial && !field.sequence_group.is_empty() {
                    if processed[i] {
                        continue;
                    }
                    let group = &field.sequence_group;
                    if group.iter().all(|&s| self.batch.column(s).is_null(row)) {
                        for &s in group {
                            processed[s] = true;
                        }
                        continue;
                    }
                    let order = group
                        .iter()
                        .map(|&s| {
                            compare(
                                &Cell::input(self.batch.column(s), row),
                                &current[s],
                                self.batch.column(s),
                            )
                        })
                        .find(|order| !order.is_eq())
                        .unwrap_or(Ordering::Equal);
                    if order.is_ge() && group.contains(&i) {
                        if kind == DELETE
                            && group
                                .iter()
                                .any(|s| self.options.remove_on_sequence_group.contains(s))
                        {
                            deleted = true;
                            self.copy_row(&mut current, row);
                            break;
                        }
                        for &s in group {
                            current[s] = Cell::input(self.batch.column(s), row);
                            processed[s] = true;
                        }
                    } else if order.is_ge() || field.function.is_some() {
                        current[i] = if field.function.is_none() && retract {
                            Cell::Null
                        } else {
                            aggregate(
                                field,
                                &mut initialized[i],
                                current[i].clone(),
                                input,
                                self.batch.column(i),
                                retract,
                                order.is_lt(),
                                &mut self.host_programs[i],
                            )
                        };
                    }
                } else if !partial || !retract {
                    // Java ignores configured aggregators when partial-update has no sequence group.
                    current[i] = if partial && !grouped {
                        if input.is_null() {
                            current[i].clone()
                        } else {
                            input
                        }
                    } else {
                        aggregate(
                            field,
                            &mut initialized[i],
                            current[i].clone(),
                            input,
                            self.batch.column(i),
                            retract,
                            false,
                            &mut self.host_programs[i],
                        )
                    };
                }
            }
            if !retract {
                meet_insert = true;
                filled = true;
            }
        }
        self.kinds.push(if deleted || partial && !meet_insert {
            DELETE
        } else {
            0
        });
        for (column, cell) in self.cells.iter_mut().zip(current) {
            column.push(cell);
        }
    }

    fn copy_row(&self, current: &mut [Cell], row: usize) {
        for (i, cell) in current.iter_mut().enumerate() {
            *cell = Cell::input(self.batch.column(i), row);
        }
    }

    pub fn finish(self) -> RecordBatch {
        let mut columns: Vec<ArrayRef> = self
            .cells
            .into_iter()
            .zip(self.host_programs)
            .enumerate()
            .map(|(column, (cells, program))| {
                if program.is_empty() {
                    materialize(self.batch.column(column), cells)
                } else {
                    evaluate_host_column(
                        self.options.host_aggregates[column]
                            .as_ref()
                            .expect("host field kernel"),
                        self.batch.column(column),
                        program,
                        cells,
                    )
                }
            })
            .collect();
        columns.push(Arc::new(Int8Array::from(self.kinds)));
        RecordBatch::try_new(self.batch.schema(), columns).expect("merged value columns")
    }
}

fn materialize(input: &ArrayRef, cells: Vec<Cell>) -> ArrayRef {
    if cells.iter().all(|cell| !matches!(cell, Cell::Value(_))) {
        let indices: UInt32Array = cells
            .iter()
            .map(|cell| match cell {
                Cell::Input(row) => Some(*row as u32),
                _ => None,
            })
            .collect();
        return take(input, &indices, None).expect("gather merged cells");
    }
    let null = ScalarValue::try_from(input.data_type()).expect("typed null");
    let mut computed = vec![null];
    let indices: Vec<(usize, usize)> = cells
        .into_iter()
        .map(|cell| match cell {
            Cell::Null => (1, 0),
            Cell::Input(row) => (0, row),
            Cell::Host(_) => panic!("host field requires its column kernel"),
            Cell::Value(value) => {
                computed.push(value);
                (1, computed.len() - 1)
            }
        })
        .collect();
    let computed = ScalarValue::iter_to_array(computed).expect("aggregate values");
    interleave(&[input.as_ref(), computed.as_ref()], &indices).expect("merged column")
}

fn compare(left: &Cell, right: &Cell, column: &ArrayRef) -> Ordering {
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => match (left.scalar(column), right.scalar(column)) {
            (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
                canonical_f32(a).total_cmp(&canonical_f32(b))
            }
            (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
                canonical_f64(a).total_cmp(&canonical_f64(b))
            }
            (a, b) => a.partial_cmp(&b).expect("comparable merge field"),
        },
    }
}

fn canonical_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else {
        value
    }
}

fn canonical_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else {
        value
    }
}

fn aggregate(
    field: &FieldOptions,
    initialized: &mut bool,
    mut accumulator: Cell,
    mut input: Cell,
    column: &ArrayRef,
    retract: bool,
    reversed: bool,
    host_program: &mut Vec<i32>,
) -> Cell {
    let Some(function) = field.function.as_deref() else {
        return if field.sequence_group.is_empty() && input.is_null() {
            accumulator
        } else {
            input
        };
    };
    if retract && field.ignore_retract {
        return accumulator;
    }
    if field.host {
        let node = host_program.len() / 3;
        host_program.extend([
            host_reference(&accumulator),
            host_reference(&input),
            if retract {
                1
            } else if reversed {
                2
            } else {
                0
            },
        ]);
        return Cell::Host(node);
    }
    if reversed && !retract {
        std::mem::swap(&mut accumulator, &mut input);
    }
    if retract {
        match function {
            "primary-key" => return input,
            "last_value" => return Cell::Null,
            "last_non_null_value" => {
                return if input.is_null() {
                    accumulator
                } else {
                    Cell::Null
                };
            }
            "sum" | "product" => {
                return numeric(
                    function,
                    &accumulator,
                    &input,
                    column,
                    true,
                    field.legacy_numeric,
                );
            }
            _ => panic!(
                "Aggregate function '{function}' does not support retraction; configure fields.<field>.ignore-retract=true"
            ),
        }
    }
    match function {
        "primary-key" | "last_value" => input,
        "last_non_null_value" => {
            if input.is_null() {
                accumulator
            } else {
                input
            }
        }
        "first_value" | "first_non_null_value" | "first_not_null_value" => {
            if !*initialized && (function == "first_value" || !input.is_null()) {
                *initialized = true;
                input
            } else {
                accumulator
            }
        }
        "sum" | "product" => numeric(
            function,
            &accumulator,
            &input,
            column,
            false,
            field.legacy_numeric,
        ),
        "min" | "max" => {
            if accumulator.is_null() {
                return input;
            }
            if input.is_null() {
                return accumulator;
            }
            let order = compare(&accumulator, &input, column);
            if function == "min" && order.is_lt() || function == "max" && order.is_gt() {
                accumulator
            } else {
                input
            }
        }
        "bool_and" | "bool_or" => {
            if accumulator.is_null() {
                return input;
            }
            if input.is_null() {
                return accumulator;
            }
            let (ScalarValue::Boolean(Some(a)), ScalarValue::Boolean(Some(b))) =
                (accumulator.scalar(column), input.scalar(column))
            else {
                panic!("boolean aggregate");
            };
            Cell::Value(ScalarValue::Boolean(Some(if function == "bool_and" {
                a && b
            } else {
                a || b
            })))
        }
        "listagg" => {
            let a = accumulator.scalar(column);
            let b = input.scalar(column);
            let ScalarValue::Utf8(b) = b else {
                panic!("listagg string");
            };
            let Some(b) = b.filter(|value| !java_blank(value)) else {
                return accumulator;
            };
            let ScalarValue::Utf8(a) = a else {
                panic!("listagg string");
            };
            let Some(a) = a.filter(|value| !java_blank(value)) else {
                return input;
            };
            if field.distinct {
                let separator = if field.delimiter.is_empty() {
                    " "
                } else {
                    &field.delimiter
                };
                let mut existing: std::collections::HashSet<&str> = a.split(separator).collect();
                let mut result = a.clone();
                for token in b.split(separator) {
                    if !java_blank(token) && existing.insert(token) {
                        result.push_str(&field.delimiter);
                        result.push_str(token);
                    }
                }
                Cell::Value(ScalarValue::Utf8(Some(result)))
            } else {
                Cell::Value(ScalarValue::Utf8(Some(format!(
                    "{a}{}{b}",
                    field.delimiter
                ))))
            }
        }
        _ => panic!("unsupported native aggregate: {function}"),
    }
}

fn java_blank(value: &str) -> bool {
    value.chars().all(|c| matches!(c, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}'))
}

fn numeric(
    function: &str,
    accumulator: &Cell,
    input: &Cell,
    column: &ArrayRef,
    retract: bool,
    legacy: bool,
) -> Cell {
    if input.is_null() {
        return accumulator.clone();
    }
    if accumulator.is_null() && (!retract || function == "product") {
        return if retract && !legacy {
            accumulator.clone()
        } else {
            input.clone()
        };
    }
    let a = accumulator.scalar(column);
    let b = input.scalar(column);
    macro_rules! integer {
        ($variant:ident, $a:expr, $b:expr) => {{
            let a = $a.unwrap_or(0);
            // Paimon 1.0 uses Java primitive arithmetic; 2.0 checks overflow.
            if legacy {
                return Cell::Value(ScalarValue::$variant(Some(match (function, retract) {
                    ("sum", false) => a.wrapping_add($b),
                    ("sum", true) => a.wrapping_sub($b),
                    ("product", false) => a.wrapping_mul($b),
                    _ => a.wrapping_div($b),
                })));
            }
            let value = match (function, retract) {
                ("sum", false) => a.checked_add($b),
                ("sum", true) => a.checked_sub($b),
                ("product", false) => a.checked_mul($b),
                _ => a.checked_div($b),
            }
            .expect("Paimon aggregate integer overflow or division by zero");
            ScalarValue::$variant(Some(value))
        }};
    }
    macro_rules! float {
        ($variant:ident, $a:expr, $b:expr) => {{
            let original = $a;
            let a = original.unwrap_or(0.0);
            ScalarValue::$variant(Some(match (function, retract) {
                ("sum", false) => a + $b,
                ("sum", true) if original.is_none() => -$b,
                ("sum", true) => a - $b,
                ("product", false) => a * $b,
                _ => a / $b,
            }))
        }};
    }
    let result = match (a, b) {
        (ScalarValue::Int8(a), ScalarValue::Int8(Some(b))) => integer!(Int8, a, b),
        (ScalarValue::Int16(a), ScalarValue::Int16(Some(b))) => integer!(Int16, a, b),
        (ScalarValue::Int32(a), ScalarValue::Int32(Some(b))) => integer!(Int32, a, b),
        (ScalarValue::Int64(a), ScalarValue::Int64(Some(b))) => integer!(Int64, a, b),
        (ScalarValue::Float32(a), ScalarValue::Float32(Some(b))) => float!(Float32, a, b),
        (ScalarValue::Float64(a), ScalarValue::Float64(Some(b))) => float!(Float64, a, b),
        (ScalarValue::Decimal128(a, p, s), ScalarValue::Decimal128(Some(b), _, _)) => {
            if function == "product" {
                return Cell::Value(ScalarValue::Decimal128(
                    decimal_product(a.unwrap(), b, p, s, retract),
                    p,
                    s,
                ));
            }
            let value = if retract {
                a.unwrap_or(0).checked_sub(b)
            } else {
                a.unwrap_or(0).checked_add(b)
            };
            // DecimalUtils uses an unchecked-precision compact constructor when long arithmetic
            // succeeds; only its BigDecimal fallback checks the declared precision.
            let compact = p <= 18 && a.is_some() && value.is_some_and(|v| i64::try_from(v).is_ok());
            ScalarValue::Decimal128(
                value.filter(|v| compact || v.unsigned_abs() < 10_u128.pow(p as u32)),
                p,
                s,
            )
        }
        _ => panic!("unsupported native numeric aggregate type"),
    };
    Cell::Value(result)
}

fn decimal_product(a: i128, b: i128, precision: u8, scale: i8, retract: bool) -> Option<i128> {
    use num_bigint::BigInt;
    use num_traits::{One, Signed, Zero};
    if !retract {
        return rescale_half_up(
            BigInt::from(a) * BigInt::from(b),
            2 * i64::from(scale),
            precision,
            scale,
        );
    }
    assert_ne!(b, 0, "Division by zero");
    let mut gcd = BigInt::from(a).abs();
    let mut remainder = BigInt::from(b).abs();
    while !remainder.is_zero() {
        let next = gcd % &remainder;
        gcd = remainder;
        remainder = next;
    }
    let mut denominator = BigInt::from(b).abs() / gcd;
    let mut twos = 0;
    let mut fives = 0;
    for (factor, count) in [(2, &mut twos), (5, &mut fives)] {
        let factor = BigInt::from(factor);
        while (&denominator % &factor).is_zero() {
            denominator /= &factor;
            *count += 1;
        }
    }
    assert!(
        denominator.is_one(),
        "Non-terminating decimal expansion; no exact representable decimal result."
    );
    let quotient_scale = twos.max(fives);
    let quotient = BigInt::from(a) * BigInt::from(10).pow(quotient_scale) / BigInt::from(b);
    rescale_half_up(quotient, i64::from(quotient_scale), precision, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_numeric_wraps_while_current_numeric_checks_overflow() {
        macro_rules! check {
            ($variant:ident, $type:ty) => {{
                let column: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![0]));
                let cell = |v| Cell::Value(ScalarValue::$variant(Some(v)));
                let max = <$type>::MAX;
                let min = <$type>::MIN;
                for (function, a, b, retract, expected) in [
                    ("sum", max, 1, false, min),
                    ("sum", min, 1, true, max),
                    ("product", max, 2, false, -2),
                    ("product", min, -1, true, min),
                ] {
                    assert_eq!(
                        numeric(function, &cell(a), &cell(b), &column, retract, true)
                            .scalar(&column),
                        ScalarValue::$variant(Some(expected))
                    );
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| numeric(
                            function,
                            &cell(a),
                            &cell(b),
                            &column,
                            retract,
                            false
                        )))
                        .is_err()
                    );
                }
                assert_eq!(
                    numeric("product", &Cell::Null, &cell(5), &column, true, true).scalar(&column),
                    ScalarValue::$variant(Some(5))
                );
                assert!(numeric("product", &Cell::Null, &cell(5), &column, true, false).is_null());
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| numeric(
                        "product",
                        &cell(5),
                        &cell(0),
                        &column,
                        true,
                        true
                    )))
                    .is_err()
                );
            }};
        }
        check!(Int8, i8);
        check!(Int16, i16);
        check!(Int32, i32);
        check!(Int64, i64);
    }

    #[test]
    fn decimal_products_round_half_up_check_precision_and_require_exact_division() {
        assert_eq!(decimal_product(125, 102, 6, 2, false), Some(128));
        assert_eq!(decimal_product(-125, 102, 6, 2, false), Some(-128));
        assert_eq!(decimal_product(999999, 200, 6, 2, false), None);
        assert_eq!(decimal_product(125, 200, 6, 2, true), Some(63));
        assert_eq!(decimal_product(0, 300, 6, 2, true), Some(0));
        assert!(std::panic::catch_unwind(|| decimal_product(100, 300, 6, 2, true)).is_err());
        assert_eq!(
            decimal_product(10_i128.pow(37), 10, 38, 2, false),
            Some(10_i128.pow(36))
        );
    }

    #[test]
    fn floating_comparisons_canonicalize_nan_but_preserve_signed_zero() {
        let column: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::from_bits(0xfff8000000000001),
            -0.0,
            0.0,
            f64::INFINITY,
            f64::NAN,
        ]));
        assert_eq!(
            compare(&Cell::Input(0), &Cell::Input(4), &column),
            Ordering::Equal
        );
        assert_eq!(
            compare(&Cell::Input(1), &Cell::Input(2), &column),
            Ordering::Less
        );
        assert_eq!(
            compare(&Cell::Input(0), &Cell::Input(3), &column),
            Ordering::Greater
        );
        let options = Options {
            sequence_columns: vec![0],
            ..Default::default()
        };
        let batch = RecordBatch::try_from_iter(vec![("seq", column)]).unwrap();
        let rows = options.user_sequences(&batch).unwrap();
        assert_eq!(rows.row(0), rows.row(4));
        assert!(rows.row(1) < rows.row(2));
    }
}

// Negative references address input rows (-row-2), -1 denotes NULL, and nonnegative
// references address earlier aggregate results. A column crosses JNI once per flush.
fn host_reference(cell: &Cell) -> i32 {
    match cell {
        Cell::Null => -1,
        Cell::Input(row) => -i32::try_from(*row).expect("input row index") - 2,
        Cell::Host(node) => i32::try_from(*node).expect("aggregate program index"),
        Cell::Value(_) => panic!("mixed host and native aggregate for one field"),
    }
}

fn evaluate_host_column(
    kernel: &jni::objects::GlobalRef,
    column: &ArrayRef,
    program: Vec<i32>,
    cells: Vec<Cell>,
) -> ArrayRef {
    use jni::objects::JValue;
    use streamfusion_bridge::bridge::JavaException;
    let mut env = streamfusion_bridge::bridge::JVM
        .get()
        .expect("JVM captured")
        .attach_current_thread()
        .expect("attach host field kernel");
    env.with_local_frame(
        8,
        |env| -> jni::errors::Result<Result<ArrayRef, JavaException>> {
            let mut input_array = FFI_ArrowArray::empty();
            let mut input_schema = FFI_ArrowSchema::empty();
            let mut output_array = FFI_ArrowArray::empty();
            let mut output_schema = FFI_ArrowSchema::empty();
            let input_address = &mut input_array as *mut _ as jlong;
            let input_schema_address = &mut input_schema as *mut _ as jlong;
            let output_address = &mut output_array as *mut _ as jlong;
            let output_schema_address = &mut output_schema as *mut _ as jlong;
            export_record_batch(
                RecordBatch::try_from_iter(vec![("value", column.clone())])
                    .expect("host aggregate input"),
                input_address,
                input_schema_address,
            );
            let instructions = env.new_int_array(program.len() as i32)?;
            env.set_int_array_region(&instructions, 0, &program)?;
            let references: Vec<i32> = cells.iter().map(host_reference).collect();
            let outputs = env.new_int_array(references.len() as i32)?;
            env.set_int_array_region(&outputs, 0, &references)?;
            let called = env.call_method(
                kernel.as_obj(),
                "merge",
                "(JJJJ[I[I)V",
                &[
                    JValue::Long(input_address),
                    JValue::Long(input_schema_address),
                    JValue::Long(output_address),
                    JValue::Long(output_schema_address),
                    JValue::Object(&instructions),
                    JValue::Object(&outputs),
                ],
            );
            if matches!(called, Err(jni::errors::Error::JavaException)) {
                let throwable = env.exception_occurred()?;
                env.exception_clear()?;
                return Ok(Err(JavaException(env.new_global_ref(throwable)?)));
            }
            called?;
            let batch = import_record_batch(output_address, output_schema_address);
            assert_eq!(batch.num_rows(), references.len());
            Ok(Ok(batch.column(0).clone()))
        },
    )
    .expect("Paimon field aggregate")
    .unwrap_or_else(|failure| std::panic::resume_unwind(Box::new(failure)))
}
