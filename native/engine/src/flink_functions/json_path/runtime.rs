use super::Reader;
use crate::bridge::JVM;
use arrow::array::{Array, ArrayRef, StringArray, StructArray};
use datafusion::common::{exec_datafusion_err, Result};
use datafusion::logical_expr::ColumnarValue;
use jni::objects::JValue;
use std::cell::RefCell;

thread_local! {
    static SCHEDULE: RefCell<Option<Schedule>> = const { RefCell::new(None) };
}

struct Schedule {
    initial: usize,
    legacy: bool,
    next: usize,
    capacities: Vec<Option<Vec<usize>>>,
}

struct ScheduleGuard;

impl Drop for ScheduleGuard {
    fn drop(&mut self) {
        SCHEDULE.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(crate) fn scheduled() -> bool {
    SCHEDULE.with(|slot| slot.borrow().is_some())
}

/// Only unconditional direct-input projection readers may enter this scope. The planner
/// retains generated execution for conditional readers and for calls into the JVM.
pub(crate) fn with_projection_readers<T>(
    inputs: &[ColumnarValue],
    rows: usize,
    evaluate: impl FnOnce() -> T,
) -> Result<T> {
    let inputs = inputs
        .iter()
        .map(|input| input.to_array(rows))
        .collect::<Result<Vec<_>>>()?;
    let result = with_reader(|reader| {
        let (schedule, final_capacity) = prepare(&inputs, rows, reader)?;
        SCHEDULE.with(|slot| {
            assert!(slot.borrow().is_none(), "nested SQL/JSON reader schedule");
            *slot.borrow_mut() = Some(schedule);
        });
        let guard = ScheduleGuard;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(evaluate));
        if result.is_ok() {
            SCHEDULE.with(|slot| {
                let state = slot.borrow();
                let state = state.as_ref().unwrap();
                assert_eq!(
                    state.next,
                    state.capacities.len(),
                    "SQL/JSON reader schedule width"
                );
            });
        }
        drop(guard);
        // The common JNI owner publishes once, including when projection evaluation fails.
        reader.set_buffer_size(final_capacity);
        Ok(result)
    })?;
    match result {
        Ok(value) => Ok(value),
        Err(failure) => std::panic::resume_unwind(failure),
    }
}

struct Documents<'a> {
    input: &'a ArrayRef,
    text: &'a StringArray,
    cache: Option<streamfusion_bridge::JavaStringCacheColumn<'a>>,
}

impl Documents<'_> {
    fn length(&self, row: usize, exact: bool) -> Option<usize> {
        if self.input.is_null(row) || self.text.is_null(row) {
            return None;
        }
        if let Some(units) = self.cache.as_ref().and_then(|cache| cache.value(row)) {
            return Some(units.len());
        }
        let text = self.text.value(row);
        Some(if exact {
            text.encode_utf16().take(32769).count()
        } else {
            text.len()
        })
    }
}

fn prepare(inputs: &[ArrayRef], rows: usize, reader: &Reader) -> Result<(Schedule, usize)> {
    let documents = inputs
        .iter()
        .map(|input| {
            let represented = input.as_any().downcast_ref::<StructArray>();
            Ok(Documents {
                input,
                text: datafusion::common::cast::as_string_array(
                    represented.map(|value| value.column(0)).unwrap_or(input),
                )?,
                cache: represented
                    .map(|value| streamfusion_bridge::JavaStringCacheColumn::new(value.column(1))),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let initial = reader.buffer_size();
    let mut state = Schedule {
        initial,
        legacy: reader.legacy_semantics(),
        next: 0,
        capacities: (0..inputs.len()).map(|_| None).collect(),
    };
    // Short ordinary documents cannot grow the recycler. Avoid per-row state allocation
    // and UTF-16 scans on that common path.
    if initial >= 32768
        || documents.iter().all(|input| {
            (0..rows).all(|row| {
                input
                    .length(row, false)
                    .is_none_or(|length| length <= initial)
            })
        })
    {
        return Ok((state, initial));
    }
    for capacities in &mut state.capacities {
        *capacities = Some(Vec::with_capacity(rows));
    }
    let mut capacity = initial;
    for row in 0..rows {
        for (index, input) in documents.iter().enumerate() {
            state.capacities[index].as_mut().unwrap().push(capacity);
            if let Some(length) = input.length(row, true).filter(|&length| length <= 32768) {
                capacity = capacity.max(length);
            }
        }
    }
    Ok((state, capacity))
}

/// Hold Jackson's recycled input buffer for a batch, then publish the capacity reached by
/// the native reader. No document or result crosses JNI; both parsers share the same state.
pub(crate) fn with_reader<T>(evaluate: impl FnOnce(&mut Reader) -> Result<T>) -> Result<T> {
    let scheduled = SCHEDULE.with(|slot| {
        let mut state = slot.borrow_mut();
        state.as_mut().map(|state| {
            let capacities = state
                .capacities
                .get_mut(state.next)
                .expect("unexpected SQL/JSON reader inside a scheduled Calc")
                .take();
            state.next += 1;
            Reader::new(state.initial)
                .with_legacy_semantics(state.legacy)
                .with_row_buffers(capacities)
        })
    });
    if let Some(mut reader) = scheduled {
        return evaluate(&mut reader);
    }
    // Standalone Rust tests supply the same initial state as a fresh Jackson recycler.
    #[cfg(test)]
    if JVM.get().is_none() {
        return evaluate(&mut Reader::new(4000));
    }

    let vm = JVM
        .get()
        .ok_or_else(|| exec_datafusion_err!("JVM not captured for SQL/JSON runtime"))?;
    let mut env = vm
        .attach_current_thread()
        .map_err(|e| exec_datafusion_err!("SQL/JSON runtime: {e}"))?;
    env.with_local_frame(4, |env| -> jni::errors::Result<_> {
        let runtime = env.new_object("tech/streamfusion/operator/NativeJsonRuntime", "()V", &[])?;
        let size = env.call_method(&runtime, "bufferSize", "()I", &[])?.i()?;
        let legacy = env
            .call_method(&runtime, "legacySemantics", "()Z", &[])?
            .z()?;
        let mut reader = Reader::new(size as usize).with_legacy_semantics(legacy);
        let result = evaluate(&mut reader);
        // Publish growth even if an ON ERROR/EMPTY policy fails the expression.
        env.call_method(
            &runtime,
            "release",
            "(I)V",
            &[JValue::Int(reader.buffer_size() as i32)],
        )?;
        Ok(result)
    })
    .map_err(|e| exec_datafusion_err!("SQL/JSON runtime: {e}"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink_functions::json_path::{Path, Value};
    use std::sync::Arc;

    fn boundary() -> String {
        let number = format!("1.{}", "2".repeat(1000));
        let prefix = "{\"a\":\"ok\",\"padding\":\"";
        let before = format!("{prefix}{}\",\"n\":", "x".repeat(3990 - prefix.len() - 6));
        let tail = ",\"tail\":\"";
        format!(
            "{before}{number}{tail}{}\"}}",
            "x".repeat(37502 - before.len() - number.len() - tail.len() - 2)
        )
    }

    #[test]
    fn columns_observe_the_same_capacity_as_row_major_readers() {
        let left = ["{}".to_owned(), format!("\"{}\"", "x".repeat(16000))];
        let right = [boundary(), "{}".into()];
        let path = Path::parse("strict $.a", "13.0").unwrap();
        let inputs = [&left, &right]
            .map(|values| ColumnarValue::Array(Arc::new(StringArray::from(values.to_vec()))));
        let mut expected = [Vec::new(), Vec::new()];
        let mut reader = Reader::new(4000);
        for row in 0..2 {
            for (index, values) in [&left, &right].into_iter().enumerate() {
                expected[index].push(matches!(
                    reader.read(&path, &values[row]),
                    Ok(Value::String("ok"))
                ));
            }
        }
        assert!(expected[1][0]);
        let actual = with_projection_readers(&inputs, 2, || {
            [&left, &right].map(|values| {
                with_reader(|reader| {
                    Ok(values
                        .iter()
                        .enumerate()
                        .map(|(row, value)| {
                            reader.begin_row(row);
                            matches!(reader.read(&path, value), Ok(Value::String("ok")))
                        })
                        .collect::<Vec<_>>())
                })
                .unwrap()
            })
        })
        .unwrap();
        assert_eq!(actual, expected);
        assert!(!scheduled());
    }

    #[test]
    fn ordinary_documents_avoid_per_row_capacity_tables() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("{}"), None]));
        let (state, final_capacity) =
            prepare(&[input.clone(), input], 2, &Reader::new(4000)).unwrap();
        assert_eq!(final_capacity, 4000);
        assert!(state.capacities.iter().all(Option::is_none));
    }

    #[test]
    fn failed_evaluation_does_not_leak_a_schedule_into_the_next_calc() {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec!["{}", "null"])));
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ =
                with_projection_readers(&[input.clone(), input], 2, || panic!("projection failed"));
        }));
        assert!(failure.is_err());
        assert!(!scheduled());
        assert_eq!(
            with_reader(|reader| Ok(reader.buffer_size())).unwrap(),
            4000
        );
    }
}
