use super::*;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("interval processing clock: {message}"))
}

impl IntervalJoiner {
    pub(super) fn bind_clock(&mut self, arities: [usize; 2]) -> Result<(), DataFusionError> {
        if let Some(bound) = self.clock_arities {
            return if bound == arities {
                Ok(())
            } else {
                Err(invalid("layout changed"))
            };
        }
        for (schema, time, arity) in [
            (&self.left_data_schema, self.left_time, arities[0]),
            (&self.right_data_schema, self.right_time, arities[1]),
        ] {
            if schema.fields().len() != arity + 1
                || time != arity
                || schema.field(arity).data_type() != &DataType::Int64
            {
                return Err(invalid("cache timestamp must follow the original payload"));
            }
        }
        if let Some(predicate) = &mut self.predicate {
            if predicate.compiled.is_some() {
                return Err(invalid("binding must precede predicate execution"));
            }
            // Rex refs describe [original left, original right], before the left cache clock.
            for (kind, column) in predicate.kinds.iter().zip(&mut predicate.payload) {
                if *kind == 0 && *column >= arities[0] as i64 {
                    *column += 1;
                }
            }
        }
        self.clock_arities = Some(arities);
        Ok(())
    }

    pub(super) fn clock_input(
        &self,
        batch: RecordBatch,
        left: bool,
        now: Option<i64>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = if left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        };
        if let Some(arities) = self.clock_arities {
            let now = now.ok_or_else(|| invalid("bound input requires a processing clock"))?;
            let arity = arities[if left { 0 } else { 1 }];
            let logical = Arc::new(schema.project(&(0..arity).collect::<Vec<_>>())?);
            let data = crate::logical_input::insert_only(batch, &logical, "interval join")?;
            let mut columns = data.columns().to_vec();
            columns.push(Arc::new(Int64Array::from_value(now, data.num_rows())));
            return Ok(RecordBatch::try_new(schema.clone(), columns)?);
        }
        let batch = crate::logical_input::insert_only(batch, schema, "interval join")?;
        Ok(self.stamp(batch, left, now))
    }

    pub(super) fn user_output(&self, output: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let Some([left, right]) = self.clock_arities else {
            return Ok(output);
        };
        if output.num_columns() == 0 {
            return Ok(output);
        }
        if output.num_columns() != left + right + 2 {
            return Err(invalid("joined payload layout changed"));
        }
        Ok(output.project(
            &(0..left)
                .chain(left + 1..left + 1 + right)
                .collect::<Vec<_>>(),
        )?)
    }

    fn next_cleanup(&mut self) -> Result<Option<i64>, DataFusionError> {
        let mut next: Option<i64> = None;
        for left in [true, false] {
            self.visit_canonical_timers(left, |_, deadline| {
                next = Some(next.map_or(deadline, |old| old.min(deadline)));
                Ok(())
            })?;
        }
        Ok(next)
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindIntervalJoinCanonicalClock<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jint,
    right: jint,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        let result = if left < 0 || right < 0 {
            Err(invalid("negative payload arity"))
        } else {
            joiner.bind_clock([left as usize, right as usize])
        };
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_intervalJoinCleanupDeadline<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        match joiner.next_cleanup() {
            Ok(deadline) => {
                let values = deadline.into_iter().collect::<Vec<_>>();
                let result = env
                    .new_long_array(values.len() as i32)
                    .expect("cleanup deadline");
                env.set_long_array_region(&result, 0, &values)
                    .expect("cleanup deadline");
                result.into_raw()
            }
            Err(error) => {
                let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[cfg(test)]
mod tests;
