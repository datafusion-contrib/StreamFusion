use super::*;
use std::sync::Mutex;

struct SharedAccumulator {
    value: Arc<Mutex<Box<dyn Accumulator>>>,
    aggregate: Arc<WindowAggregate>,
    counted: bool,
    frozen_size: Option<usize>,
}

impl std::fmt::Debug for SharedAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CapturedAccumulator")
    }
}

impl SharedAccumulator {
    fn exclusive(&mut self) -> Result<&mut Box<dyn Accumulator>, DataFusionError> {
        if Arc::get_mut(&mut self.value).is_none() {
            let state = {
                let mut value = self.value.lock().unwrap_or_else(|error| error.into_inner());
                snapshot_accumulator_state(&mut value, || self.aggregate.create_accumulator())
            };
            let arrays = state
                .iter()
                .map(ScalarValue::to_array)
                .collect::<Result<Vec<_>, _>>()?;
            let mut copied = self.aggregate.create_accumulator();
            restore_accumulator_state(&mut copied, &arrays)?;
            self.value = Arc::new(Mutex::new(copied));
        }
        self.frozen_size = None;
        Ok(Arc::get_mut(&mut self.value)
            .expect("exclusive accumulator")
            .get_mut()
            .unwrap_or_else(|error| error.into_inner()))
    }
}

impl Accumulator for SharedAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.exclusive()?.update_batch(values)
    }
    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        self.exclusive()?.evaluate()
    }
    fn size(&self) -> usize {
        self.frozen_size.unwrap_or_else(|| {
            self.value
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .size()
        }) + wrapper_bytes()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        self.exclusive()?.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.exclusive()?.merge_batch(states)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.exclusive()?.retract_batch(values)
    }
    fn supports_retract_batch(&self) -> bool {
        self.value
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .supports_retract_batch()
    }
}

fn wrapper_bytes() -> usize {
    std::mem::size_of::<SharedAccumulator>()
        + std::mem::size_of::<Mutex<Box<dyn Accumulator>>>()
        + 2 * std::mem::size_of::<usize>()
}

pub(super) fn capture_growth(accumulator: &dyn Accumulator) -> usize {
    if (accumulator as &dyn std::any::Any).is::<SharedAccumulator>() {
        0
    } else {
        wrapper_bytes()
    }
}

pub(super) fn capture(
    accumulator: &mut Box<dyn Accumulator>,
    aggregate: &Arc<WindowAggregate>,
) -> Box<dyn Accumulator> {
    if let Some(shared) =
        (accumulator.as_mut() as &mut dyn std::any::Any).downcast_mut::<SharedAccumulator>()
    {
        let size = shared.size() - wrapper_bytes();
        shared.frozen_size = Some(size);
        return Box::new(SharedAccumulator {
            value: Arc::clone(&shared.value),
            aggregate: Arc::clone(&shared.aggregate),
            counted: shared.counted,
            frozen_size: Some(size),
        });
    }
    let size = accumulator.size();
    let counted = (accumulator.as_ref() as &dyn std::any::Any).is::<CountedDistinctAccumulator>();
    let value = std::mem::replace(accumulator, Box::<WindowCountAccumulator>::default());
    let value = Arc::new(Mutex::new(value));
    *accumulator = Box::new(SharedAccumulator {
        value: Arc::clone(&value),
        aggregate: Arc::clone(aggregate),
        counted,
        frozen_size: Some(size),
    });
    Box::new(SharedAccumulator {
        value,
        aggregate: Arc::clone(aggregate),
        counted,
        frozen_size: Some(size),
    })
}

pub(super) fn counted(accumulator: &dyn Accumulator) -> bool {
    (accumulator as &dyn std::any::Any)
        .downcast_ref::<SharedAccumulator>()
        .is_some_and(|shared| shared.counted)
}

pub(super) fn restore(
    accumulator: &mut dyn Accumulator,
    states: &[ArrayRef],
) -> Option<Result<(), DataFusionError>> {
    (accumulator as &mut dyn std::any::Any)
        .downcast_mut::<SharedAccumulator>()
        .map(|shared| restore_accumulator_state(shared.exclusive()?, states))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_capture_preserves_multiplicity_and_consuming_state() {
        for aggregate in [
            WindowAggregate::CountedDistinct(DataType::Int64),
            WindowAggregate::new(7, &DataType::Int64),
        ] {
            let aggregate = Arc::new(aggregate);
            let mut live = aggregate.create_accumulator();
            live.update_batch(&[Arc::new(Int64Array::from(vec![Some(3), Some(3), None]))])
                .unwrap();
            let mut first = capture(&mut live, &aggregate);
            live.update_batch(&[Arc::new(Int64Array::from(vec![7]))])
                .unwrap();
            let mut second = capture(&mut live, &aggregate);
            live.update_batch(&[Arc::new(Int64Array::from(vec![11]))])
                .unwrap();
            assert_eq!(live.evaluate().unwrap(), ScalarValue::Int64(Some(3)));
            drop(live);
            for _ in 0..2 {
                let first_state =
                    snapshot_accumulator_state(&mut first, || aggregate.create_accumulator());
                assert!(!first_state.is_empty());
                assert_eq!(first.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
                assert_eq!(second.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
            }
        }
    }

    #[test]
    fn wrapper_sizes_are_charged_once_and_frozen_peers_keep_their_estimates() {
        let aggregate = Arc::new(WindowAggregate::new(7, &DataType::Utf8));
        let mut live = aggregate.create_accumulator();
        let values = Arc::new(StringArray::from_iter_values(
            (0..513).map(|value| format!("value-{value}")),
        )) as ArrayRef;
        live.update_batch(&[values]).unwrap();
        let before = live.size();
        let growth = capture_growth(live.as_ref());
        assert!(growth > 0);
        let mut first = capture(&mut live, &aggregate);
        assert_eq!(live.size(), before + growth);
        assert_eq!(first.size(), before + growth);
        assert_eq!(capture_growth(live.as_ref()), 0);
        let mut second = capture(&mut live, &aggregate);
        let retained = second.size();
        snapshot_accumulator_state(&mut first, || aggregate.create_accumulator());
        assert_eq!(second.size(), retained);
        live.update_batch(&[Arc::new(StringArray::from(vec!["new"]))])
            .unwrap();
        assert_eq!(live.evaluate().unwrap(), ScalarValue::Int64(Some(514)));
        assert_eq!(second.size(), retained);
        assert_eq!(second.evaluate().unwrap(), ScalarValue::Int64(Some(513)));
        assert_eq!(first.evaluate().unwrap(), ScalarValue::Int64(Some(513)));
    }

    #[test]
    fn overlapping_accumulator_views_survive_live_mutation_and_release() {
        let aggregate = Arc::new(WindowAggregate::WrappingIntSum);
        let mut live = aggregate.create_accumulator();
        live.update_batch(&[Arc::new(Int32Array::from(vec![3]))])
            .unwrap();
        let mut first = capture(&mut live, &aggregate);
        live.update_batch(&[Arc::new(Int32Array::from(vec![7]))])
            .unwrap();
        let mut second = capture(&mut live, &aggregate);
        live.update_batch(&[Arc::new(Int32Array::from(vec![11]))])
            .unwrap();
        assert_eq!(second.evaluate().unwrap(), ScalarValue::Int32(Some(10)));
        drop(live);
        assert_eq!(first.evaluate().unwrap(), ScalarValue::Int32(Some(3)));
        assert_eq!(second.evaluate().unwrap(), ScalarValue::Int32(Some(10)));
    }
}

pub(super) fn raw(
    accumulator: &mut dyn Accumulator,
) -> Result<&mut dyn Accumulator, DataFusionError> {
    if (accumulator as &dyn std::any::Any).is::<SharedAccumulator>() {
        let shared = (accumulator as &mut dyn std::any::Any)
            .downcast_mut::<SharedAccumulator>()
            .unwrap();
        Ok(shared.exclusive()?.as_mut())
    } else {
        Ok(accumulator)
    }
}
