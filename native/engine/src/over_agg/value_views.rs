use super::string_extreme::StringExtreme;
use super::string_first_last::{StringFirstLast, TextValue};
use super::*;

#[derive(Clone)]
pub(super) enum ValueView {
    FirstLast(StringFirstLast),
    Extreme(StringExtreme),
    Distinct(DistinctSelection),
}

impl ValueView {
    pub fn new(kind: i64) -> Self {
        match kind {
            1 | 2 => Self::Extreme(StringExtreme::new(kind == 2)),
            5 | 6 => Self::FirstLast(StringFirstLast::new(kind == 6)),
            105 | 106 => Self::Distinct(DistinctSelection::new(kind == 106)),
            _ => unreachable!(),
        }
    }
    pub fn selected(&self) -> Option<&TextValue> {
        match self {
            Self::FirstLast(view) => view.selected(),
            Self::Extreme(view) => view.selected(),
            Self::Distinct(view) => view.selected().and_then(|value| value.text.as_ref()),
        }
    }
    pub fn set_persistent(&mut self, persistent: bool) {
        if let Self::Extreme(view) = self {
            view.set_persistent(persistent);
        }
    }
    pub fn copy_heap(&mut self) {
        if let Self::Extreme(view) = self {
            view.copy_heap();
        }
    }
    pub fn restore_canonical_order(&mut self) {
        if let Self::Extreme(view) = self {
            view.restore_canonical_order();
        }
    }
    pub fn add(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        match self {
            Self::FirstLast(view) => view.add(text, cache),
            Self::Extreme(view) => view.add(text, cache),
            Self::Distinct(view) => view.add_text(text, cache),
        }
    }
    pub fn retract(&mut self, text: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        match self {
            Self::FirstLast(view) => view.retract(text, cache),
            Self::Extreme(view) => view.retract(text, cache),
            Self::Distinct(view) => view.retract_text(text),
        }
    }
    pub fn cold(&mut self) {
        match self {
            Self::FirstLast(view) => view.cold(),
            Self::Extreme(view) => view.cold(),
            Self::Distinct(view) => {
                for value in &mut view.order {
                    if let Some(text) = &mut value.text {
                        text.cache = None;
                    }
                }
            }
        }
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::FirstLast(view) => view.bytes() - std::mem::size_of::<StringFirstLast>(),
                Self::Extreme(view) => view.bytes() - std::mem::size_of::<StringExtreme>(),
                Self::Distinct(view) => view.bytes() - std::mem::size_of::<DistinctSelection>(),
            }
    }
    pub fn snapshot(&self) -> Vec<u8> {
        match self {
            Self::FirstLast(view) => view.snapshot(),
            Self::Extreme(view) => view.snapshot(),
            Self::Distinct(view) => view.snapshot(),
        }
    }
    pub fn restore(kind: i64, bytes: &[u8]) -> Self {
        match kind {
            1 | 2 => Self::Extreme(StringExtreme::restore(kind == 2, bytes)),
            5 | 6 => Self::FirstLast(StringFirstLast::restore(kind == 6, bytes)),
            105 | 106 => Self::Distinct(DistinctSelection::restore(kind == 106, bytes)),
            _ => unreachable!(),
        }
    }

    pub fn add_num(&mut self, value: Num, data_type: &DataType) {
        let Self::Distinct(view) = self else {
            unreachable!("numeric retained selection")
        };
        view.add_scalar(num_to_scalar(data_type, Some(value)));
    }

    pub fn retract_num(&mut self, value: Num, data_type: &DataType) {
        let Self::Distinct(view) = self else {
            unreachable!("numeric retained selection")
        };
        view.retract_scalar(&num_to_scalar(data_type, Some(value)));
    }

    pub fn scalar(&self) -> Option<&ScalarValue> {
        let Self::Distinct(view) = self else {
            unreachable!("scalar retained selection")
        };
        view.selected().map(|value| &value.scalar)
    }
}

#[derive(Clone)]
struct SelectedValue {
    scalar: ScalarValue,
    text: Option<TextValue>,
}

/// Duplicate eviction changes membership only at zero; the original live arrival order survives.
pub(super) struct DistinctSelection {
    last: bool,
    value_type: DataType,
    counts: Option<DistinctSet>,
    order: std::collections::VecDeque<SelectedValue>,
}

impl Clone for DistinctSelection {
    fn clone(&self) -> Self {
        Self {
            last: self.last,
            value_type: self.value_type.clone(),
            counts: self.counts.as_ref().map(|counts| {
                let mut cloned = DistinctSet::new(&self.value_type);
                for (value, count) in counts.scalar_entries() {
                    cloned.insert_restored(value, count);
                }
                cloned
            }),
            order: self.order.clone(),
        }
    }
}

impl DistinctSelection {
    fn new(last: bool) -> Self {
        Self {
            last,
            value_type: DataType::Null,
            counts: None,
            order: Default::default(),
        }
    }

    fn counts(&mut self, data_type: DataType) -> &mut DistinctSet {
        if self.counts.is_none() {
            self.value_type = data_type;
            self.counts = Some(DistinctSet::new(&self.value_type));
        }
        self.counts.as_mut().unwrap()
    }

    fn selected(&self) -> Option<&SelectedValue> {
        if self.last {
            self.order.back()
        } else {
            self.order.front()
        }
    }

    fn add_scalar(&mut self, value: ScalarValue) {
        if self.counts(value.data_type()).add_scalar(value.clone()) {
            self.order.push_back(SelectedValue {
                scalar: value,
                text: None,
            });
        }
    }

    fn add_text(&mut self, value: &Arc<str>, cache: Option<&Arc<[u16]>>) {
        if self.counts(DataType::Utf8).change_string(value, false, 1) {
            self.order.push_back(SelectedValue {
                scalar: ScalarValue::Utf8(Some(value.to_string())),
                text: Some(TextValue {
                    text: value.clone(),
                    cache: cache.cloned(),
                }),
            });
        }
    }

    fn retract_scalar(&mut self, value: &ScalarValue) {
        if self
            .counts
            .as_mut()
            .is_some_and(|counts| counts.remove_scalar(value))
        {
            let position = self
                .order
                .iter()
                .position(|candidate| crate::flink_float::boxed_equal(&candidate.scalar, value))
                .expect("distinct selection retains every live membership");
            self.order.remove(position);
        }
    }

    fn retract_text(&mut self, value: &str) {
        if self
            .counts
            .as_mut()
            .is_some_and(|counts| counts.change_string(value, true, 1))
        {
            let position = self
                .order
                .iter()
                .position(|candidate| candidate.text.as_ref().unwrap().text.as_ref() == value)
                .expect("distinct text selection retains every live membership");
            self.order.remove(position);
        }
    }

    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.order.capacity() * std::mem::size_of::<SelectedValue>()
            + self.counts.as_ref().map_or(0, DistinctSet::capacity_bytes)
            + self
                .order
                .iter()
                .map(|value| {
                    2 * value.scalar.size()
                        + value.text.as_ref().map_or(0, |text| {
                            text.text.len() + text.cache.as_ref().map_or(0, |cache| cache.len() * 2)
                        })
                })
                .sum::<usize>()
    }

    fn snapshot(&self) -> Vec<u8> {
        let values = self
            .order
            .iter()
            .map(|value| value.scalar.clone())
            .collect();
        let counts = self
            .order
            .iter()
            .map(|value| self.counts.as_ref().unwrap().scalar_count(&value.scalar))
            .collect::<Vec<_>>();
        let caches = self
            .order
            .iter()
            .map(|value| {
                value
                    .text
                    .as_ref()
                    .and_then(|text| text.cache.as_ref())
                    .map(|cache| {
                        cache
                            .iter()
                            .flat_map(|unit| unit.to_le_bytes())
                            .collect::<Vec<_>>()
                    })
            })
            .collect::<Vec<_>>();
        write_ipc(
            &RecordBatch::try_from_iter(vec![
                ("value", scalars_to_array(values, &self.value_type)),
                ("count", Arc::new(Int64Array::from(counts)) as ArrayRef),
                (
                    "cache",
                    Arc::new(BinaryArray::from_iter(
                        caches.iter().map(|cache| cache.as_deref()),
                    )) as ArrayRef,
                ),
            ])
            .expect("retained distinct selection snapshot"),
        )
    }

    fn restore(last: bool, bytes: &[u8]) -> Self {
        let mut result = Self::new(last);
        for batch in read_ipc(bytes) {
            let values = batch.column(0);
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let caches = batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let scalar = ScalarValue::try_from_array(values, row).unwrap();
                result
                    .counts(values.data_type().clone())
                    .insert_restored(scalar.clone(), counts.value(row));
                let text = if let ScalarValue::Utf8(Some(text)) = &scalar {
                    Some(TextValue {
                        text: Arc::from(text.as_str()),
                        cache: (!caches.is_null(row)).then(|| {
                            Arc::from(
                                caches
                                    .value(row)
                                    .chunks_exact(2)
                                    .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
                                    .collect::<Vec<_>>(),
                            )
                        }),
                    })
                } else {
                    None
                };
                result.order.push_back(SelectedValue { scalar, text });
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_arrival_history_outlives_the_first_duplicate_and_survives_restore() {
        for last in [false, true] {
            let mut view = ValueView::new(if last { 106 } else { 105 });
            let ty = DataType::Date32;
            view.add_num(Num::I32(1), &ty);
            view.add_num(Num::I32(2), &ty);
            view.add_num(Num::I32(1), &ty);
            view.retract_num(Num::I32(1), &ty);
            let mut view = ValueView::restore(if last { 106 } else { 105 }, &view.snapshot());
            assert_eq!(
                view.scalar(),
                Some(&ScalarValue::Date32(Some(if last { 2 } else { 1 })))
            );
            view.retract_num(Num::I32(1), &ty);
            assert_eq!(view.scalar(), Some(&ScalarValue::Date32(Some(2))));
            view.add_num(Num::I32(1), &ty);
            assert_eq!(
                view.scalar(),
                Some(&ScalarValue::Date32(Some(if last { 1 } else { 2 })))
            );
        }
    }

    #[test]
    fn distinct_text_restore_keeps_multiplicity_and_the_original_java_cache() {
        let text: Arc<str> = Arc::from("?");
        let first: Arc<[u16]> = Arc::from([0xd800]);
        let second: Arc<[u16]> = Arc::from([0xdfff]);
        let other: Arc<str> = Arc::from("other");
        for kind in [105, 106] {
            let mut view = ValueView::new(kind);
            view.add(&text, Some(&first));
            view.add(&other, None);
            view.add(&text, Some(&second));
            view.retract(&text, None);
            let mut view = ValueView::restore(kind, &view.snapshot());
            if kind == 105 {
                assert_eq!(
                    view.selected().unwrap().cache.as_deref(),
                    Some(first.as_ref())
                );
            } else {
                assert_eq!(view.selected().unwrap().text.as_ref(), "other");
            }
            view.retract(&text, None);
            assert_eq!(view.selected().unwrap().text.as_ref(), "other");
        }
    }
}
