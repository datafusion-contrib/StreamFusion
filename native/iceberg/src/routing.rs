use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::take;
use std::collections::{BTreeMap, VecDeque};

pub(crate) struct Routing {
    pub batches: VecDeque<(i32, RecordBatch)>,
}

impl Routing {
    pub(crate) fn new(batch: RecordBatch, destinations: &[i32]) -> Self {
        assert_eq!(
            batch.num_rows(),
            destinations.len(),
            "Iceberg routing length mismatch"
        );
        let mut groups = BTreeMap::<i32, Vec<u32>>::new();
        for (row, destination) in destinations.iter().copied().enumerate() {
            assert!(destination >= 0, "Invalid Iceberg destination");
            groups.entry(destination).or_default().push(row as u32);
        }
        let batches = groups
            .into_iter()
            .map(|(destination, rows)| {
                let contiguous = rows.windows(2).all(|w| w[1] == w[0] + 1);
                let indices = UInt32Array::from(rows.clone());
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if contiguous {
                            column.slice(rows[0] as usize, rows.len())
                        } else {
                            take(column, &indices, None).expect("Iceberg batch routing")
                        }
                    })
                    .collect();
                let selected =
                    RecordBatch::try_new(batch.schema(), columns).expect("Iceberg routed batch");
                (destination, selected)
            })
            .collect();
        Self { batches }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn routes_keep_schema_nulls_and_original_order_per_destination() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as arrow::array::ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("d"),
                    None,
                ])) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap();
        let mut routing = Routing::new(batch.clone(), &[7, 2, 7, 2, 7]);
        let (destination, selected) = routing.batches.pop_front().unwrap();
        assert_eq!(destination, 2);
        assert_eq!(selected.schema(), batch.schema());
        let values = selected
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(2), Some(4)]);
        let (destination, selected) = routing.batches.pop_front().unwrap();
        assert_eq!(destination, 7);
        let values = selected
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(5)]
        );
        assert!(routing.batches.is_empty());
        let routing = Routing::new(batch.clone(), &[4; 5]);
        assert_eq!(
            routing.batches[0].1.columns()[0].to_data().buffers()[0].as_ptr(),
            batch.columns()[0].to_data().buffers()[0].as_ptr(),
            "A contiguous destination retains Arrow buffers"
        );
    }
}
