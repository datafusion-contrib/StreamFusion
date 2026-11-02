use super::*;

impl WindowJoiner {
    pub(super) fn logical_input(
        &self,
        batch: RecordBatch,
        left: bool,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = if left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        };
        crate::logical_input::insert_only(batch, schema, "window join")
    }
}
