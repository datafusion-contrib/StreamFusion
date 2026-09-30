//! Production post-decode normalization; JVM-backed file I/O is outside this boundary.
use arrow::{array::ArrayRef, datatypes::DataType};
use orc_rust::schema::DataType as OrcType;
pub fn normalize(input: &ArrayRef, physical: &OrcType, target: &DataType) -> ArrayRef {
    crate::reader::normalize(input, physical, target)
}
