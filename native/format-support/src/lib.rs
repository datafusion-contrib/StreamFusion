#[allow(unused_imports)]
use streamfusion_bridge::prelude::*;
#[allow(unused_imports)]
use streamfusion_bridge::{self as bridge, flink_text, json_string, *};

mod raw;
pub use raw::*;
mod cdc;
pub use cdc::*;
mod options;
pub use options::*;
mod keyed;
pub use keyed::*;
mod decoder;
pub use decoder::*;
mod jni;
pub use jni::*;
mod facade;

#[cfg(test)]
mod tests;
