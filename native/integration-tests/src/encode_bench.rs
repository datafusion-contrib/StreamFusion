#![allow(dead_code)]
use base64::Engine;
use streamfusion_bridge::prelude::*;
use streamfusion_bridge::*;
use streamfusion_format_support::*;

pub fn encode_kafka_json(batch: &RecordBatch) -> EncodedLines {
    streamfusion_json::encode::encode_json_batch(batch, &Default::default(), &[], &[])
        .expect("JSON encode")
}
pub enum KafkaTimestampStrategy {
    ChronoFormat,
    ChronoComponents,
    DirectDigits,
}

pub fn encode_kafka_timestamps(values: &[i64], strategy: KafkaTimestampStrategy) -> usize {
    let mut output = Vec::with_capacity(32);
    let mut bytes = 0;
    for &value in values {
        output.clear();
        match strategy {
            KafkaTimestampStrategy::ChronoFormat => {
                encode_local_timestamp_chrono(value, 3, false, &mut output)
            }
            KafkaTimestampStrategy::ChronoComponents => {
                encode_local_timestamp_chrono_components(value, 3, false, &mut output)
            }
            KafkaTimestampStrategy::DirectDigits => {
                encode_local_timestamp(value, 3, false, &mut output)
            }
        }
        bytes += output.len();
    }
    bytes
}

pub struct FormatEncoder(Box<dyn MessageEncoder>);
impl FormatEncoder {
    pub fn json() -> Self {
        Self(streamfusion_json::encode::create_encoder(FORMAT_JSON, "").unwrap())
    }
    pub fn csv() -> Self {
        Self(streamfusion_csv::encode::create_encoder(FORMAT_CSV, "").unwrap())
    }
    pub fn raw(little: bool) -> Self {
        Self(
            streamfusion_raw::encode::create_encoder(
                FORMAT_RAW,
                if little {
                    "endianness=little-endian"
                } else {
                    "endianness=big-endian"
                },
            )
            .unwrap(),
        )
    }
    pub fn avro(schema: &str, confluent: bool) -> Self {
        let options = format!(
            "avro-schema={schema}{}",
            if confluent { "\nschema-id=1" } else { "" }
        );
        Self(
            streamfusion_avro::encode::create_encoder(
                if confluent {
                    FORMAT_AVRO_CONFLUENT
                } else {
                    FORMAT_AVRO
                },
                &options,
            )
            .unwrap(),
        )
    }
    pub fn protobuf(descriptor: &[u8]) -> Self {
        let options = format!(
            "descriptor={}\nmessage=criterion.Row\n",
            base64::engine::general_purpose::STANDARD.encode(descriptor)
        );
        Self(streamfusion_protobuf::encode::create_encoder(FORMAT_PROTOBUF, &options).unwrap())
    }
    pub fn encode(&self, batch: &RecordBatch) -> EncodedLines {
        self.0.encode(batch, None, &[], &[]).unwrap()
    }
}
