use crate::*;
/// The production Kafka JSON encoder, exposed only to its Criterion benchmark.
pub fn encode_kafka_json(batch: &RecordBatch) -> crate::kafka::EncodedLines {
    encode_json_batch(batch, &JsonEncodeOptions::default(), &[], &[]).expect("Kafka JSON encode")
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

enum Encoder {
    Json,
    Csv(crate::csv_encode::CsvEncodeOptions),
    Raw(crate::raw_encode::RawEncodeOptions),
    Avro(crate::avro::AvroEncodeOptions),
    Protobuf(crate::protobuf_encode::ProtobufEncoder),
}
pub struct FormatEncoder(Encoder);
impl Encoder {
    pub fn json() -> Self {
        Self::Json
    }
    pub fn csv() -> Self {
        Self::Csv(crate::csv_encode::CsvEncodeOptions::default())
    }
    pub fn raw(little: bool) -> Self {
        Self::Raw(
            crate::raw_encode::RawEncodeOptions::parse(if little {
                "endianness=little-endian"
            } else {
                "endianness=big-endian"
            })
            .unwrap(),
        )
    }
    pub fn avro(schema: &str, confluent: bool) -> Self {
        let config = format!(
            "avro-schema={schema}{}",
            if confluent { "\nschema-id=1" } else { "" }
        );
        Self::Avro(crate::avro::AvroEncodeOptions::parse(&config, confluent).unwrap())
    }
    pub fn protobuf(descriptor: &[u8]) -> Self {
        Self::Protobuf(crate::protobuf_encode::ProtobufEncoder::new(
            descriptor,
            "criterion.Row",
            "",
        ))
    }
    pub fn encode(&self, batch: &RecordBatch) -> crate::kafka::EncodedLines {
        match self {
            Self::Json => encode_kafka_json(batch),
            Self::Csv(options) => {
                crate::csv_encode::encode_csv_batch(batch, options, &[], &[]).unwrap()
            }
            Self::Raw(options) => {
                let (bytes, lines) = crate::raw_encode::encode_raw_batch(batch, options).unwrap();
                crate::kafka::EncodedLines::new(bytes, lines)
            }
            Self::Avro(options) => {
                crate::avro::encode_avro_batch(batch, options, &[], &[]).unwrap()
            }
            Self::Protobuf(encoder) => {
                let (bytes, lines) = encoder.encode(batch).into_parts();
                crate::kafka::EncodedLines::new(bytes, lines)
            }
        }
    }
}

impl FormatEncoder {
    pub fn json() -> Self {
        Self(Encoder::json())
    }
    pub fn csv() -> Self {
        Self(Encoder::csv())
    }
    pub fn raw(little: bool) -> Self {
        Self(Encoder::raw(little))
    }
    pub fn avro(schema: &str, confluent: bool) -> Self {
        Self(Encoder::avro(schema, confluent))
    }
    pub fn protobuf(descriptor: &[u8]) -> Self {
        Self(Encoder::protobuf(descriptor))
    }
    pub fn encode(&self, batch: &RecordBatch) -> crate::kafka::EncodedLines {
        self.0.encode(batch)
    }
}
