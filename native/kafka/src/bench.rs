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
