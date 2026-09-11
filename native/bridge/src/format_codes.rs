// The MessageDecoder/encode format-code protocol, mirroring FormatCodes.java. The codes are wire
// format on the JNI boundary — never renumber them. Connectors can dispatch on these codes
// without depending on any format implementation.
#[allow(dead_code)]
pub const FORMAT_JSON: i32 = 0;
#[allow(dead_code)]
pub const FORMAT_AVRO_CONFLUENT: i32 = 1;
#[allow(dead_code)]
pub const FORMAT_CSV: i32 = 2;
#[allow(dead_code)]
pub const FORMAT_RAW: i32 = 3;
#[allow(dead_code)]
pub const FORMAT_AVRO: i32 = 4;
// Protobuf decoders are built from their descriptor, rather than through the format-code factory,
// but the constant completes the mirrored protocol.
#[allow(dead_code)]
pub const FORMAT_PROTOBUF: i32 = 5;
#[allow(dead_code)]
pub const FORMAT_DEBEZIUM_JSON: i32 = 6;
#[allow(dead_code)]
pub const FORMAT_OGG_JSON: i32 = 7;
#[allow(dead_code)]
pub const FORMAT_MAXWELL_JSON: i32 = 8;
#[allow(dead_code)]
pub const FORMAT_CANAL_JSON: i32 = 9;
#[allow(dead_code)]
pub const FORMAT_DEBEZIUM_AVRO_CONFLUENT: i32 = 10;
