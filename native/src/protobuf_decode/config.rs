// Derived from ptars 0.0.18's Apache-2.0 configuration API; retained with the decoder's ported
// tests so changes to its Arrow builder contract remain explicit.
use arrow::datatypes::TimeUnit;
use std::sync::Arc;

/// How to represent protobuf enum fields in Arrow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnumRepr {
    /// Store as Int32 (protobuf enum number). This is the default.
    #[default]
    Int32,
    /// Store as Utf8/LargeUtf8 (enum value name as string).
    String,
    /// Store as Binary/LargeBinary (enum value name as bytes).
    Binary,
}

/// Policy for stripping Confluent Schema Registry wire format prefix from messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConfluentWirePolicy {
    /// No prefix stripping — bytes are raw protobuf wire format. This is the default.
    #[default]
    Raw,
    /// Strip 5-byte Confluent header (1 magic byte + 4-byte schema ID).
    /// Use with Avro and JSON Schema.
    Standard,
    /// Strip 5-byte Confluent header plus varint-encoded message index array.
    /// Use with Protobuf schemas in Confluent Schema Registry.
    Protobuf,
}

/// Configuration for protobuf to Arrow conversions.
///
/// This struct allows customizing how protobuf types are mapped to Arrow types,
/// similar to Python's `ProtarrowConfig`.
#[derive(Debug, Clone)]
pub struct PtarsConfig {
    /// Timezone for timestamp values. Default: Some("UTC")
    /// Set to None for timezone-naive timestamps.
    pub timestamp_tz: Option<Arc<str>>,

    /// Time unit for timestamp values. Default: Nanosecond
    pub timestamp_unit: TimeUnit,

    /// Time unit for time of day values. Default: Nanosecond
    pub time_unit: TimeUnit,

    /// Time unit for duration values. Default: Nanosecond
    pub duration_unit: TimeUnit,

    /// Name for list item field. Default: "item"
    pub list_value_name: Arc<str>,

    /// Name for map value field. Default: "value"
    pub map_value_name: Arc<str>,

    /// Whether list fields can be null. Default: false
    pub list_nullable: bool,

    /// Whether map fields can be null. Default: false
    pub map_nullable: bool,

    /// Whether list element values can be null. Default: false
    pub list_value_nullable: bool,

    /// Whether map values can be null. Default: false
    pub map_value_nullable: bool,

    /// Whether to use LargeUtf8 instead of Utf8 for string fields. Default: false
    pub use_large_string: bool,

    /// Whether to use LargeBinary instead of Binary for bytes fields. Default: false
    pub use_large_binary: bool,

    /// Whether to use LargeList instead of List for repeated fields. Default: false
    pub use_large_list: bool,

    /// How to represent enum fields in Arrow. Default: Int32
    /// When String, use_large_string controls Utf8 vs LargeUtf8.
    /// When Binary, use_large_binary controls Binary vs LargeBinary.
    pub enum_repr: EnumRepr,

    /// Policy for stripping Confluent Schema Registry wire format prefix. Default: Raw
    pub confluent_wire_policy: ConfluentWirePolicy,
}

impl Default for PtarsConfig {
    fn default() -> Self {
        Self {
            timestamp_tz: Some(Arc::from("UTC")),
            timestamp_unit: TimeUnit::Nanosecond,
            time_unit: TimeUnit::Nanosecond,
            duration_unit: TimeUnit::Nanosecond,
            list_value_name: Arc::from("item"),
            map_value_name: Arc::from("value"),
            list_nullable: false,
            map_nullable: false,
            list_value_nullable: false,
            map_value_nullable: false,
            use_large_string: false,
            use_large_binary: false,
            use_large_list: false,
            enum_repr: EnumRepr::default(),
            confluent_wire_policy: ConfluentWirePolicy::default(),
        }
    }
}

impl PtarsConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timezone for timestamp values.
    pub fn with_timestamp_tz(mut self, tz: Option<&str>) -> Self {
        self.timestamp_tz = tz.map(Arc::from);
        self
    }

    /// Set the time unit for timestamp values.
    pub fn with_timestamp_unit(mut self, unit: TimeUnit) -> Self {
        self.timestamp_unit = unit;
        self
    }

    /// Set the time unit for time of day values.
    pub fn with_time_unit(mut self, unit: TimeUnit) -> Self {
        self.time_unit = unit;
        self
    }

    /// Set the time unit for duration values.
    pub fn with_duration_unit(mut self, unit: TimeUnit) -> Self {
        self.duration_unit = unit;
        self
    }

    /// Set the name for list item fields.
    pub fn with_list_value_name(mut self, name: &str) -> Self {
        self.list_value_name = Arc::from(name);
        self
    }

    /// Set the name for map value fields.
    pub fn with_map_value_name(mut self, name: &str) -> Self {
        self.map_value_name = Arc::from(name);
        self
    }

    /// Set whether list fields can be null.
    pub fn with_list_nullable(mut self, nullable: bool) -> Self {
        self.list_nullable = nullable;
        self
    }

    /// Set whether map fields can be null.
    pub fn with_map_nullable(mut self, nullable: bool) -> Self {
        self.map_nullable = nullable;
        self
    }

    /// Set whether list element values can be null.
    pub fn with_list_value_nullable(mut self, nullable: bool) -> Self {
        self.list_value_nullable = nullable;
        self
    }

    /// Set whether map values can be null.
    pub fn with_map_value_nullable(mut self, nullable: bool) -> Self {
        self.map_value_nullable = nullable;
        self
    }

    /// Set whether to use LargeUtf8 instead of Utf8 for string fields.
    pub fn with_use_large_string(mut self, use_large: bool) -> Self {
        self.use_large_string = use_large;
        self
    }

    /// Set whether to use LargeBinary instead of Binary for bytes fields.
    pub fn with_use_large_binary(mut self, use_large: bool) -> Self {
        self.use_large_binary = use_large;
        self
    }

    /// Set whether to use LargeList instead of List for repeated fields.
    pub fn with_use_large_list(mut self, use_large: bool) -> Self {
        self.use_large_list = use_large;
        self
    }

    /// Set how enum fields are represented in Arrow.
    pub fn with_enum_repr(mut self, repr: EnumRepr) -> Self {
        self.enum_repr = repr;
        self
    }

    /// Set the Confluent Schema Registry wire format policy.
    pub fn with_confluent_wire_policy(mut self, policy: ConfluentWirePolicy) -> Self {
        self.confluent_wire_policy = policy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PtarsConfig::default();
        assert_eq!(config.timestamp_tz, Some(Arc::from("UTC")));
        assert_eq!(config.timestamp_unit, TimeUnit::Nanosecond);
        assert_eq!(config.time_unit, TimeUnit::Nanosecond);
        assert_eq!(config.duration_unit, TimeUnit::Nanosecond);
        assert_eq!(config.list_value_name.as_ref(), "item");
        assert_eq!(config.map_value_name.as_ref(), "value");
        assert!(!config.list_nullable);
        assert!(!config.map_nullable);
        assert!(!config.list_value_nullable);
        assert!(!config.map_value_nullable);
        assert!(!config.use_large_string);
        assert!(!config.use_large_binary);
        assert!(!config.use_large_list);
        assert_eq!(config.enum_repr, EnumRepr::Int32);
        assert_eq!(config.confluent_wire_policy, ConfluentWirePolicy::Raw);
    }

    #[test]
    fn test_new_config() {
        let config = PtarsConfig::new();
        assert_eq!(config.timestamp_tz, Some(Arc::from("UTC")));
    }

    #[test]
    fn test_with_timestamp_tz() {
        let config = PtarsConfig::new().with_timestamp_tz(Some("America/New_York"));
        assert_eq!(config.timestamp_tz, Some(Arc::from("America/New_York")));

        let config = PtarsConfig::new().with_timestamp_tz(None);
        assert_eq!(config.timestamp_tz, None);
    }

    #[test]
    fn test_with_timestamp_unit() {
        let config = PtarsConfig::new().with_timestamp_unit(TimeUnit::Microsecond);
        assert_eq!(config.timestamp_unit, TimeUnit::Microsecond);
    }

    #[test]
    fn test_with_time_unit() {
        let config = PtarsConfig::new().with_time_unit(TimeUnit::Millisecond);
        assert_eq!(config.time_unit, TimeUnit::Millisecond);
    }

    #[test]
    fn test_with_duration_unit() {
        let config = PtarsConfig::new().with_duration_unit(TimeUnit::Second);
        assert_eq!(config.duration_unit, TimeUnit::Second);
    }

    #[test]
    fn test_with_list_value_name() {
        let config = PtarsConfig::new().with_list_value_name("element");
        assert_eq!(config.list_value_name.as_ref(), "element");
    }

    #[test]
    fn test_with_map_value_name() {
        let config = PtarsConfig::new().with_map_value_name("val");
        assert_eq!(config.map_value_name.as_ref(), "val");
    }

    #[test]
    fn test_with_list_nullable() {
        let config = PtarsConfig::new().with_list_nullable(true);
        assert!(config.list_nullable);
    }

    #[test]
    fn test_with_map_nullable() {
        let config = PtarsConfig::new().with_map_nullable(true);
        assert!(config.map_nullable);
    }

    #[test]
    fn test_with_list_value_nullable() {
        let config = PtarsConfig::new().with_list_value_nullable(true);
        assert!(config.list_value_nullable);
    }

    #[test]
    fn test_with_map_value_nullable() {
        let config = PtarsConfig::new().with_map_value_nullable(true);
        assert!(config.map_value_nullable);
    }

    #[test]
    fn test_with_use_large_string() {
        let config = PtarsConfig::new().with_use_large_string(true);
        assert!(config.use_large_string);
    }

    #[test]
    fn test_with_use_large_binary() {
        let config = PtarsConfig::new().with_use_large_binary(true);
        assert!(config.use_large_binary);
    }

    #[test]
    fn test_with_use_large_list() {
        let config = PtarsConfig::new().with_use_large_list(true);
        assert!(config.use_large_list);
    }

    #[test]
    fn test_builder_chaining() {
        let config = PtarsConfig::new()
            .with_timestamp_tz(Some("Europe/London"))
            .with_timestamp_unit(TimeUnit::Millisecond)
            .with_time_unit(TimeUnit::Microsecond)
            .with_duration_unit(TimeUnit::Second)
            .with_list_value_name("elem")
            .with_map_value_name("v")
            .with_list_nullable(true)
            .with_map_nullable(true)
            .with_list_value_nullable(true)
            .with_map_value_nullable(true)
            .with_use_large_string(true)
            .with_use_large_binary(true)
            .with_use_large_list(true)
            .with_enum_repr(EnumRepr::String)
            .with_confluent_wire_policy(ConfluentWirePolicy::Protobuf);

        assert_eq!(config.timestamp_tz, Some(Arc::from("Europe/London")));
        assert_eq!(config.timestamp_unit, TimeUnit::Millisecond);
        assert_eq!(config.time_unit, TimeUnit::Microsecond);
        assert_eq!(config.duration_unit, TimeUnit::Second);
        assert_eq!(config.list_value_name.as_ref(), "elem");
        assert_eq!(config.map_value_name.as_ref(), "v");
        assert!(config.list_nullable);
        assert!(config.map_nullable);
        assert!(config.list_value_nullable);
        assert!(config.map_value_nullable);
        assert!(config.use_large_string);
        assert!(config.use_large_binary);
        assert!(config.use_large_list);
        assert_eq!(config.enum_repr, EnumRepr::String);
        assert_eq!(config.confluent_wire_policy, ConfluentWirePolicy::Protobuf);
    }
}
