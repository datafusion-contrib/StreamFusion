use crate::HashMap;
use serde_json::Value;

/// Finds where one Avro binary datum ends by walking its bytes against the writer schema, without
/// materializing any value. Flink's deserializers read exactly one datum per Kafka message and
/// never look at the bytes after it, while arrow-avro's streaming decoder keeps consuming frames
/// until the buffer runs out — so the decode measures the first datum's extent up front and hands
/// the decoder exactly that frame. The walk applies the same structural checks the Java decoder
/// makes (buffer overrun, negative length, out-of-range union branch), so a datum it rejects is
/// one Flink fails on too.
pub(crate) struct DatumSkipper {
    nodes: Vec<SkipNode>,
    root: usize,
}

/// One writer-schema element's wire footprint, with children as arena indices (named types can be
/// referenced more than once, and legally even recursively).
enum SkipNode {
    Null,
    Boolean,
    /// int, long, and enum indexes share the zigzag varint encoding.
    Varint,
    Float,
    Double,
    /// bytes and string: a length varint followed by that many bytes.
    Len,
    Fixed(usize),
    Record(Vec<usize>),
    Array(usize),
    Map(usize),
    Union(Vec<usize>),
}

impl DatumSkipper {
    pub(crate) fn parse(schema_json: &str) -> Result<DatumSkipper, String> {
        let schema: Value = serde_json::from_str(schema_json)
            .map_err(|error| format!("invalid avro writer schema: {error}"))?;
        let mut nodes = Vec::new();
        let mut names = HashMap::default();
        let root = compile(&schema, None, &mut nodes, &mut names)?;
        Ok(DatumSkipper { nodes, root })
    }

    /// The end offset (exclusive) of the single datum starting at `start`.
    pub(crate) fn datum_end(&self, bytes: &[u8], start: usize) -> Result<usize, String> {
        self.skip(self.root, bytes, start).map_err(Fault::describe)
    }

    fn skip(&self, node: usize, bytes: &[u8], pos: usize) -> Result<usize, Fault> {
        match &self.nodes[node] {
            SkipNode::Null => Ok(pos),
            SkipNode::Boolean => advance(bytes, pos, 1),
            SkipNode::Varint => skip_varint(bytes, pos),
            SkipNode::Float => advance(bytes, pos, 4),
            SkipNode::Double => advance(bytes, pos, 8),
            SkipNode::Len => {
                let (length, pos) = read_long(bytes, pos)?;
                advance(bytes, pos, byte_count(length)?)
            }
            SkipNode::Fixed(size) => advance(bytes, pos, *size),
            SkipNode::Record(fields) => {
                fields.iter().try_fold(pos, |pos, field| self.skip(*field, bytes, pos))
            }
            SkipNode::Union(branches) => {
                let (branch, pos) = read_long(bytes, pos)?;
                let branch = usize::try_from(branch)
                    .ok()
                    .and_then(|branch| branches.get(branch))
                    .ok_or(Fault::BranchOutOfRange(branch))?;
                self.skip(*branch, bytes, pos)
            }
            SkipNode::Array(item) => {
                self.blocks(bytes, pos, |bytes, pos| self.skip(*item, bytes, pos))
            }
            SkipNode::Map(value) => self.blocks(bytes, pos, |bytes, pos| {
                let (key_length, pos) = read_long(bytes, pos)?;
                let pos = advance(bytes, pos, byte_count(key_length)?)?;
                self.skip(*value, bytes, pos)
            }),
        }
    }

    /// Array/map block runs: an item count, then the items; a negative count carries the block's
    /// byte size next, letting the walk jump the whole block.
    fn blocks(
        &self,
        bytes: &[u8],
        mut pos: usize,
        skip_item: impl Fn(&[u8], usize) -> Result<usize, Fault>,
    ) -> Result<usize, Fault> {
        loop {
            let (count, next) = read_long(bytes, pos)?;
            pos = next;
            if count == 0 {
                return Ok(pos);
            }
            if count < 0 {
                let (size, next) = read_long(bytes, pos)?;
                pos = advance(bytes, next, byte_count(size)?)?;
            } else {
                for _ in 0..count {
                    pos = skip_item(bytes, pos)?;
                }
            }
        }
    }
}

/// A structural defect in the datum, kept register-sized on the walk's hot return path and only
/// rendered to a message once the decode fails.
#[derive(Clone, Copy)]
enum Fault {
    Overrun,
    InvalidVarint,
    NegativeLength(i64),
    BranchOutOfRange(i64),
}

impl Fault {
    fn describe(self) -> String {
        match self {
            Fault::Overrun => "datum overruns the message body".to_string(),
            Fault::InvalidVarint => "invalid varint encoding in the datum".to_string(),
            Fault::NegativeLength(length) => format!("negative length {length} in the datum"),
            Fault::BranchOutOfRange(branch) => format!("union branch {branch} out of range"),
        }
    }
}

#[inline]
fn advance(bytes: &[u8], pos: usize, by: usize) -> Result<usize, Fault> {
    pos.checked_add(by).filter(|end| *end <= bytes.len()).ok_or(Fault::Overrun)
}

#[inline]
fn byte_count(length: i64) -> Result<usize, Fault> {
    usize::try_from(length).map_err(|_| Fault::NegativeLength(length))
}

/// Skips one varint without decoding its value — all a skipped int/long/enum needs is its
/// terminating byte (high bit clear), within the Java decoder's 10-byte bound.
#[inline]
fn skip_varint(bytes: &[u8], pos: usize) -> Result<usize, Fault> {
    let limit = bytes.len().min(pos + 10);
    for p in pos..limit {
        if bytes[p] & 0x80 == 0 {
            return Ok(p + 1);
        }
    }
    Err(if limit < pos + 10 { Fault::Overrun } else { Fault::InvalidVarint })
}

/// Zigzag varint, with the Java decoder's 10-byte bound.
#[inline]
fn read_long(bytes: &[u8], mut pos: usize) -> Result<(i64, usize), Fault> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let Some(&byte) = bytes.get(pos) else {
            return Err(Fault::Overrun);
        };
        pos += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((((value >> 1) as i64) ^ -((value & 1) as i64), pos));
        }
        shift += 7;
        if shift >= 70 {
            return Err(Fault::InvalidVarint);
        }
    }
}

fn compile(
    schema: &Value,
    namespace: Option<&str>,
    nodes: &mut Vec<SkipNode>,
    names: &mut HashMap<String, usize>,
) -> Result<usize, String> {
    match schema {
        Value::String(name) => leaf_or_ref(name, namespace, nodes, names),
        Value::Array(branches) => {
            let branches = branches
                .iter()
                .map(|branch| compile(branch, namespace, nodes, names))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(push(nodes, SkipNode::Union(branches)))
        }
        Value::Object(object) => {
            let type_value =
                object.get("type").ok_or("avro schema object carries no type")?;
            let Value::String(type_name) = type_value else {
                return compile(type_value, namespace, nodes, names);
            };
            match type_name.as_str() {
                "record" | "error" => {
                    let (fullname, child_namespace) = fullname(object, namespace)?;
                    // Registered before its fields compile so a recursive reference resolves.
                    let index = push(nodes, SkipNode::Record(Vec::new()));
                    names.insert(fullname, index);
                    let fields = object
                        .get("fields")
                        .and_then(Value::as_array)
                        .ok_or("avro record carries no fields")?;
                    let fields = fields
                        .iter()
                        .map(|field| {
                            let field_type =
                                field.get("type").ok_or("avro record field carries no type")?;
                            compile(field_type, child_namespace.as_deref(), nodes, names)
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    nodes[index] = SkipNode::Record(fields);
                    Ok(index)
                }
                "enum" => {
                    let (fullname, _) = fullname(object, namespace)?;
                    let index = push(nodes, SkipNode::Varint);
                    names.insert(fullname, index);
                    Ok(index)
                }
                "fixed" => {
                    let (fullname, _) = fullname(object, namespace)?;
                    let size = object
                        .get("size")
                        .and_then(Value::as_u64)
                        .ok_or("avro fixed carries no size")? as usize;
                    let index = push(nodes, SkipNode::Fixed(size));
                    names.insert(fullname, index);
                    Ok(index)
                }
                "array" => {
                    let items = object.get("items").ok_or("avro array carries no items")?;
                    let items = compile(items, namespace, nodes, names)?;
                    Ok(push(nodes, SkipNode::Array(items)))
                }
                "map" => {
                    let values = object.get("values").ok_or("avro map carries no values")?;
                    let values = compile(values, namespace, nodes, names)?;
                    Ok(push(nodes, SkipNode::Map(values)))
                }
                other => leaf_or_ref(other, namespace, nodes, names),
            }
        }
        other => Err(format!("unsupported avro schema element: {other}")),
    }
}

fn leaf_or_ref(
    name: &str,
    namespace: Option<&str>,
    nodes: &mut Vec<SkipNode>,
    names: &HashMap<String, usize>,
) -> Result<usize, String> {
    let node = match name {
        "null" => SkipNode::Null,
        "boolean" => SkipNode::Boolean,
        "int" | "long" => SkipNode::Varint,
        "float" => SkipNode::Float,
        "double" => SkipNode::Double,
        "bytes" | "string" => SkipNode::Len,
        _ => {
            // A reference to an earlier named type: a dotted name is already full, a bare one
            // resolves in the enclosing namespace first (the Avro name rules).
            let resolved = if name.contains('.') {
                names.get(name)
            } else {
                namespace
                    .filter(|namespace| !namespace.is_empty())
                    .and_then(|namespace| names.get(format!("{namespace}.{name}").as_str()))
                    .or_else(|| names.get(name))
            };
            return resolved.copied().ok_or_else(|| format!("unresolved avro type name {name}"));
        }
    };
    Ok(push(nodes, node))
}

/// The declared fullname plus the namespace the type's children inherit.
fn fullname(
    object: &serde_json::Map<String, Value>,
    enclosing: Option<&str>,
) -> Result<(String, Option<String>), String> {
    let name =
        object.get("name").and_then(Value::as_str).ok_or("avro named type carries no name")?;
    if let Some((namespace, _)) = name.rsplit_once('.') {
        return Ok((name.to_string(), Some(namespace.to_string())));
    }
    let namespace = object
        .get("namespace")
        .and_then(Value::as_str)
        .or(enclosing)
        .filter(|namespace| !namespace.is_empty())
        .map(str::to_string);
    let full = match &namespace {
        Some(namespace) => format!("{namespace}.{name}"),
        None => name.to_string(),
    };
    Ok((full, namespace))
}

fn push(nodes: &mut Vec<SkipNode>, node: SkipNode) -> usize {
    nodes.push(node);
    nodes.len() - 1
}
