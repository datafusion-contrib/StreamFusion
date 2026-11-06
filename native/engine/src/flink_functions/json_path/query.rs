use super::{number_text, unescape_utf16, Value};
use simd_json::{Node as TapeNode, StaticNode};
use std::fmt::{self, Write};

pub(super) fn write_tape(
    nodes: &[TapeNode<'_>],
    legacy: bool,
    out: &mut impl Write,
) -> fmt::Result {
    write_tape_value(nodes, legacy, out, 0)
}

fn write_tape_value(
    nodes: &[TapeNode<'_>],
    legacy: bool,
    out: &mut impl Write,
    depth: usize,
) -> fmt::Result {
    if depth % 64 == 0 {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
            write_tape_inner(nodes, legacy, out, depth)
        })
    } else {
        write_tape_inner(nodes, legacy, out, depth)
    }
}

fn write_tape_inner(
    nodes: &[TapeNode<'_>],
    legacy: bool,
    out: &mut impl Write,
    depth: usize,
) -> fmt::Result {
    use super::simd::width;
    match &nodes[0] {
        TapeNode::String(text) => crate::json_string::write_json_string(out, text),
        TapeNode::Static(StaticNode::Null) => out.write_str("null"),
        TapeNode::Static(StaticNode::Bool(value)) => {
            out.write_str(if *value { "true" } else { "false" })
        }
        TapeNode::Static(StaticNode::I64(value)) => write!(out, "{value}"),
        TapeNode::Static(StaticNode::U64(value)) => write!(out, "{value}"),
        TapeNode::Array { len, .. } => {
            out.write_char('[')?;
            let mut rest = &nodes[1..];
            for index in 0..*len {
                if index != 0 {
                    out.write_char(',')?;
                }
                let (value, tail) = rest.split_at(width(&rest[0]));
                write_tape_value(value, legacy, out, depth + 1)?;
                rest = tail;
            }
            out.write_char(']')
        }
        TapeNode::Object { len, .. } => {
            out.write_char('{')?;
            let mut rest = &nodes[1..];
            if *len <= 1 {
                if *len == 1 {
                    let TapeNode::String(key) = &rest[0] else {
                        return Err(fmt::Error);
                    };
                    crate::json_string::write_json_string(out, key)?;
                    out.write_char(':')?;
                    write_tape_value(&rest[1..], legacy, out, depth + 1)?;
                }
            } else {
                let mut members = Vec::with_capacity(*len);
                for index in 0..*len {
                    let TapeNode::String(key) = &rest[0] else {
                        return Err(fmt::Error);
                    };
                    rest = &rest[1..];
                    let (value, tail) = rest.split_at(width(&rest[0]));
                    members.push((*key, value, index));
                    rest = tail;
                }
                members.sort_by(|left, right| Key::Text(left.0).compare(&Key::Text(right.0)));
                let mut unique = 0;
                let mut start = 0;
                while start < members.len() {
                    let mut end = start + 1;
                    while end < members.len() && members[start].0 == members[end].0 {
                        end += 1;
                    }
                    members[unique] = (members[start].0, members[end - 1].1, members[start].2);
                    unique += 1;
                    start = end;
                }
                members.truncate(unique);
                if legacy {
                    members.sort_unstable_by_key(|member| member.2);
                }
                for (index, (key, value, _)) in members.into_iter().enumerate() {
                    if index != 0 {
                        out.write_char(',')?;
                    }
                    crate::json_string::write_json_string(out, key)?;
                    out.write_char(':')?;
                    write_tape_value(value, legacy, out, depth + 1)?;
                }
            }
            out.write_char('}')
        }
        _ => Err(fmt::Error), // Floating-point tapes retain the original-spelling parser.
    }
}

pub(crate) fn write_container(raw: &str, legacy: bool, out: &mut impl Write) -> fmt::Result {
    let mut parser = TreeParser {
        raw,
        position: 0,
        legacy,
        nodes: Vec::with_capacity(32),
    };
    let root = parser.value(0);
    parser.write(root, out, 0)
}

pub(crate) fn write_scalar(value: Value<'_>, legacy: bool, out: &mut impl Write) -> fmt::Result {
    match value {
        Value::String(raw) => write_string(raw, out),
        Value::DecodedString(text) => crate::json_string::write_json_string(out, text),
        Value::Number(raw) => write_number(raw, legacy, out),
        Value::LegacyFloat(value) => write_double(value, out),
        Value::Boolean(value) => out.write_str(if value { "true" } else { "false" }),
        Value::Null => out.write_str("null"),
        _ => Err(fmt::Error),
    }
}

fn write_string(raw: &str, out: &mut impl Write) -> fmt::Result {
    if raw.contains('\\') {
        write_units(&unescape_utf16(raw), out)
    } else {
        crate::json_string::write_json_string(out, raw)
    }
}

fn write_units(units: &[u16], out: &mut impl Write) -> fmt::Result {
    let text: String = char::decode_utf16(units.iter().copied())
        .map(|value| value.unwrap_or('?'))
        .collect();
    crate::json_string::write_json_string(out, &text)
}

fn write_number(raw: &str, legacy: bool, out: &mut impl Write) -> fmt::Result {
    if legacy && raw.contains(['.', 'e', 'E']) {
        write_double(raw.parse().map_err(|_| fmt::Error)?, out)
    } else {
        out.write_str(&number_text(raw))
    }
}

fn write_double(value: f64, out: &mut impl Write) -> fmt::Result {
    let mut bytes = Vec::new();
    streamfusion_bridge::jdk_double::jdk_double_to_string(value, &mut bytes);
    let text = std::str::from_utf8(&bytes).map_err(|_| fmt::Error)?;
    if value.is_finite() {
        out.write_str(text)
    } else {
        crate::json_string::write_json_string(out, text)
    }
}

const END: usize = usize::MAX;

enum Key<'a> {
    Text(&'a str),
    Units(Vec<u16>),
}

impl<'a> Key<'a> {
    fn new(raw: &'a str) -> Self {
        if raw.contains('\\') {
            Self::Units(unescape_utf16(raw))
        } else {
            Self::Text(raw)
        }
    }

    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Text(left), Self::Text(right)) if left.is_ascii() && right.is_ascii() => {
                left.cmp(right)
            }
            (Self::Text(left), Self::Text(right)) => left.encode_utf16().cmp(right.encode_utf16()),
            (Self::Text(left), Self::Units(right)) => {
                left.encode_utf16().cmp(right.iter().copied())
            }
            (Self::Units(left), Self::Text(right)) => {
                left.iter().copied().cmp(right.encode_utf16())
            }
            (Self::Units(left), Self::Units(right)) => left.cmp(right),
        }
    }

    fn write(&self, out: &mut impl Write) -> fmt::Result {
        match self {
            Self::Text(text) => crate::json_string::write_json_string(out, text),
            Self::Units(units) => write_units(units, out),
        }
    }
}

#[derive(Clone, Copy)]
enum NodeValue<'a> {
    Scalar(Value<'a>),
    Array(usize),
    Object(usize),
}

struct Node<'a> {
    value: NodeValue<'a>,
    key: Option<Key<'a>>,
    next: usize,
}

// The selector validates the entire document first, including fields outside the result.
// This second pass retains only the selected tree and its original UTF-16 map-key identity.
struct TreeParser<'a> {
    raw: &'a str,
    position: usize,
    legacy: bool,
    // Links use arena indexes, so containers need neither child allocations nor recursive drop.
    nodes: Vec<Node<'a>>,
}

impl<'a> TreeParser<'a> {
    fn whitespace(&mut self) {
        while self.peek().is_some_and(|byte| b" \t\r\n".contains(&byte)) {
            self.position += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.raw.as_bytes().get(self.position).copied()
    }

    fn string(&mut self) -> &'a str {
        let start = self.position + 1;
        self.position = start;
        loop {
            match self.peek().expect("validated string") {
                b'"' => {
                    let end = self.position;
                    self.position += 1;
                    return &self.raw[start..end];
                }
                b'\\' => self.position += 2,
                _ => self.position += 1,
            }
        }
    }

    fn value(&mut self, depth: usize) -> usize {
        if depth % 64 == 0 {
            stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.value_inner(depth))
        } else {
            self.value_inner(depth)
        }
    }

    fn value_inner(&mut self, depth: usize) -> usize {
        self.whitespace();
        let node = self.nodes.len();
        self.nodes.push(Node {
            value: NodeValue::Scalar(Value::Null),
            key: None,
            next: END,
        });
        self.nodes[node].value = match self.peek().expect("validated value") {
            b'{' | b'[' => {
                let object = self.peek() == Some(b'{');
                let end = if object { b'}' } else { b']' };
                self.position += 1;
                self.whitespace();
                let mut first = END;
                let mut previous = END;
                while self.peek() != Some(end) {
                    let key = if object {
                        let key = Key::new(self.string());
                        self.whitespace();
                        self.position += 1;
                        Some(key)
                    } else {
                        None
                    };
                    let child = self.value(depth + 1);
                    self.nodes[child].key = key;
                    if previous == END {
                        first = child;
                    } else {
                        self.nodes[previous].next = child;
                    }
                    previous = child;
                    self.whitespace();
                    if self.peek() != Some(b',') {
                        break;
                    }
                    self.position += 1;
                    self.whitespace();
                }
                self.position += 1;
                if object {
                    NodeValue::Object(self.order_members(first))
                } else {
                    NodeValue::Array(first)
                }
            }
            b'"' => NodeValue::Scalar(Value::String(self.string())),
            b't' => {
                self.position += 4;
                NodeValue::Scalar(Value::Boolean(true))
            }
            b'f' => {
                self.position += 5;
                NodeValue::Scalar(Value::Boolean(false))
            }
            b'n' => {
                self.position += 4;
                NodeValue::Scalar(Value::Null)
            }
            _ => {
                let start = self.position;
                while self
                    .peek()
                    .is_some_and(|byte| byte.is_ascii_digit() || b"-.eE+".contains(&byte))
                {
                    self.position += 1;
                }
                NodeValue::Scalar(Value::Number(&self.raw[start..self.position]))
            }
        };
        node
    }

    fn order_members(&mut self, first: usize) -> usize {
        if first == END || self.nodes[first].next == END {
            return first;
        }
        let mut members = Vec::new();
        let mut next = first;
        while next != END {
            members.push(next);
            next = self.nodes[next].next;
        }
        // Stable sorting groups equal original UTF-16 names in their insertion order.
        members.sort_by(|&left, &right| {
            self.nodes[left]
                .key
                .as_ref()
                .unwrap()
                .compare(self.nodes[right].key.as_ref().unwrap())
        });
        let mut unique = 0;
        let mut start = 0;
        while start < members.len() {
            let mut end = start + 1;
            while end < members.len()
                && self.nodes[members[start]]
                    .key
                    .as_ref()
                    .unwrap()
                    .compare(self.nodes[members[end]].key.as_ref().unwrap())
                    .is_eq()
            {
                end += 1;
            }
            let first = members[start];
            self.nodes[first].value = self.nodes[members[end - 1]].value;
            members[unique] = first;
            unique += 1;
            start = end;
        }
        members.truncate(unique);
        if self.legacy {
            // Preorder indexes retain the first insertion position of each replaced member.
            members.sort_unstable();
        }
        for pair in members.windows(2) {
            self.nodes[pair[0]].next = pair[1];
        }
        self.nodes[*members.last().unwrap()].next = END;
        members[0]
    }

    fn write(&self, node: usize, out: &mut impl Write, depth: usize) -> fmt::Result {
        if depth % 64 == 0 {
            stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
                self.write_inner(node, out, depth)
            })
        } else {
            self.write_inner(node, out, depth)
        }
    }

    fn write_inner(&self, node: usize, out: &mut impl Write, depth: usize) -> fmt::Result {
        match self.nodes[node].value {
            NodeValue::Scalar(value) => write_scalar(value, self.legacy, out),
            NodeValue::Array(first) | NodeValue::Object(first) => {
                let object = matches!(self.nodes[node].value, NodeValue::Object(_));
                out.write_char(if object { '{' } else { '[' })?;
                let mut child = first;
                while child != END {
                    if child != first {
                        out.write_char(',')?;
                    }
                    if object {
                        self.nodes[child].key.as_ref().unwrap().write(out)?;
                        out.write_char(':')?;
                    }
                    self.write(child, out, depth + 1)?;
                    child = self.nodes[child].next;
                }
                out.write_char(if object { '}' } else { ']' })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_objects_keep_insertion_positions_and_last_values() {
        let mut output = String::new();
        write_container(
            r#"{"z":1.2300,"a":-0,"z":2.5000,"x":{"b":1,"a":2}}"#,
            true,
            &mut output,
        )
        .unwrap();
        assert_eq!(output, r#"{"z":2.5,"a":0,"x":{"b":1,"a":2}}"#);
    }

    #[test]
    fn arena_members_replace_equal_escaped_keys_without_merging_surrogates() {
        let raw = r#"{"z":[0],"a":{},"\u007a":[{"x":1,"x":2}],"?":3,"\uD800":4}"#;
        for (legacy, expected) in [
            (true, r#"{"z":[{"x":2}],"a":{},"?":3,"?":4}"#),
            (false, r#"{"?":3,"a":{},"z":[{"x":2}],"?":4}"#),
        ] {
            let mut output = String::new();
            write_container(raw, legacy, &mut output).unwrap();
            assert_eq!(output, expected);
        }
    }
}
