use super::{Path, Step, Value};
use simd_json::{tape::Tape, Buffers, Node, StaticNode};

/// Reuses SIMD scratch space across rows, retaining the scalar parser for Jackson edges.
pub(crate) struct Reader {
    scratch: Vec<u8>,
    buffers: Buffers,
    tape: Option<Tape<'static>>,
    buffer_size: usize,
}

impl Reader {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            scratch: Vec::new(),
            buffers: Buffers::default(),
            tape: Some(Tape::null()),
            buffer_size,
        }
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn read<'a>(&'a mut self, path: &Path<'_>, input: &'a str) -> Result<Value<'a>, ()> {
        path.apply_policy(self.parse(path, input))
    }

    /// IS JSON validates the first document without Jayway's root-null or path-mode policy.
    pub fn read_document<'a>(
        &'a mut self,
        root: &Path<'_>,
        input: &'a str,
    ) -> Result<Value<'a>, ()> {
        self.parse(root, input).map(|(value, _)| value)
    }

    fn parse<'a>(&'a mut self, path: &Path<'_>, input: &'a str) -> Result<(Value<'a>, bool), ()> {
        // Jackson copies short documents into its recycled token buffer before parsing,
        // including malformed documents and documents handled by the SIMD path here.
        if input.len() > self.buffer_size && self.buffer_size < 32768 {
            let units = input.encode_utf16().take(32769).count();
            if units <= 32768 {
                self.buffer_size = self.buffer_size.max(units);
            }
        }
        if !candidate(input) {
            return path.parse_with_buffer(input, self.buffer_size);
        }
        self.scratch.clear();
        self.scratch.extend_from_slice(input.as_bytes());
        let mut tape = self.tape.take().unwrap_or_else(Tape::null).reset();
        let value = match simd_json::fill_tape(&mut self.scratch, &mut self.buffers, &mut tape) {
            Ok(()) if compatible(&tape) => select(&tape, &path.steps).map(|value| {
                (
                    value,
                    matches!(tape.0.first(), Some(Node::Static(StaticNode::Null))),
                )
            }),
            _ => None,
        };
        self.tape = Some(tape.reset());
        match value {
            Some(value) => Ok(value),
            None => path.parse_with_buffer(input, self.buffer_size),
        }
    }
}

fn candidate(input: &str) -> bool {
    // Shorter documents cannot exceed Jackson's string/name limits. SIMD pays off for
    // many members; inspecting only the prefix avoids rescanning long padding strings.
    input.len() < 50_000
        && memchr::memchr_iter(b':', &input.as_bytes()[..input.len().min(256)])
            .nth(7)
            .is_some()
        && memchr::memmem::find(input.as_bytes(), b"\\u").is_none()
}

fn compatible(tape: &Tape<'_>) -> bool {
    // A small tape cannot exceed the nesting limit. Floating numbers require the
    // original spelling and Jackson's BigDecimal validation, even in unselected fields.
    tape.0.len() <= 1000
        && !tape
            .0
            .iter()
            .any(|node| matches!(node, Node::Static(StaticNode::F64(_))))
}

fn select<'a>(tape: &Tape<'a>, steps: &[Step<'_>]) -> Option<Value<'a>> {
    let mut nodes = tape.0.as_slice();
    for step in steps {
        nodes = match (nodes.first()?, step) {
            (Node::Object { len, .. }, Step::Member(name)) => {
                let mut rest = &nodes[1..];
                let mut selected = None;
                for _ in 0..*len {
                    let Node::String(key) = rest.first()? else {
                        return None;
                    };
                    rest = &rest[1..];
                    let (value, tail) = rest.split_at(width(rest.first()?));
                    if key == name {
                        selected = Some(value);
                    }
                    rest = tail;
                }
                // Keep the last matching member before descending into its subtree.
                match selected {
                    Some(value) => value,
                    None => return Some(Value::Missing),
                }
            }
            (Node::Array { len, .. }, Step::Index(index)) if index < len => {
                let mut rest = &nodes[1..];
                for _ in 0..*index {
                    rest = &rest[width(rest.first()?)..];
                }
                &rest[..width(rest.first()?)]
            }
            _ => return Some(Value::Missing),
        };
    }
    match nodes.first()? {
        Node::String(value) => Some(Value::DecodedString(value)),
        Node::Object { .. } | Node::Array { .. } => Some(Value::Container),
        Node::Static(StaticNode::Null) => Some(Value::Null),
        Node::Static(StaticNode::Bool(value)) => Some(Value::Boolean(*value)),
        // Preserve integer spelling such as -0 through the scalar parser as well.
        _ => None,
    }
}

fn width(node: &Node<'_>) -> usize {
    match node {
        Node::Object { count, .. } | Node::Array { count, .. } => count + 1,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> String {
        (0..16)
            .map(|i| format!(r#", "k{i}": "value{i}""#))
            .collect()
    }

    fn equivalent(path: &Path<'_>, input: &str, reader: &mut Reader) {
        let expected = path.read(input);
        let actual = reader.read(path, input);
        match (actual, expected) {
            (Ok(Value::DecodedString(actual)), Ok(Value::String(expected))) => {
                assert_eq!(actual, super::super::unescape(expected));
            }
            (actual, expected) => assert_eq!(actual, expected, "{input}"),
        }
    }

    #[test]
    fn wide_documents_preserve_last_duplicate_members_and_scalar_results() {
        let mut reader = Reader::new(4000);
        for mode in ["strict", "lax"] {
            for suffix in [
                r#""last""#,
                r#""line\nquote\"""#,
                "null",
                "false",
                "{}",
                "[]",
                "-0",
                "1.2300",
            ] {
                let input = format!(r#"{{"a":"first"{},"a":{suffix}}}"#, fields());
                assert!(candidate(&input));
                let text = format!("{mode} $.a");
                let path = Path::parse(&text, "13.0").unwrap();
                equivalent(&path, &input, &mut reader);
            }
            let text = format!("{mode} $.a[1].b");
            let path = Path::parse(&text, "13.0").unwrap();
            for replacement in [r#"[null,{"b":"last"}]"#, "{}", "[]", "null"] {
                let input = format!(
                    r#"{{"a":[null,{{"b":"first"}}]{},"a":{replacement}}}"#,
                    fields()
                );
                equivalent(&path, &input, &mut reader);
            }
        }
    }

    #[test]
    fn jackson_edges_use_the_existing_parser() {
        let path = Path::parse("lax $.a", "13.0").unwrap();
        let mut reader = Reader::new(4000);
        for tail in [
            r#", "bad": tru"#,
            r#", "bad": "\ud800""#,
            r#", "bad": 1e2147483648"#,
            r#", "bad": 1.2300"#,
            r#", "bad": -0"#,
        ] {
            let input = format!(r#"{{"a":"ok"{}{tail}}}"#, fields());
            equivalent(&path, &input, &mut reader);
        }
        let input = format!(r#"{{"a":"ok"{}}} trailing"#, fields());
        equivalent(&path, &input, &mut reader);
        let input = format!(
            r#"{{"a":"ok"{},"deep":{}0{}}}"#,
            fields(),
            "[".repeat(1001),
            "]".repeat(1001)
        );
        equivalent(&path, &input, &mut reader);
        let input = format!(r#"{{"a":"ok"{},"{}":0}}"#, fields(), "k".repeat(50_001));
        assert!(!candidate(&input));
        equivalent(&path, &input, &mut reader);
    }

    #[test]
    fn reuses_allocations_and_returns_decoded_strings() {
        let path = Path::parse("lax $.a", "13.0").unwrap();
        let input = format!(r#"{{"a":"line\ntext"{}}}"#, fields());
        let mut reader = Reader::new(4000);
        for _ in 0..10 {
            assert!(matches!(
                reader.read(&path, &input),
                Ok(Value::DecodedString("line\ntext"))
            ));
        }
        assert!(reader.tape.as_ref().unwrap().0.is_empty());
        assert!(reader.tape.as_ref().unwrap().0.capacity() > 0);
    }
}
