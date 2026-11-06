use super::{indefinite, Path, Step, Value};
use simd_json::{tape::Tape, Buffers, Node, StaticNode};

/// Reuses SIMD scratch space across rows, retaining the scalar parser for Jackson edges.
pub(crate) struct Reader {
    scratch: Vec<u8>,
    buffers: Buffers,
    tape: Option<Tape<'static>>,
    buffer_size: usize,
    legacy: bool,
    row_buffers: Option<Vec<usize>>,
}

impl Reader {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            scratch: Vec::new(),
            buffers: Buffers::default(),
            tape: Some(Tape::null()),
            buffer_size,
            legacy: false,
            row_buffers: None,
        }
    }

    pub fn with_legacy_semantics(mut self, legacy: bool) -> Self {
        self.legacy = legacy;
        self
    }

    pub fn legacy_semantics(&self) -> bool {
        self.legacy
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub(super) fn with_row_buffers(mut self, capacities: Option<Vec<usize>>) -> Self {
        self.row_buffers = capacities;
        self
    }

    pub(super) fn set_buffer_size(&mut self, capacity: usize) {
        self.buffer_size = capacity;
    }

    pub fn begin_row(&mut self, row: usize) {
        if let Some(capacities) = &self.row_buffers {
            self.buffer_size = capacities[row];
        }
    }

    pub fn write_query_container(
        &mut self,
        path: &Path<'_>,
        input: &str,
        wrapper: &str,
        out: &mut impl std::fmt::Write,
    ) -> Option<std::fmt::Result> {
        if !path.definite() || !candidate(input) {
            return None;
        }
        self.scratch.clear();
        self.scratch.extend_from_slice(input.as_bytes());
        let mut tape = self.tape.take().unwrap_or_else(Tape::null).reset();
        let valid = simd_json::fill_tape(&mut self.scratch, &mut self.buffers, &mut tape).is_ok()
            && compatible(&tape);
        let result = (|| {
            if !valid {
                return None;
            }
            let selected = select_nodes(tape.0.as_slice(), &path.steps)?;
            let object = match selected.first()? {
                Node::Object { .. } => true,
                Node::Array { .. } => false,
                _ => return None,
            };
            let units = input.encode_utf16().count();
            if units <= 32768 {
                self.buffer_size = self.buffer_size.max(units);
            }
            let wrap = wrapper == "UNCONDITIONAL_ARRAY" || wrapper == "CONDITIONAL_ARRAY" && object;
            Some((|| {
                if wrap {
                    out.write_char('[')?;
                }
                super::query::write_tape(selected, self.legacy, out)?;
                if wrap {
                    out.write_char(']')?;
                }
                Ok(())
            })())
        })();
        self.tape = Some(tape.reset());
        result
    }

    pub fn read_query<'a>(
        &mut self,
        path: &Path<'_>,
        input: &'a str,
        surrogate_escapes: &'a [usize],
    ) -> Result<Value<'a>, ()> {
        if !path.definite() {
            return Err(());
        }
        let units = input.encode_utf16().count() - 5 * surrogate_escapes.len();
        if units <= 32768 {
            self.buffer_size = self.buffer_size.max(units);
        }
        path.apply_policy(path.parse_selected(
            input,
            self.buffer_size,
            self.legacy,
            surrogate_escapes,
            true,
        ))
    }

    pub fn read<'a>(&'a mut self, path: &Path<'_>, input: &'a str) -> Result<Value<'a>, ()> {
        path.apply_policy(self.parse(path, input))
    }

    pub fn read_escaped<'a>(
        &mut self,
        path: &Path<'_>,
        input: &'a str,
        surrogate_escapes: &'a [usize],
    ) -> Result<Value<'a>, ()> {
        // Each synthetic \\uXXXX occupies one original Java unit. Jackson's buffer
        // growth and number-token limits must observe those original positions.
        let units = input.encode_utf16().count() - 5 * surrogate_escapes.len();
        if units <= 32768 {
            self.buffer_size = self.buffer_size.max(units);
        }
        path.apply_policy(path.parse_escaped_with_profile(
            input,
            self.buffer_size,
            self.legacy,
            surrogate_escapes,
        ))
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
            return path.parse_with_profile(input, self.buffer_size, self.legacy);
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
            None => path.parse_with_profile(input, self.buffer_size, self.legacy),
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
        && tape.0.iter().all(|node| {
            matches!(
                node,
                Node::Object { .. }
                    | Node::Array { .. }
                    | Node::String(_)
                    | Node::Static(
                        StaticNode::Null
                            | StaticNode::Bool(_)
                            | StaticNode::I64(_)
                            | StaticNode::U64(_)
                    )
            )
        })
}

fn select<'a>(tape: &Tape<'a>, steps: &[Step<'_>]) -> Option<Value<'a>> {
    let mut nodes = tape.0.as_slice();
    for step in steps {
        nodes = match (nodes.first()?, step) {
            (_, Step::Wildcard) => return Some(Value::Container),
            (Node::Array { .. }, Step::IndexUnion) => return Some(Value::Container),
            (Node::Object { len, .. }, Step::Member(name)) => {
                let mut rest = &nodes[1..];
                let mut selected = None;
                for _ in 0..*len {
                    let Node::String(key) = rest.first()? else {
                        return None;
                    };
                    rest = &rest[1..];
                    let (value, tail) = rest.split_at(width(rest.first()?));
                    if *key == name.as_ref() {
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
            (Node::Array { len, .. }, Step::Index(index)) => {
                let offset = if *index < 0 {
                    len.checked_sub(index.unsigned_abs() as usize)
                } else {
                    Some(*index as usize)
                };
                let Some(offset) = offset.filter(|offset| offset < len) else {
                    return Some(if indefinite(steps) {
                        Value::Container
                    } else {
                        Value::Missing
                    });
                };
                let mut rest = &nodes[1..];
                for _ in 0..offset {
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

fn select_nodes<'t, 'a>(mut nodes: &'t [Node<'a>], steps: &[Step<'_>]) -> Option<&'t [Node<'a>]> {
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
                    if *key == name.as_ref() {
                        selected = Some(value);
                    }
                    rest = tail;
                }
                selected?
            }
            (Node::Array { len, .. }, Step::Index(index)) => {
                let offset = if *index < 0 {
                    len.checked_sub(index.unsigned_abs() as usize)?
                } else {
                    *index as usize
                };
                if offset >= *len {
                    return None;
                }
                let mut rest = &nodes[1..];
                for _ in 0..offset {
                    rest = &rest[width(rest.first()?)..];
                }
                &rest[..width(rest.first()?)]
            }
            _ => return None,
        };
    }
    Some(nodes)
}

pub(super) fn width(node: &Node<'_>) -> usize {
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

    #[test]
    fn query_containers_reuse_compatible_tapes_and_preserve_provider_order() {
        let selected = r#"{"z":[0],"a":{},"z":[{"x":1,"x":2}],"?":3,"中":4,"😀":5}"#;
        let input = format!(r#"{{"a":{selected}{}}}"#, fields());
        for legacy in [false, true] {
            for suffix in ["$", "$.a", "$.a.z", "$.a.z[-1]"] {
                let path = Path::parse(suffix, "13.0").unwrap();
                let mut reader = Reader::new(4000).with_legacy_semantics(legacy);
                for wrapper in ["WITHOUT_ARRAY", "CONDITIONAL_ARRAY", "UNCONDITIONAL_ARRAY"] {
                    let mut actual = String::new();
                    reader
                        .write_query_container(&path, &input, wrapper, &mut actual)
                        .unwrap()
                        .unwrap();
                    let Value::CapturedContainer(raw) =
                        reader.read_query(&path, &input, &[]).unwrap()
                    else {
                        panic!("expected selected container")
                    };
                    let mut expected = String::new();
                    let wrap = wrapper == "UNCONDITIONAL_ARRAY"
                        || wrapper == "CONDITIONAL_ARRAY" && raw.starts_with('{');
                    if wrap {
                        expected.push('[');
                    }
                    super::super::write_container(raw, legacy, &mut expected).unwrap();
                    if wrap {
                        expected.push(']');
                    }
                    assert_eq!(actual, expected, "{suffix}, legacy={legacy}, {wrapper}");
                }
            }
            let path = Path::parse("$.a", "13.0").unwrap();
            for tail in [
                r#", "bad": 1.2300"#,
                r#", "bad": "\uD800""#,
                r#", "bad": }"#,
            ] {
                let input = format!(r#"{{"a":{selected}{}{tail}}}"#, fields());
                let mut reader = Reader::new(4000).with_legacy_semantics(legacy);
                let mut output = String::new();
                assert!(reader
                    .write_query_container(&path, &input, "WITHOUT_ARRAY", &mut output)
                    .is_none());
                assert!(output.is_empty());
            }
        }
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
    fn indefinite_paths_keep_missing_properties_and_skipped_indexes_distinct() {
        let mut reader = Reader::new(4000);
        for mode in ["strict", "lax"] {
            for suffix in [
                "[0,1]",
                ".a[0,0]",
                ".a[0,1].x[*]",
                ".a[1].x[0,1]",
                ".a[-2].missing[0,1]",
                "[0,1][*]",
                "[*][0,1]",
                "[*]",
                ".a[*]",
                ".a[1][*]",
                ".a[-2].x[*]",
                ".missing[*]",
                "[1][*]",
                ".a[*].x",
                "[*][0].x[*]",
            ] {
                let text = format!("{mode} ${suffix}");
                let path = Path::parse(&text, "13.0").unwrap();
                for value in [
                    "null",
                    "false",
                    "[]",
                    "[{}]",
                    "[{},{}]",
                    "{}",
                    r#"{"x":[]}"#,
                ] {
                    let input = format!(r#"{{"a":[{{}},{{}}]{},"a":{value}}}"#, fields());
                    assert!(candidate(&input));
                    equivalent(&path, &input, &mut reader);
                }
            }
            for suffix in [
                "[*]",
                "[1][*]",
                "[-2].missing[*]",
                "[0].a[*]",
                "[0].missing[*]",
                "[0,1]",
                "[1].missing[0,1]",
                "[0].a[0,1]",
                "[0].missing[0,1]",
            ] {
                let text = format!("{mode} ${suffix}");
                let path = Path::parse(&text, "13.0").unwrap();
                let input = format!(r#"[{{"a":[]{}}}]"#, fields());
                assert!(candidate(&input));
                equivalent(&path, &input, &mut reader);
            }
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
    fn unicode_and_quoted_paths_agree_on_the_simd_tape() {
        let input = format!(
            r#"{{"用户":{{"姓.名":"first","姓.名":"last"}},"O'Reilly":true,"a\"b":"quoted"{}}}"#,
            fields()
        );
        assert!(candidate(&input));
        let mut reader = Reader::new(4000);
        for text in ["$.用户['姓.名']", "$[\"O'Reilly\"]", "$['a\"b']"] {
            let path = Path::parse(text, "13.0").unwrap();
            equivalent(&path, &input, &mut reader);
        }
        assert!(reader.tape.as_ref().unwrap().0.capacity() > 0);
    }

    #[test]
    fn escaped_member_names_select_identically_on_both_readers() {
        let mut reader = Reader::new(4000);
        for name in [
            "a'b", "a\"b", "a\\b", "a\nb", "\0", "\u{1f}", "用户", "😀", "\\u0061",
        ] {
            let key = serde_json::to_string(name).unwrap();
            let text = format!("lax $[{key}][-1][\"\\u007a\"]");
            let path = Path::parse(&text, "13.0").unwrap();
            for padding in [String::new(), fields()] {
                let input = format!(r#"{{{key}:[{{"z":"old"}}]{padding},{key}:[{{"z":"last"}}]}}"#);
                assert_eq!(path.read(&input), Ok(Value::String("last")));
                equivalent(&path, &input, &mut reader);
                if !padding.is_empty() && !key.contains(r"\u") {
                    assert!(candidate(&input));
                    assert_eq!(reader.read(&path, &input), Ok(Value::DecodedString("last")));
                }
                let input = format!(r#"{{{key}:[{{"z":"old"}}]{padding},{key}:[]}}"#);
                assert_eq!(path.read(&input), Ok(Value::Missing));
                equivalent(&path, &input, &mut reader);
                let input = format!(r#"{{{key}:[{{"z":"old"}}]{padding},"bad":1e2147483648}}"#);
                assert_eq!(path.read(&input), Ok(Value::Missing));
                equivalent(&path, &input, &mut reader);
            }
        }
        for text in [
            r#"$["\uD800"]"#,
            r#"$["\uDC00"]"#,
            r#"$["\q"]"#,
            r#"$["\u12"]"#,
            r#"$["a\"]"#,
        ] {
            assert!(Path::parse(text, "13.0").is_none(), "{text}");
        }
    }

    #[test]
    fn empty_members_select_on_both_readers_and_replace_duplicate_ancestors() {
        let mut reader = Reader::new(4000);
        for text in ["lax $['']['']", "lax $[\"\"][\"\"]"] {
            let path = Path::parse(text, "13.0").unwrap();
            for padding in [String::new(), fields()] {
                let input =
                    format!(r#"{{"":{{"":"first"}}{padding},"":{{" ":"space","":"last"}}}}"#);
                assert_eq!(path.read(&input), Ok(Value::String("last")));
                equivalent(&path, &input, &mut reader);
                if !padding.is_empty() {
                    assert!(candidate(&input));
                    assert_eq!(reader.read(&path, &input), Ok(Value::DecodedString("last")));
                }
                let input = format!(r#"{{"":{{"":"first"}}{padding},"":{{}}}}"#);
                assert_eq!(path.read(&input), Ok(Value::Missing));
                equivalent(&path, &input, &mut reader);
            }
        }
    }

    #[test]
    fn negative_indexes_use_tape_array_lengths_and_keep_independent_selections() {
        let input = format!(r#"{{"a":["first",{{"b":["x","y"]}},"last"]{}}}"#, fields());
        let mut reader = Reader::new(4000);
        assert!(candidate(&input));
        for (text, expected) in [
            ("$.a[-1]", "last"),
            ("$.a[-3]", "first"),
            ("$.a[-2].b[-1]", "y"),
            ("$.a[-0]", "first"),
        ] {
            let path = Path::parse(text, "13.0").unwrap();
            assert_eq!(
                reader.read(&path, &input),
                Ok(Value::DecodedString(expected))
            );
            equivalent(&path, &input, &mut reader);
        }
        for text in ["lax $.a[-4]", "lax $.a[-2147483648]", "lax $.a[2147483647]"] {
            let path = Path::parse(text, "13.0").unwrap();
            assert_eq!(reader.read(&path, &input), Ok(Value::Missing));
        }
        let input = format!(r#"{{"a":["first"]{},"a":[]}}"#, fields());
        let path = Path::parse("lax $.a[-1]", "13.0").unwrap();
        assert_eq!(reader.read(&path, &input), Ok(Value::Missing));
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
