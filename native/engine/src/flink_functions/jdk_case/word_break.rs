//! The forward word-boundary table from the worker's OpenJDK runtime image.

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct WordBreak {
    transitions: Vec<u16>,
    backward: Vec<u16>,
    accepting: Vec<u8>,
    lookahead: Vec<u8>,
    bmp_blocks: Vec<u16>,
    bmp_categories: Vec<u8>,
    supplementary: Vec<u32>,
    categories: usize,
}

impl WordBreak {
    pub(super) fn from_words(mut words: impl Iterator<Item = u64>) -> Option<Self> {
        let size = words.next()?;
        let mut bytes = words
            .flat_map(|word| word.to_le_bytes())
            .collect::<Vec<_>>();
        if size as usize > bytes.len() {
            return None;
        }
        bytes.truncate(size as usize);
        Self::parse(&bytes)
    }

    fn parse(bytes: &[u8]) -> Option<Self> {
        let mut data = Data(bytes);
        if data.take(8)? != b"BIdata\0\x01" || data.u32()? as usize != data.0.len() {
            return None;
        }
        let transitions_len = data.u32()? as usize;
        let backward_len = data.u32()? as usize;
        let states = data.u32()? as usize;
        let lookahead_len = data.u32()? as usize;
        let bmp_len = data.u32()? as usize;
        let supplementary_len = data.u32()? as usize;
        let extra_len = data.u32()? as usize;
        data.take(8)?; // checksum, verified by the worker against its actual iterator
        if states < 2 || states != lookahead_len || transitions_len % states != 0 {
            return None;
        }
        let categories = transitions_len / states;
        if !(1..=254).contains(&categories) {
            return None;
        }
        let transitions = data.u16s(transitions_len)?;
        if transitions.iter().any(|&state| state as usize >= states) {
            return None;
        }
        if backward_len < 2 * categories || backward_len % categories != 0 {
            return None;
        }
        let backward = data.u16s(backward_len)?;
        if backward
            .iter()
            .any(|&state| state as usize >= backward_len / categories)
        {
            return None;
        }
        let accepting = data.take(states)?.to_vec();
        let lookahead = data.take(states)?.to_vec();
        let bmp_blocks = data.u16s(512)?;
        let bmp_categories = data.take(bmp_len)?.to_vec();
        let supplementary = (0..supplementary_len)
            .map(|_| data.u32())
            .collect::<Option<Vec<_>>>()?;
        // Thai dictionary rules define dictionary text separately from cased text.
        // Extra boundaries within its words cannot change the final-sigma decision.
        data.take(extra_len)?;
        let valid_category = |category: u8| category == 255 || (category as usize) < categories;
        if !data.0.is_empty()
            || bmp_blocks
                .iter()
                .any(|&offset| offset as usize + 128 > bmp_len)
            || bmp_categories
                .iter()
                .copied()
                .any(|category| !valid_category(category))
            || supplementary
                .first()
                .is_none_or(|word| word >> 8 != 0x10000)
            || supplementary
                .last()
                .is_none_or(|word| word >> 8 != 0x110000)
            || supplementary
                .windows(2)
                .any(|pair| pair[0] >> 8 > pair[1] >> 8)
            || supplementary
                .iter()
                .any(|&word| !valid_category(word as u8))
        {
            return None;
        }
        Some(Self {
            transitions,
            backward,
            accepting,
            lookahead,
            bmp_blocks,
            bmp_categories,
            supplementary,
            categories,
        })
    }

    fn category(&self, point: u32) -> u8 {
        if point < 0x10000 {
            let point = point as usize;
            self.bmp_categories[self.bmp_blocks[point >> 7] as usize + (point & 127)]
        } else {
            let end = self
                .supplementary
                .partition_point(|&word| word >> 8 <= point);
            self.supplementary[end - 1] as u8
        }
    }

    /// The stock casing code asks `isBoundary` at both sides of sigma. An iterator's
    /// answer at a UTF-16 offset can differ from a forward scan of the same string.
    pub(super) fn final_sigma(
        &self,
        text: &[u16],
        sigma: usize,
        is_cased: impl Fn(u32) -> bool,
    ) -> bool {
        let mut cached = None;
        let mut before = sigma;
        while !self.is_boundary(text, before, &mut cached) {
            let point = previous_point(text, &mut before);
            if is_cased(point) {
                let mut after = sigma + 1;
                while after < text.len() && !self.is_boundary(text, after, &mut cached) {
                    let (point, width) = point_at(text, after);
                    if is_cased(point) {
                        return false;
                    }
                    after += width;
                }
                return true;
            }
        }
        false
    }

    fn is_boundary(&self, text: &[u16], index: usize, cached: &mut Option<usize>) -> bool {
        if index == 0 {
            return true;
        }
        let offset = index - 1;
        let mut result = if offset == 0 {
            self.next(text, 0)
        } else {
            Some(
                cached
                    .filter(|&previous| previous < offset)
                    .unwrap_or_else(|| self.safe_start(text, offset)),
            )
        };
        while let Some(previous) = result {
            if previous > offset {
                break;
            }
            result = self.next(text, previous);
        }
        *cached = result;
        result == Some(index)
    }

    fn next(&self, text: &[u16], start: usize) -> Option<usize> {
        if start == text.len() {
            return None;
        }
        let mut boundary = start + point_at(text, start).1;
        let mut pending = 0;
        let mut state = 1;
        let mut cursor = start;
        while state != 0 && cursor < text.len() {
            let (point, width) = point_at(text, cursor);
            if point == 0xffff {
                break;
            }
            let category = self.category(point);
            if category != 255 {
                state = self.transitions[state * self.categories + category as usize] as usize;
            }
            cursor += width;
            let accepting = self.accepting[state] == 1;
            if self.lookahead[state] == 1 {
                if accepting {
                    boundary = pending;
                } else {
                    pending = cursor;
                }
            } else if accepting {
                boundary = cursor;
            }
        }
        if pending == text.len() {
            boundary = pending;
        }
        Some(boundary)
    }

    fn safe_start(&self, text: &[u16], offset: usize) -> usize {
        let mut cursor = offset;
        let mut point = point_at(text, cursor).0;
        let mut state = 1;
        let mut category = 0;
        let mut previous_category = 0;
        while point != 0xffff && state != 0 {
            previous_category = category;
            category = self.category(point);
            if category != 255 {
                state = self.backward[state * self.categories + category as usize] as usize;
            }
            if cursor == 0 {
                return 0;
            }
            point = previous_point(text, &mut cursor);
        }
        if point != 0xffff {
            let movement = if previous_category == 255 { 1 } else { 2 };
            for _ in 0..movement {
                let next = cursor + point_at(text, cursor).1;
                if next < text.len() {
                    cursor = next;
                }
            }
        }
        cursor
    }
}

fn point_at(text: &[u16], index: usize) -> (u32, usize) {
    let Some(&first) = text.get(index) else {
        return (0xffff, 1);
    };
    if (0xd800..=0xdbff).contains(&first) {
        if let Some(&second) = text.get(index + 1) {
            if (0xdc00..=0xdfff).contains(&second) {
                return (paired_point(first, second), 2);
            }
        }
    }
    (u32::from(first), 1)
}

fn previous_point(text: &[u16], index: &mut usize) -> u32 {
    *index -= 1;
    let last = text[*index];
    if (0xdc00..=0xdfff).contains(&last) && *index > 0 {
        let first = text[*index - 1];
        if (0xd800..=0xdbff).contains(&first) {
            *index -= 1;
            return paired_point(first, last);
        }
    }
    u32::from(last)
}

fn paired_point(first: u16, last: u16) -> u32 {
    0x10000 + (u32::from(first - 0xd800) << 10) + u32::from(last - 0xdc00)
}

struct Data<'a>(&'a [u8]);

impl<'a> Data<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (value, remaining) = self.0.split_at_checked(n)?;
        self.0 = remaining;
        Some(value)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u16s(&mut self, n: usize) -> Option<Vec<u16>> {
        Some(
            self.take(n.checked_mul(2)?)?
                .chunks_exact(2)
                .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                .collect(),
        )
    }
}
