use std::ops::{Bound, Deref, RangeBounds, RangeInclusive};

use crate::utils::RangeLength;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Ranges<Tag> {
    spans: Vec<Span<Tag>>,
    maximum: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Span<Tag> {
    pub tag: Tag,
    pub start: u64,
}

impl<Tag> Default for Ranges<Tag>
where
    Tag: Eq + Clone + Default,
{
    fn default() -> Self {
        Self::new(Tag::default(), Some(u64::MAX))
    }
}

impl<Tag> Ranges<Tag>
where
    Tag: Eq + Clone,
{
    pub fn new(default_tag: Tag, maximum: Option<u64>) -> Self {
        Self {
            spans: vec![Span {
                tag: default_tag,
                start: 0,
            }],
            maximum,
        }
    }

    #[must_use]
    pub fn get(&self, offset: u64) -> &Tag {
        let span = match self.binary_search_by(|span| span.start.cmp(&offset)) {
            Ok(index) => self.spans.get(index),
            Err(index) => {
                // This offset is contained by the previous span than `offset`
                self.spans.get(
                    index
                        .checked_sub(1)
                        .expect("first entry should always have start of 0"),
                )
            }
        };
        span.map(|span| &span.tag).expect("index is checked above")
    }

    pub fn set(&mut self, range: impl RangeBounds<u64>, tag: Tag) {
        let maximum = if let Some(maximum) = self.maximum {
            maximum
        } else {
            // No maximum means this range set can't contain anything.
            return;
        };
        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => start.saturating_add(1),
            Bound::Unbounded => 0,
        }
        .min(maximum);
        let end = match range.end_bound() {
            Bound::Included(end) => *end,
            Bound::Excluded(end) => end.saturating_sub(1),
            Bound::Unbounded => maximum,
        }
        .min(maximum);
        if start == 0 && end == maximum {
            // The entire range is being set
            self.spans.truncate(1);
            self.spans[0].start = 0;
            self.spans[0].tag = tag;
            return;
        } else if start > end {
            // No-op
            return;
        }

        self.set_between(start, end, maximum, tag);
    }

    fn set_between(&mut self, start: u64, end: u64, maximum: u64, tag: Tag) {
        let first_span_index = match self.binary_search_by(|span| span.start.cmp(&start)) {
            Ok(index) => {
                if index > 0 && self.spans[index - 1].tag == tag {
                    // When the tag matches, we will join with the previous span.
                    index - 1
                } else {
                    index
                }
            }
            Err(index) => {
                // This offset is contained by the previous span than `offset`
                index - 1
            }
        };
        if let Some(first_span) = self.spans.get_mut(first_span_index) {
            let (next_index, first_span_tag) = if first_span.tag == tag {
                // We already match the tag, so we don't need to update the
                // start.
                (first_span_index + 1, tag.clone())
            } else if first_span.start == start {
                // Because the span's offset matches, we are replacing this
                // entry's tag.
                let mut temp_tag = tag.clone();
                std::mem::swap(&mut temp_tag, &mut first_span.tag);
                if first_span_index > 0
                    && self.spans[first_span_index - 1].tag == self.spans[first_span_index].tag
                {
                    self.spans.remove(first_span_index);
                    (first_span_index, temp_tag)
                } else {
                    (first_span_index + 1, temp_tag)
                }
            } else {
                let first_span_tag = first_span.tag.clone();
                let new_index = first_span_index + 1;
                self.spans.insert(
                    new_index,
                    Span {
                        tag: tag.clone(),
                        start,
                    },
                );
                (new_index + 1, first_span_tag)
            };
            let mut end_index = next_index
                + match self.spans[next_index..]
                    .binary_search_by(|span| span.start.cmp(&end.saturating_add(1)))
                {
                    Ok(index) => {
                        if self.spans[next_index + index].tag == first_span_tag {
                            // Because the tag matches, we're going to join with
                            // this node rather than insert a new one.
                            index
                        } else {
                            // We need to insert a new node before this, or modify
                            // the previous entry.
                            index.saturating_sub(1)
                        }
                    }
                    Err(index) => {
                        // if index == self.spans.len() - next_index {
                        //     index
                        // } else {
                        index.saturating_sub(1)
                        // }
                    }
                };
            if end_index > next_index {
                self.spans.drain(next_index..end_index);
                end_index = next_index;
            }
            // Now to ensure the end of this range is updated correctly,
            // which may include inserting `tag` which now contains
            // first_span's tag.
            let subsequent_start = self.spans.get(next_index + 1).map(|span| span.start);
            if end == maximum {
                // The new span covers the remaining portion.
                self.spans.truncate(end_index);
            } else if let Some(next_span) = self.spans.get_mut(end_index) {
                if next_span.start > end + 1 && next_span.tag != first_span_tag {
                    // The new span still should have some remaining
                    // bytes after the inserted span's end.
                    self.spans.insert(
                        end_index,
                        Span {
                            tag: first_span_tag,
                            start: end + 1,
                        },
                    );
                } else if next_span.tag == tag || subsequent_start == Some(end + 1) {
                    // These two nodes can merge.
                    self.spans.remove(end_index);
                } else {
                    // The new span overlaps the next span, so the next
                    // span's start becomes the end of the inserted
                    // span.
                    next_span.start = end + 1;
                }
            } else {
                // The former tag becomes the new tail.
                self.spans.push(Span {
                    tag: first_span_tag,
                    start: end + 1,
                });
            }
        } else {
            let tail_span = end.checked_add(1).map(|start| Span {
                tag: self
                    .spans
                    .last()
                    .map(|span| &span.tag)
                    .cloned()
                    .expect("spans can't be empty here"),
                start,
            });
            self.spans.push(Span { tag, start });
            if let Some(tail_span) = tail_span {
                // The span didn't last until the end, but the previous tail
                // did. This places a clone of the former last tail at the
                // end.
                self.spans.push(tail_span);
            }
        }
    }

    #[must_use]
    pub fn iter(&self) -> Iter<'_, Tag> {
        self.into_iter()
    }

    #[must_use]
    pub fn maximum(&self) -> Option<u64> {
        self.maximum
    }

    pub fn extend_by(&mut self, amount: u64, tag: Tag) {
        self.extend(
            self.maximum.map_or_else(
                || amount.saturating_sub(1),
                |maximum| maximum.saturating_add(amount),
            ),
            tag,
        );
    }

    pub fn extend(&mut self, new_maximum: u64, tag: Tag) {
        match self.maximum {
            // Shortening is ignored.
            Some(existing_maximum) if new_maximum <= existing_maximum => {}
            Some(existing_maximum) => {
                let last_span = self.spans.last_mut().expect("should never be empty");
                // The maximum is inclusive, so the new range starts
                // after it
                if let Some(new_start) = existing_maximum.checked_add(1) {
                    if last_span.start == new_start {
                        last_span.tag = tag;
                    } else if last_span.tag != tag {
                        self.spans.push(Span {
                            tag,
                            start: new_start,
                        });
                    }
                }
                self.maximum = Some(new_maximum);
            }
            None => {
                self.spans.push(Span { tag, start: 0 });
                self.maximum = Some(new_maximum);
            }
        }
    }
}

impl<Tag> Deref for Ranges<Tag> {
    type Target = [Span<Tag>];

    fn deref(&self) -> &Self::Target {
        &self.spans
    }
}

#[derive(Debug)]
pub struct Iter<'a, Tag> {
    ranges: &'a Ranges<Tag>,
    index: Option<usize>,
}

impl<'a, Tag> Iterator for Iter<'a, Tag> {
    type Item = (RangeInclusive<u64>, &'a Tag);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(index) = self.index {
            if let Some(span) = self.ranges.get(index) {
                let next_index = index.checked_add(1);
                let result = if let Some(next_span) =
                    next_index.and_then(|index| self.ranges.spans.get(index))
                {
                    (span.start..=next_span.start.saturating_sub(1), &span.tag)
                } else if let Some(maximum) = self.ranges.maximum {
                    (span.start..=maximum, &span.tag)
                } else {
                    // Empty set.
                    return None;
                };
                self.index = index.checked_add(1);
                return Some(result);
            }
        }
        None
    }
}

impl<'a, Tag> IntoIterator for &'a Ranges<Tag> {
    type Item = (RangeInclusive<u64>, &'a Tag);
    type IntoIter = Iter<'a, Tag>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            ranges: self,
            index: Some(0),
        }
    }
}

impl<Tag> FromIterator<(RangeInclusive<u64>, Tag)> for Ranges<Tag>
where
    Tag: Eq + Clone,
{
    fn from_iter<T: IntoIterator<Item = (RangeInclusive<u64>, Tag)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut collected = Self {
            spans: Vec::with_capacity(iter.size_hint().0),
            maximum: None,
        };
        for (range, tag) in iter {
            assert!(range.start() == &collected.maximum.map_or(0, |max| max + 1));
            collected.extend_by(range.len(), tag);
        }
        collected
    }
}

impl<Tag> FromIterator<Tag> for Ranges<Tag>
where
    Tag: Eq + Clone,
{
    fn from_iter<T: IntoIterator<Item = Tag>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut collected = Self {
            spans: Vec::with_capacity(iter.size_hint().0),
            maximum: None,
        };
        for tag in iter {
            collected.extend_by(1, tag);
        }
        collected
    }
}

#[test]
fn simple_overwrite_test() {
    let mut ranges = Ranges::new(0, Some(2));
    ranges.set(1..2, 1);
    assert_eq!(ranges.get(0), &0);
    assert_eq!(ranges.get(1), &1);
    assert_eq!(ranges.get(0), &0);
}

#[test]
fn basics() {
    let mut ranges = Ranges::<usize>::default();
    assert_eq!(ranges.get(0), &0);
    assert_eq!(ranges.get(u64::MAX), &0);
    ranges.set(0..100, 1);
    assert_eq!(ranges.get(0), &1);
    assert_eq!(ranges.get(99), &1);
    assert_eq!(ranges.get(100), &0);
    assert_eq!(ranges.get(u64::MAX), &0);
    ranges.set(1..3, 2);
    assert_eq!(ranges.get(0), &1);
    for i in 1..3 {
        assert_eq!(ranges.get(i), &2, "ranges.tag_for({i}) != 2");
    }
    assert_eq!(ranges.get(3), &1);
    assert_eq!(ranges.get(u64::MAX), &0);
    assert_eq!(ranges.len(), 4);
    ranges.set(1..=2, 1);
    assert_eq!(ranges.len(), 2);
    let ranges = ranges.into_iter().collect::<Vec<_>>();
    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[0].0.start(), &0);
    assert_eq!(ranges[0].0.end(), &99);
    assert!(ranges[0].1 == &1);
    assert!(ranges[1].0.start() == &100);
    assert!(ranges[1].0.end() == &u64::MAX);
    assert!(ranges[1].1 == &0);
}

#[test]
fn multi_overwrite() {
    let mut ranges = Ranges::<usize>::default();
    ranges.set(1..5, 1);
    ranges.set(10..20, 2);
    ranges.set(30..40, 1);
    assert_eq!(ranges.len(), 7);
    // Combine all the ranges.
    ranges.set(5..=29, 1);
    assert_eq!(ranges.len(), 3);
}
