//! Ordered reads over the parts of a snapshot.

use super::part::Part;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// `(part index, row)` of the live rows of `parts` in key order, at most
/// `limit` of them (0 = all). Each part is sorted by key and live keys are
/// unique across parts, so a k-way merge yields the order directly and stops
/// after `limit` rows: O(limit * log parts), plus the dead rows skipped.
pub(super) fn ordered_live(parts: &[Part], limit: usize) -> Vec<(usize, usize)> {
    let next_live =
        |part: usize, from: usize| (from..parts[part].rows()).find(|&row| parts[part].is_live(row));
    let mut heads = BinaryHeap::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        if let Some(row) = next_live(i, 0) {
            heads.push(Reverse((part.key(row), i, row)));
        }
    }
    let wanted = if limit == 0 { usize::MAX } else { limit };
    let mut picks = Vec::new();
    while picks.len() < wanted {
        let Some(Reverse((_, part, row))) = heads.pop() else {
            break;
        };
        picks.push((part, row));
        if let Some(next) = next_live(part, row + 1) {
            heads.push(Reverse((parts[part].key(next), part, next)));
        }
    }
    picks
}
