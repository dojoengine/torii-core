use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::hash::{BuildHasher, Hash, RandomState};

#[derive(Clone, Copy, Debug)]
struct HeapEntry<W, V> {
    weight: W,
    value: V,
}

impl<W, V> PartialEq for HeapEntry<W, V>
where
    W: Eq,
    V: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.weight == other.weight && self.value == other.value
    }
}
impl<W, V> Eq for HeapEntry<W, V>
where
    W: Eq,
    V: Eq,
{
}

impl<W, V> PartialOrd for HeapEntry<W, V>
where
    W: Ord,
    V: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<W, V> Ord for HeapEntry<W, V>
where
    W: Ord,
    V: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by weight first, then value for deterministic ordering.
        self.weight
            .cmp(&other.weight)
            .then_with(|| self.value.cmp(&other.value))
    }
}

#[derive(Debug, Clone, Default)]
pub struct WeightedQueue<W, V, S = RandomState>
where
    V: Copy + Eq + Hash + Ord,
    W: Ord,
{
    // Reverse turns BinaryHeap into min-heap by weight.
    heap: BinaryHeap<Reverse<HeapEntry<W, V>>>,
    // Tracks the latest queued weight for each value.
    latest_weight: HashMap<V, W, S>,
}

impl<W, V, S> WeightedQueue<W, V, S>
where
    V: Copy + Eq + Hash + Ord,
    W: Copy + Ord,
    S: BuildHasher + Default,
{
    pub fn new(initial: impl IntoIterator<Item = (V, W)>) -> Self {
        let mut heap = BinaryHeap::new();
        let mut latest_weight = HashMap::<V, W, S>::with_hasher(Default::default());

        for (value, weight) in initial {
            latest_weight.insert(value, weight);
            heap.push(Reverse(HeapEntry { weight, value }));
        }

        Self {
            heap,
            latest_weight,
        }
    }

    pub fn insert(&mut self, value: V, weight: W) {
        if self.latest_weight.get(&value).copied() == Some(weight) {
            return;
        }

        self.latest_weight.insert(value, weight);
        self.heap.push(Reverse(HeapEntry { weight, value }));
    }

    pub fn pop_next(&mut self) -> Option<(V, W)> {
        while let Some(Reverse(entry)) = self.heap.pop() {
            let current = self.latest_weight.get(&entry.value).copied();
            if current == Some(entry.weight) {
                self.latest_weight.remove(&entry.value);
                return Some((entry.value, entry.weight));
            }
        }
        None
    }

    pub fn pop_batch(&mut self, amount: usize) -> Vec<(V, W)> {
        let mut items = Vec::with_capacity(amount);
        for _ in 0..amount {
            match self.pop_next() {
                Some(item) => items.push(item),
                None => break,
            }
        }
        items
    }
}

#[cfg(test)]
mod tests {
    use super::WeightedQueue;

    #[test]
    fn pops_smallest_weight_first() {
        let mut q = WeightedQueue::new(vec![(1_u8, 10_u64), (2_u8, 5_u64), (3_u8, 7_u64)]);
        assert_eq!(q.pop_next(), Some((2, 5)));
        assert_eq!(q.pop_next(), Some((3, 7)));
        assert_eq!(q.pop_next(), Some((1, 10)));
        assert_eq!(q.pop_next(), None);
    }

    #[test]
    fn insert_places_entry_by_new_weight() {
        let mut q = WeightedQueue::new(vec![(1_u8, 10_u64), (2_u8, 5_u64)]);

        let popped = q.pop_next();
        assert_eq!(popped, Some((2, 5)));

        q.insert(2, 12);
        assert_eq!(q.pop_next(), Some((1, 10)));
        assert_eq!(q.pop_next(), Some((2, 12)));
        assert_eq!(q.pop_next(), None);
    }

    #[test]
    fn pop_up_to_returns_at_most_requested_items() {
        let mut q = WeightedQueue::new(vec![(1_u8, 10_u64), (2_u8, 5_u64), (3_u8, 7_u64)]);

        let first_batch = q.pop_batch(2);
        assert_eq!(first_batch, vec![(2, 5), (3, 7)]);

        let second_batch = q.pop_batch(2);
        assert_eq!(second_batch, vec![(1, 10)]);

        let third_batch = q.pop_up_to(2);
        assert!(third_batch.is_empty());
    }

    #[test]
    fn pop_up_to_zero_returns_empty() {
        let mut q = WeightedQueue::new(vec![(1_u8, 10_u64)]);
        assert!(q.pop_up_to(0).is_empty());
        assert_eq!(q.pop_next(), Some((1, 10)));
    }
}
