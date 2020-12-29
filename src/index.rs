use std::cmp;
use std::collections::{BinaryHeap,HashMap, HashSet};
use std::ops::Index;
use std::hash::Hash;
use chrono::{Duration, NaiveDateTime};
use serde::{Serialize};

/// DateTimeIndex is represented as an array of timestamps (i64)
#[derive(Clone, Debug)]
pub struct HashableIndex<TIndex: Serialize + Hash + Clone + cmp::Eq + cmp::Ord> {
    pub values: Vec<TIndex>
}

//SRC:: https://stackoverflow.com/questions/64262297/rust-how-to-find-n-th-most-frequent-element-in-a-collection
fn most_frequent<T>(array: &Vec<T>) -> Vec<(usize, T)>  
where
    T: Hash + Eq + Ord + Clone, 
{ #![allow(clippy::ptr_arg)]
    let mut map = HashMap::new();
    for x in array {
        *map.entry(x).or_default() += 1;
    }
    let k = map.len();
    let mut heap = BinaryHeap::with_capacity(k);
    for (x, count) in map.into_iter() {
        heap.push(cmp::Reverse((count, x.clone())));
    }
    heap.into_sorted_vec().iter().map(|r| r.0.clone()).collect()
}

pub trait SampleableIndex<TIndex: Serialize + Hash + Copy + cmp::Eq + cmp::Ord,TInterval>{
    fn sample_rates(&self) -> Vec<(usize, TInterval)>;
    fn is_mono_intervaled(&self) -> bool;
}

impl SampleableIndex<NaiveDateTime,Duration> for HashableIndex<NaiveDateTime>
{
    /// Infer index sample rate, returns a vector that represtest (number of times a sample rate is observed, the sample rate)
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::index::HashableIndex;
    /// use tsxlib::index::SampleableIndex;
    /// use tsxlib::timeutils;
    /// use chrono::{NaiveDateTime,Duration};
    /// 
    /// let index = HashableIndex::new(vec![ timeutils::naive_datetime_from_millis(0), timeutils::naive_datetime_from_millis(5),timeutils::naive_datetime_from_millis(10), timeutils::naive_datetime_from_millis(15), timeutils::naive_datetime_from_millis(20), timeutils::naive_datetime_from_millis(25), timeutils::naive_datetime_from_millis(75)]);
    /// let exp =  vec![(5,Duration::milliseconds(5)),(1,Duration::milliseconds(50))];
    /// assert_eq!(index.sample_rates(), exp);
    fn sample_rates(&self) -> Vec<(usize, Duration)> { 

        let timediffs =  self.values
            .iter()
            .zip(self.values.iter().skip(1))
            .map(|(x, y)| y.signed_duration_since(*x))
            .collect();
        
        most_frequent(&timediffs)
    }
    /// returns true if the index is spaced at equal itervals
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::index::HashableIndex;
    /// use tsxlib::index::SampleableIndex;
    /// use tsxlib::timeutils;
    /// use chrono::{NaiveDateTime,Duration};
    /// 
    /// let index = HashableIndex::new(vec![ timeutils::naive_datetime_from_millis(0), timeutils::naive_datetime_from_millis(5),timeutils::naive_datetime_from_millis(10), timeutils::naive_datetime_from_millis(15), timeutils::naive_datetime_from_millis(20), timeutils::naive_datetime_from_millis(25), timeutils::naive_datetime_from_millis(75)]);
    /// let index_mono = HashableIndex::new(vec![ timeutils::naive_datetime_from_millis(0), timeutils::naive_datetime_from_millis(5),timeutils::naive_datetime_from_millis(10), timeutils::naive_datetime_from_millis(15), timeutils::naive_datetime_from_millis(20), timeutils::naive_datetime_from_millis(25)]);
    /// assert_eq!(index.is_mono_intervaled(), false);
    /// assert_eq!(index_mono.is_mono_intervaled(), true);
    fn is_mono_intervaled(&self) -> bool{
        let samp_rates = self.sample_rates();
        samp_rates.len() == 1
    }
}

impl HashableIndex<NaiveDateTime>{
    pub fn from_int_stamps(stamps: Vec<i64>) -> HashableIndex<NaiveDateTime> {
        let values = stamps.iter().map(|i| NaiveDateTime::from_timestamp(*i,0)).collect();
        HashableIndex { values }
    }
}

impl <TIndex: Serialize + Hash + Clone + cmp::Eq + cmp::Ord> HashableIndex<TIndex> {
    /// Create new index from a vec of values of type TIndex
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::index::HashableIndex;
    ///
    /// let values = vec![1, 2, 3, 4];
    /// let index = HashableIndex::new(values);
    /// assert_eq!(index.len(), 4);
    /// ```
    pub fn new(values: Vec<TIndex>) -> HashableIndex<TIndex> {

        HashableIndex { values }
    }



    /// test the monotonicity test for an index
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::index::HashableIndex;
    ///
    /// let vs = HashableIndex::new(vec![1, 2, 3, 4]);
    /// let xs = HashableIndex::new(vec![1, 2, 3, 3]);
    /// let ys = HashableIndex::new(vec![1, 2, 3, 2]);
    /// assert_eq!(vs.is_monotonic(), true);
    /// assert_eq!(xs.is_monotonic(), false);
    /// assert_eq!(ys.is_monotonic(), false);
    /// ```
    pub fn is_monotonic(&self) -> bool {
        self.values
            .iter()
            .zip(self.values.iter().skip(1))
            .all(|(x, y)| x < y)
    }


    /// get length of the index
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// is the index empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// ref to the last value of an index
    pub fn last(&self) -> std::option::Option<&TIndex> {
        self.values.last()
    }

    /// very slow, tests if index is unique by generating a hashset of the index keys and then comparing lengths
    pub fn is_unique(&self) -> bool {
        let set: HashSet<&TIndex> = self.iter().collect();
        set.len() == self.len()
    }

    /// generate and iterator for the index
    pub fn iter(&self) -> std::slice::Iter<TIndex> {
        self.values.iter()
    }

}


impl <TIndex: Serialize + Hash + Clone + cmp::Eq + cmp::Ord> Index<usize> for  HashableIndex<TIndex>  {
    type Output = TIndex;

    fn index(&self, pos: usize) -> &Self::Output {
        &self.values[pos]
    }
}

impl <TIndex: Serialize + Hash + Clone + cmp::Eq + cmp::Ord> cmp::PartialEq for HashableIndex<TIndex> {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeutils;
    use chrono::{Duration};

    #[test]
    fn test_increasing() {
        let values = vec![1, 2, 3, 4, 3];
        let index = HashableIndex::from_int_stamps(values);
        assert_eq!(index.len(), 5);
    }

    #[test]
    fn test_monotonic_empty() {
        let index: HashableIndex<NaiveDateTime> = HashableIndex::new(vec![]);
        assert!(index.is_monotonic());
    }

    #[test]
    fn test_monotonic_singleton() {
        let index = HashableIndex::from_int_stamps(vec![1]);
        assert!(index.is_monotonic());
    }

    #[test]
    fn test_sample_rate_info(){        
        let index = HashableIndex::new(vec![ timeutils::naive_datetime_from_millis(0), timeutils::naive_datetime_from_millis(5),timeutils::naive_datetime_from_millis(10), timeutils::naive_datetime_from_millis(15), timeutils::naive_datetime_from_millis(20), timeutils::naive_datetime_from_millis(25), timeutils::naive_datetime_from_millis(75)]);
        let exp =  vec![(5,Duration::milliseconds(5)),(1,Duration::milliseconds(50))];
        assert_eq!(index.sample_rates(), exp);
    }

    #[test]
    fn test_monosampled_test(){
        let index = HashableIndex::new(vec![ timeutils::naive_datetime_from_millis(0), timeutils::naive_datetime_from_millis(5),timeutils::naive_datetime_from_millis(10), timeutils::naive_datetime_from_millis(15), timeutils::naive_datetime_from_millis(20), timeutils::naive_datetime_from_millis(25), timeutils::naive_datetime_from_millis(75)]);
        let index_mono = HashableIndex::new(vec![ timeutils::naive_datetime_from_millis(0), timeutils::naive_datetime_from_millis(5),timeutils::naive_datetime_from_millis(10), timeutils::naive_datetime_from_millis(15), timeutils::naive_datetime_from_millis(20), timeutils::naive_datetime_from_millis(25)]);
        assert_eq!(index.is_mono_intervaled(), false);
        assert_eq!(index_mono.is_mono_intervaled(), true);
    }

}
