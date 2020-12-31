//! # TimeSeries Data Element Representations
use chrono::{NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::cmp;
use std::hash::Hash;

///TimeSeriesDataPoint representation, consists of a timestamp and value
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TimeSeriesDataPoint<TDate: Hash + Clone + cmp::Eq + cmp::Ord, T> {
    pub timestamp: TDate,
    pub value: T,
}


impl<TDate: Hash + Clone + cmp::Eq + cmp::Ord, T> TimeSeriesDataPoint<TDate, T> {
    /// Generic new method
    pub fn new(timestamp: TDate, value: T) -> TimeSeriesDataPoint<TDate, T> {
        TimeSeriesDataPoint { timestamp, value }
    }
}
impl<T> TimeSeriesDataPoint<NaiveDateTime, T> {

    /// Convenience function makes TimeSeriesDataPoints from integer stamps
    pub fn from_int_stamp(secs: i64, value: T) -> TimeSeriesDataPoint<NaiveDateTime, T> {
        let timestamp = NaiveDateTime::from_timestamp(secs,0);
        TimeSeriesDataPoint { timestamp , value }
    }

}

impl<TDate: Hash + Copy + cmp::Eq + cmp::Ord, T: cmp::PartialEq> cmp::PartialEq for TimeSeriesDataPoint<TDate, T> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.value == other.value
    }
}
