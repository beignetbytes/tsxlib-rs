use std::cmp;
use std::fmt;
use std::hash::Hash;
use std::iter::FromIterator;
use itertools::Itertools;
use serde::{Serialize};


use crate::timeseries_iterators::{OrderedTimeSeriesIter, ShiftedTimeSeriesIter, RollingTimeSeriesIter,RollingTimeSeriesIterWithUpdate,FromUncheckedIterator,TimeSeriesRefIter,OrderedTimeSeriesRefIter, TimeSeriesIter, SkipApplyTimeSeriesIter};
use crate::data_elements::TimeSeriesDataPoint;
use crate::index::HashableIndex;
use crate::joins::{JoinEngine};

pub enum MergeAsofMode{ RollPrior, RollFollowing, NoRoll}

/// timeseries base struct of an index and a Vec<T> of values
#[derive(Clone,Debug)]
pub struct TimeSeries<TDate: Serialize + Hash + Clone + cmp::Eq + cmp::Ord, T: Clone> {
    pub timeindicies: HashableIndex<TDate>,
    pub values: Vec<T>, 
}


impl<TDate: Serialize + Hash + Clone + cmp::Eq + cmp::Ord, T: Clone> TimeSeries<TDate, T> {
    /// create an empty series with Types TDate for the index and T for the value
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::NaiveDateTime;
    /// 
    /// let ts = TimeSeries::<NaiveDateTime,f64>::empty();
    /// assert_eq!(ts.len(), 0);
    /// ```
    pub fn empty() -> TimeSeries<TDate, T> {
        TimeSeries::from_vecs(vec![], vec![]).unwrap()
    }

    /// Create a series by giving a vector of indicies and values
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// 
    /// let index = vec![1, 2, 3, 4, 5];
    /// let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let ts = TimeSeries::from_vecs(index, vals).unwrap();
    /// assert_eq!(ts.len(), 5);
    /// ```
    pub fn from_vecs(timeindicies: Vec<TDate>, values: Vec<T>) -> Result<TimeSeries<TDate, T>, std::io::Error> {
        let idx = HashableIndex::new(timeindicies);
        if !idx.is_unique() || !idx.is_monotonic() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "tied to build with an invalid index",
            ))
        } else {
            TimeSeries::from_vecs_minimal_checks(idx, values)
        }
    }

    pub fn from_vecs_minimal_checks(timeindicies: HashableIndex<TDate>, values: Vec<T>) -> Result<TimeSeries<TDate, T>, std::io::Error> {
        if timeindicies.len() != values.len() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "length mismatch",
            ))
        } else {
            Ok(TimeSeries::from_vecs_unchecked(timeindicies, values))

        }
    }

    pub fn from_vecs_unchecked(timeindicies: HashableIndex<TDate>, values: Vec<T>) -> TimeSeries<TDate, T> {
        TimeSeries::<TDate,T> {
            timeindicies,
            values
        }
    }

    /// Create a new series from a set of TimeSeriesDataPoints
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    ///
    /// let data = vec![TimeSeriesDataPoint::new(1, 1.0),
    ///                 TimeSeriesDataPoint::new(2, 2.0),
    ///                 TimeSeriesDataPoint::new(3, 3.0),
    ///                 TimeSeriesDataPoint::new(4, 4.0),
    ///                 TimeSeriesDataPoint::new(5, 5.0)];
    /// let ts = TimeSeries::from_tsdatapoints(data);
    /// assert_eq!(ts.unwrap().len(), 5);
    /// ```
    pub fn from_tsdatapoints(tsdatapoints: Vec<TimeSeriesDataPoint<TDate,T>>) -> Result<TimeSeries<TDate, T>, std::io::Error> {
        let mut dpc = tsdatapoints;
        dpc.sort_by_key(|x| x.timestamp.clone());
        let len = dpc.len();
        let mut index= Vec::with_capacity(len);
        let mut values = Vec::with_capacity(len);
        dpc.iter().for_each(|dp|{
            index.push(dp.timestamp.clone());
            values.push(dp.value.clone());
        });
        TimeSeries::from_vecs(index,values)
    }
    pub fn from_tsdatapoints_unchecked(tsdatapoints: Vec<TimeSeriesDataPoint<TDate,T>>) -> TimeSeries<TDate, T> {
        let len =  tsdatapoints.len();
        let mut index= Vec::with_capacity(len);
        let mut values = Vec::with_capacity(len);
        tsdatapoints.iter().for_each(|dp|{
            index.push(dp.timestamp.clone());
            values.push(dp.value.clone());
        });

        TimeSeries::from_vecs_unchecked(HashableIndex::new(index),values)
    }

    /// Get the length of a series
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    ///
    /// let index = vec![1, 2, 3, 4, 5];
    /// let data = vec![1.0, 2.0, 3.0, 4.0, 3.0];
    /// let ts = TimeSeries::from_vecs(index, data).unwrap();
    /// assert_eq!(ts.len(), 5);
    /// ```
    pub fn len(&self) -> usize {
        self.timeindicies.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timeindicies.is_empty()
    }

    /// index into the series by position, returns None if not found
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    ///
    /// let index = vec![1, 2, 3, 4, 5];
    /// let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let ts = TimeSeries::from_vecs(index, data).unwrap();
    /// assert_eq!(ts.at_idx_of(1), Some(TimeSeriesDataPoint::new(2, 2.0)));
    /// assert_eq!(ts.at_idx_of(10), None);
    /// ```
    pub fn at_idx_of(&self, pos: usize) -> Option<TimeSeriesDataPoint<TDate,T>> {
        if pos < self.len() {
            Some(TimeSeriesDataPoint::new(self.timeindicies[pos].clone(),self.values[pos].clone()))
        } else {
            None
        }
    }
    /// Return element by its timestamp index or none
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::{NaiveDateTime};
    ///
    /// let index = vec![NaiveDateTime::from_timestamp(1,0), NaiveDateTime::from_timestamp(5,0), NaiveDateTime::from_timestamp(10,0)];
    /// let data = vec![1.0, 2.0, 3.0];
    /// let ts = TimeSeries::from_vecs(index, data).unwrap();
    /// assert_eq!(ts.at(NaiveDateTime::from_timestamp(0,0)), None);
    /// assert_eq!(ts.at(NaiveDateTime::from_timestamp(1,0)), Some(1.0));
    /// assert_eq!(ts.at(NaiveDateTime::from_timestamp(4,0)), None);
    /// assert_eq!(ts.at(NaiveDateTime::from_timestamp(6,0)), None);
    /// assert_eq!(ts.at(NaiveDateTime::from_timestamp(20,0)), None);
    /// ```

    pub fn at(&self, timestamp: TDate) -> Option<T> {
        match self.timeindicies.values.binary_search(&timestamp) {
            Ok(pos) => Some(self.values[pos].clone()),
            Err(_pos) => None
        }
    }

    /// Return element by its timestamp index or the first prior if out of range return none
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::{NaiveDateTime};
    ///
    /// let index = vec![NaiveDateTime::from_timestamp(1,0), NaiveDateTime::from_timestamp(5,0), NaiveDateTime::from_timestamp(10,0)];
    /// let data = vec![1.0, 2.0, 3.0];
    /// let ts = TimeSeries::from_vecs(index, data).unwrap();
    /// assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(0,0)), None);
    /// assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(1,0)), Some(1.0));
    /// assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(4,0)), Some(1.0));
    /// assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(6,0)), Some(2.0));
    /// assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(20,0)), None);
    /// ```
    pub fn at_or_first_prior(&self, timestamp: TDate) -> Option<T> {
        let maxts = self.timeindicies.last();
        let pos = match self.timeindicies.iter().position(|ts| timestamp < *ts) {
            Some(idx) => idx, // timespan out of range on the other end
            _ => {
                if maxts.is_some() && timestamp > *(maxts.unwrap()) {
                    0
                } else {
                    self.len()
                }
            }
        };
        if pos > 0 {
            Some(self.values[pos - 1].clone())
        } else {
            None
        }
    }


    pub fn into_ordered_iter(&self) -> OrderedTimeSeriesIter<TDate,T> {   #![allow(clippy::wrong_self_convention)]
        OrderedTimeSeriesIter::new(&self, 0)
    }

    pub fn ordered_iter(&self) -> OrderedTimeSeriesRefIter<TDate,T> {
        OrderedTimeSeriesRefIter::new(&self, 0)
    }
    /// Convert the series to an iterator
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    ///
    /// let values = vec![1.0, 2.0];
    /// let index = (0..values.len()).map(|i| i as i64).collect();        
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// assert_eq!(ts.into_iter().count(), 2);
    /// ```
    pub fn into_iter(&self) -> TimeSeriesIter<TDate,T> {   #![allow(clippy::wrong_self_convention)]
        TimeSeriesIter::new(&self, 0)
    }


    /// Convert the series to an iterator where the TDate and T are references rather than values
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    ///
    /// let values = vec![1.0, 2.0];
    /// let index = (0..values.len()).map(|i| i as i64).collect();        
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// assert_eq!(ts.iter().count(), 2);
    /// ```
    pub fn iter(&self) -> TimeSeriesRefIter<TDate,T> {
        TimeSeriesRefIter::new(&self, 0)
    }
    /// Get the values of a series between the start and end index (inclusive)
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::NaiveDateTime;
    ///
    ///let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    ///let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
    ///let ts = TimeSeries::from_vecs(index, values).unwrap();
    ///let tsres = ts.between(NaiveDateTime::from_timestamp(60 * 2 as i64,0), NaiveDateTime::from_timestamp(60 * 4 as i64,0));
    ///assert_eq!(tsres.len(), 3);
    /// ```
    pub fn between(&self, start: TDate, end: TDate) -> TimeSeries<TDate,T>{
        // this is really ugly but since you know the stuff is ordered you want to short circuit if you can
        let mut newdps: Vec<TimeSeriesDataPoint<TDate,T>> = Vec::new();
        let tsiter = self.into_iter();
        let mut i: usize = 0;
        for dp in tsiter {
            if dp.timestamp > end{
                break
            }
            if dp.timestamp < start{
                continue;
            }           
            newdps.insert(i, dp);
            i+=1;
        }
        TimeSeries::from_tsdatapoints_unchecked(newdps)
    }

    /// Resample a Timeseries to the target duration, taking values according to the specified agg function
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    /// use tsxlib::timeutils;
    /// use chrono::{Duration,NaiveDateTime};
    /// let data = vec![
    ///     TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 1 as i64,0), 2.0),
    ///     TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 2 as i64,0), 2.0),
    ///     TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 3 as i64,0), 5.0),
    ///     TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 16 as i64,0), 99.0),
    /// ];
    /// let tsin = TimeSeries::from_tsdatapoints(data).unwrap();
    /// 
    /// let ts_rounded_up = tsin
    ///     .resample_and_agg(Duration::minutes(15),
    ///                      |dt,dur| timeutils::round_up_to_nearest_duration(dt, dur), 
    ///                      |x| *x.last().unwrap().value);
    /// let expected = vec![
    ///     TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 15 as i64,0), 5.0),
    ///     TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 30 as i64,0), 99.0),
    /// ];
    /// let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
    /// assert_eq!(ts_rounded_up, ts_expected);
    /// ```
    pub fn resample_and_agg<TRes,TDuration>(&self, sample_size :TDuration, group_func: fn(&TDate,&TDuration)->TDate, agg_func: fn(&Vec<TimeSeriesDataPoint<&TDate,&T>>)->TRes ) -> TimeSeries<TDate,TRes>
    where TRes : Copy
    {
        // let mut groupmap: HashMap<TDate, Vec<TimeSeriesDataPoint<TDate,T>>> = HashMap::with_capacity(self.len());  
        // self.iter().for_each(|dp| {
        //     let key = group_func(&dp.timestamp,&sample_size);
        //     groupmap.entry(key).or_insert_with(Vec::new).push(dp);
        // });        
        // groupmap.iter().map(|(k,v)| TimeSeriesDataPoint::new(*k, agg_func(&v))).collect_from_unchecked_iter()

        // note here you are relying EXPLICTLY on the iterator being ordered. if it isnt then they will get buggy, see above for a reference implementation that is more relaxed (but slower)
        self.iter().group_by(|dp| group_func(&dp.timestamp,&sample_size)).into_iter().map(|grp|  TimeSeriesDataPoint::new(grp.0, agg_func(&grp.1.collect()))).collect_from_unchecked_iter()
    }

    /// Shift a series by a given index, i.e. a "shift" of 1 will lag the series by 1 obs while a "shift" of 1 will nudge it fwd by 1
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::NaiveDateTime;
    ///
    ///let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    ///let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
    ///let ts = TimeSeries::from_vecs(index, values).unwrap();
    ///let tsres = ts.between(NaiveDateTime::from_timestamp(60 * 2 as i64,0), NaiveDateTime::from_timestamp(60 * 4 as i64,0));
    ///assert_eq!(tsres.len(), 3);
    /// ```
    pub fn shift(&self, shift: isize) -> ShiftedTimeSeriesIter<TDate,T>{
        ShiftedTimeSeriesIter::new(&self, 0, shift)
    }

    pub fn apply_rolling<TRes>(&self, window_size: usize,transform_func: fn(&Vec<T>)->TRes) -> RollingTimeSeriesIter<TDate,T, TRes>
    where TRes : Clone
    {
        RollingTimeSeriesIter::new(&self, window_size, transform_func)
    }

    pub fn apply_updating_rolling<TRes>(&self, window_size: usize,update_func: fn(Option<TRes>, &T)->Option<TRes>, decrement_func: fn(Option<TRes>, &T)->Option<TRes>) -> RollingTimeSeriesIterWithUpdate<TDate,T, TRes>
    where TRes : Clone
    {
        RollingTimeSeriesIterWithUpdate::new(&self, window_size, update_func, decrement_func)
    }

    /// Map the desired UDF over elements of a series
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::NaiveDateTime;
    ///
    /// let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// let result = ts.map(|x| x * 2.0);
    /// assert_eq!(result.len(), 5);
    /// ```
    pub fn map<TRes>(&self, func: fn(&T)->TRes) ->  TimeSeries<TDate,TRes>
    where TRes : Clone + Default
    { #![allow(clippy::needless_range_loop)]
        let mut newvals:Vec<TRes> = Vec::with_capacity(self.values.len());
        newvals.resize_with(self.values.len(), Default::default);
        for i in 0..newvals.len() {
            newvals[i] = func(&self.values[i]);
        }
        TimeSeries::from_vecs_unchecked(self.timeindicies.clone(), newvals)
    }

    /// Map the desired UDF over elements of a series, keeping track of the date in addition to the value
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::NaiveDateTime;
    ///
    /// let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// let result = ts.map_with_date(|_dt,x| x * 2.0);
    /// assert_eq!(result.len(), 5);
    /// ```
    pub fn map_with_date<TRes>(&self, func: fn(&TDate,&T)->TRes) ->  TimeSeries<TDate,TRes>
    where TRes : Clone + Default
    { #![allow(clippy::needless_range_loop)]
        let mut newvals:Vec<TRes> = Vec::with_capacity(self.values.len());
        newvals.resize_with(self.values.len(), Default::default);
        for i in 0..newvals.len() {
            newvals[i] = func(&self.timeindicies[i],&self.values[i]);
        }
        TimeSeries::from_vecs_unchecked(self.timeindicies.clone(), newvals)
    }
    /// Apply a function that calculates its resultant value based on the begining and end of the specified skip span
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use chrono::NaiveDateTime;
    ///
    /// let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// fn change_func(prior: &f64, curr: &f64) -> f64{
    ///     curr - prior
    /// };
    /// let ts_difference: TimeSeries<NaiveDateTime,f64> = ts.skip_apply(1, change_func).collect();
    /// fn perc_change_func(prior: &f64, curr: &f64) -> f64{
    ///     (curr - prior)/prior
    /// };
    /// let ts_percent_change: TimeSeries<NaiveDateTime,f64> = ts.skip_apply(1, perc_change_func).collect();
    /// 
    /// ```
    pub fn skip_apply<TRes>(&self, skip_span: usize, transform_func: fn(&T,&T)->TRes) -> SkipApplyTimeSeriesIter<TDate,T, TRes>
    where TRes : Copy
    {
        SkipApplyTimeSeriesIter::new(&self, skip_span, transform_func)
    }
    /// inner join two series and apply the desired UDF
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    ///
    /// let values : Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let values2 : Vec<f64> = vec![1.0, 2.0, 4.0];
    /// let index: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
    /// let index2: Vec<i32> = (0..values2.len()).map(|i| i as i32).collect();
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// let ts1 = TimeSeries::from_vecs(index2, values2).unwrap();
    /// let tsres = ts.cross_apply_inner(&ts1,|a,b| (*a,*b));
    /// let expected = vec![
    ///     TimeSeriesDataPoint { timestamp: 0, value: (1.00, 1.00) },
    ///     TimeSeriesDataPoint { timestamp: 1, value: (2.00, 2.00) },
    ///     TimeSeriesDataPoint { timestamp: 2, value: (3.00, 4.00) },
    /// ];
    /// let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
    /// assert_eq!(ts_expected, tsres)
    /// ```
    pub fn cross_apply_inner<T2,T3>(&self, other: &TimeSeries<TDate,T2>, apply_func: fn(&T,&T2) -> T3) -> TimeSeries<TDate,T3>
    where 
        T2 : Clone, 
        T3 : Clone
    {
        let je = JoinEngine{idx_this : &self.timeindicies ,idx_other : &other.timeindicies};
        let indexes = je.get_inner_merge_joined_indicies();
        //can make this parallel if you want...
        indexes.iter().map(|x| TimeSeriesDataPoint { timestamp : self.timeindicies[x.this_idx].clone(), value : apply_func(&self.values[x.this_idx], &other.values[x.other_idx]) } ).collect()
    }

    /// Left join two series and apply the desired UDF
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    ///
    /// let values : Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let values2 : Vec<f64> = vec![1.0, 2.0, 4.0];
    /// let index: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
    /// let index2: Vec<i32> = (0..values2.len()).map(|i| i as i32).collect();
    /// let ts = TimeSeries::from_vecs(index, values).unwrap();
    /// let ts1 = TimeSeries::from_vecs(index2, values2).unwrap();
    /// let tsres = ts.cross_apply_left(&ts1,|a,b| (*a, match b { Some(v) => Some(*v), _ => None }));
    /// let expected = vec![
    ///     TimeSeriesDataPoint { timestamp: 0, value: (1.00, Some(1.00)) },
    ///     TimeSeriesDataPoint { timestamp: 1, value: (2.00, Some(2.00)) },
    ///     TimeSeriesDataPoint { timestamp: 2, value: (3.00, Some(4.0)) },
    ///     TimeSeriesDataPoint { timestamp: 3, value: (4.00, None) },
    ///     TimeSeriesDataPoint { timestamp: 4, value: (5.00, None) },
    /// ];
    /// let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
    /// assert_eq!(ts_expected, tsres)
    /// ```
    pub fn cross_apply_left<T2,T3>(&self, other: &TimeSeries<TDate,T2>, apply_func: fn(&T,Option<&T2>) -> T3) -> TimeSeries<TDate,T3>
    where 
        T2 : Clone , 
        T3 : Clone + fmt::Debug
    {
        let je = JoinEngine{idx_this : &self.timeindicies ,idx_other : &other.timeindicies};
        let indexes = je.get_left_merge_joined_indicies();
        //can make this parallel if you want...
        indexes.iter().map(|x| 
            TimeSeriesDataPoint { 
                timestamp : self.timeindicies[x.this_idx].clone(), 
                value : apply_func(
                    &self.values[x.this_idx],  
                    match x.other_idx.is_some() {
                        true => Some(&other.values[x.other_idx.unwrap()]),
                        false => None
                    }
                )} )
                .collect()
    }
    /// This is similar to a left join except that it match on nearest key rather than equal keys similiar to <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.merge_asof.html>
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::{TimeSeries,MergeAsofMode};
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    /// use tsxlib::algo::chrono_utils;
    /// use chrono::{NaiveDateTime,Duration};
    /// let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
    /// let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];    
    /// let ts = TimeSeries::from_vecs(index.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values).unwrap();
    /// let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    /// let index2 = vec![2, 4, 5, 7, 8, 10];    
    /// let ts_join = TimeSeries::from_vecs(index2.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values2).unwrap();
    /// 
    /// let result = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_prior(Duration::seconds(1))),|a,b| (*a, match b {
    ///     Some(x) => Some(*x),
    ///     None => None
    /// }), MergeAsofMode::RollPrior);
    /// 
    /// let expected = vec![
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, None) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, Some(1.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, Some(2.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(3.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, Some(3.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, Some(4.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(5.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, Some(5.00)) },
    ///     TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(6.00)) },
    /// ];
    /// 
    /// let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
    /// 
    /// assert_eq!(result, ts_expected);
    /// ```
    pub fn merge_apply_asof<T2,T3>(&self, other: &TimeSeries<TDate,T2>, compare_func: Option<Box<dyn Fn(&TDate,&TDate,&TDate)->(cmp::Ordering,i64)>>, apply_func: fn(&T,Option<&T2>) -> T3,merge_mode :MergeAsofMode) -> TimeSeries<TDate,T3>
    where 
        T2 : Clone, 
        T3 : Clone
    { #![allow(clippy::type_complexity)] #![allow(clippy::redundant_closure)]
        match merge_mode {
            MergeAsofMode::NoRoll if  compare_func.is_some() => panic!("you cannot have a roll function if you do not set a merge as of mode"),
            _ => ()
        };

        let je = JoinEngine{idx_this : &self.timeindicies ,idx_other : &other.timeindicies};
        
        let other_idx_func:Option<Box<dyn Fn(usize)->usize>> = match merge_mode {
            MergeAsofMode::RollFollowing => {
                let otherlen = other.timeindicies.len();
                Some(Box::new(move |idx: usize| crate::joins::fwd_func(idx, otherlen)))
            },
            MergeAsofMode::RollPrior => Some(Box::new(|idx: usize| crate::joins::prior_func(idx))),
            MergeAsofMode::NoRoll => None
        };
        let indexes = je.get_asof_merge_joined_indicies(compare_func,other_idx_func);
        //can make this parallel if you want...
        indexes.iter().map(|x| 
            TimeSeriesDataPoint { 
                timestamp : self.timeindicies[x.this_idx].clone(), 
                value : apply_func(
                    &self.values[x.this_idx],  
                    match x.other_idx.is_some() {
                        true => Some(&other.values[x.other_idx.unwrap()]),
                        false => None
                    }
                )} )
        .collect()
    }


    /// Interweave series. If a set of points happens to match then the selec_func is used to pick (or generate one)
    ///
    /// # Example
    ///
    /// ```
    /// use tsxlib::timeseries::TimeSeries;
    /// use tsxlib::data_elements::TimeSeriesDataPoint;
    /// let data1 = vec![
    ///     TimeSeriesDataPoint::from_int_stamp(1, 1.0),
    ///     TimeSeriesDataPoint::from_int_stamp(2, 2.0),
    ///     TimeSeriesDataPoint::from_int_stamp(3, 3.0),
    ///     TimeSeriesDataPoint::from_int_stamp(4, 4.0),
    ///     TimeSeriesDataPoint::from_int_stamp(5, 5.0),
    /// ];
    /// let data2 = vec![
    ///     TimeSeriesDataPoint::from_int_stamp(4, 6.0),
    ///     TimeSeriesDataPoint::from_int_stamp(5, 7.0),
    ///     TimeSeriesDataPoint::from_int_stamp(6, 8.0),
    ///     TimeSeriesDataPoint::from_int_stamp(7, 9.0),
    ///     TimeSeriesDataPoint::from_int_stamp(8, 10.0),
    /// ];
    /// let expected = vec![
    ///     TimeSeriesDataPoint::from_int_stamp(1, 1.0),
    ///     TimeSeriesDataPoint::from_int_stamp(2, 2.0),
    ///     TimeSeriesDataPoint::from_int_stamp(3, 3.0),
    ///     TimeSeriesDataPoint::from_int_stamp(4, 4.0),
    ///     TimeSeriesDataPoint::from_int_stamp(5, 5.0),
    ///     TimeSeriesDataPoint::from_int_stamp(6, 8.0),
    ///     TimeSeriesDataPoint::from_int_stamp(7, 9.0),
    ///     TimeSeriesDataPoint::from_int_stamp(8, 10.0),
    /// ];
    /// let ts1 = TimeSeries::from_tsdatapoints(data1).unwrap();
    /// let ts2 = TimeSeries::from_tsdatapoints(data2).unwrap();
    /// let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
    /// let ts_merged = ts1.interweave(&ts2,|left,_right| left);
    /// assert_eq!(ts_merged, ts_expected);
    /// ```    
    pub fn interweave(&self, other: &TimeSeries<TDate,T>, selec_func: fn(TimeSeriesDataPoint<TDate,T>,TimeSeriesDataPoint<TDate,T>)->TimeSeriesDataPoint<TDate,T>  ) -> TimeSeries<TDate,T> {  #![allow(clippy::type_complexity)]
        let mut output: Vec<TimeSeriesDataPoint<TDate,T>> = Vec::new();
        let mut pos1 = 0;
        let mut pos2 = 0;

        while pos1 < self.len() || pos2 < other.len() {
            if pos1 == self.len() {
                output.push(other.at_idx_of(pos2).unwrap());
                pos2 += 1;
            } else if pos2 == other.len() {
                output.push(self.at_idx_of(pos1).unwrap());
                pos1 += 1;
            } else {
                let dp1 = self.at_idx_of(pos1).unwrap();
                let dp2 = other.at_idx_of(pos2).unwrap();
                match dp1.timestamp.cmp(&dp2.timestamp) {
                    cmp::Ordering::Greater => {
                        output.push(dp2);
                        pos2 += 1;
                    },
                    cmp::Ordering::Less => {
                        output.push(dp1);
                        pos1 += 1;
                    },
                    cmp::Ordering::Equal => {
                        let chosen_one = selec_func(dp1,dp2);
                        output.push(chosen_one);
                        pos1 += 1;
                        pos2 += 1;
                    }
                }
            }
        }

        TimeSeries::from_tsdatapoints(output).unwrap()
    }
}


impl<TDate: Serialize + Hash + Clone + cmp::Eq + cmp::Ord, T: Clone> FromIterator<TimeSeriesDataPoint<TDate,T>> for TimeSeries<TDate,T> {
    fn from_iter<Tin>(iter: Tin) -> Self
    where
        Tin: IntoIterator<Item = TimeSeriesDataPoint<TDate,T>>,
    {
        TimeSeries::from_tsdatapoints(iter.into_iter().collect()).unwrap()
    }
}


impl<TDate: Serialize + fmt::Display + Hash + Clone + cmp::Eq + cmp::Ord, T: fmt::Display + Clone> fmt::Display for TimeSeries<TDate,T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.len() < 10 {
            self.iter().for_each(|dp| {
                let _ = writeln!(f, "({}, {})", dp.timestamp, dp.value);
            });
        } else {
            self.iter().take(5).for_each(|dp| {
                let _ = writeln!(f, "({}, {})", dp.timestamp, dp.value);
            });
            let _ = writeln!(f, "...\n");
            self.iter().skip(self.len() - 5).for_each(|dp| {
                let _ = writeln!(f, "({}, {})", dp.timestamp, dp.value);
            });
        }
        writeln!(f)
    }
}

impl<TDate: Serialize + Hash + Clone + cmp::Eq + cmp::Ord, T: Clone + cmp::PartialEq> cmp::PartialEq for TimeSeries<TDate, T> {
    fn eq(&self, other: &Self) -> bool {
        self.timeindicies == other.timeindicies && self.values == other.values
    }
}



/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {

    use super::*;
    use chrono::{NaiveDateTime,Duration};
    use crate::timeutils;
    use crate::algo::int_utils;
    use crate::algo::chrono_utils;
    
    #[test]
    fn test_construction() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp( i as i64,0)).collect();
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        assert_eq!(ts.len(), 5);
    }

    #[test]
    fn test_between() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let tsres = ts.between(NaiveDateTime::from_timestamp(60 * 2 as i64,0), NaiveDateTime::from_timestamp(60 * 4 as i64,0));
        assert_eq!(tsres.len(), 3);
    }

    #[test]
    fn test_new_different_lengths() {
        let values = vec![1.0, 2.0, 3.0];
        let index = vec![1, 2, 3, 4, 5];
        let ts = TimeSeries::from_vecs(index.iter().map(|x| NaiveDateTime::from_timestamp((x.clone()) as i64,0)).collect(), values);

        let result = ts.map_err(|e| e.kind());
        let expected = Err(std::io::ErrorKind::InvalidData);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_data_conversion() {
        let data = vec![
            TimeSeriesDataPoint::from_int_stamp(1, 1.0),
            TimeSeriesDataPoint::from_int_stamp(2, 2.0),
            TimeSeriesDataPoint::from_int_stamp(3, 3.0),
            TimeSeriesDataPoint::from_int_stamp(4, 4.0),
            TimeSeriesDataPoint::from_int_stamp(5, 5.0),
        ];
        let ts = TimeSeries::from_tsdatapoints(data);
        assert_eq!(ts.unwrap().len(), 5);

    }
    
    #[test]
    fn test_from_malformed_and_unchecked() {
        let data = vec![
            TimeSeriesDataPoint::from_int_stamp(1, 1.0),
            TimeSeriesDataPoint::from_int_stamp(2, 2.0),
            TimeSeriesDataPoint::from_int_stamp(3, 3.0),
            TimeSeriesDataPoint::from_int_stamp(4, 4.0),
            TimeSeriesDataPoint::from_int_stamp(0, 5.0),
        ];
        let ts1: TimeSeries<NaiveDateTime, f64> = TimeSeries::from_tsdatapoints_unchecked(data);
        let ts_order_enforced: TimeSeries<NaiveDateTime, f64>  = ts1.into_ordered_iter().collect_from_unchecked_iter();
        let ts_raw_iter: TimeSeries<NaiveDateTime, f64>  = ts1.into_iter().collect_from_unchecked_iter();
        //println!("{:.2?}",ts1);
        assert_eq!(ts_order_enforced.len(), 4);
        assert_eq!(ts_raw_iter.len(), 5);

    }

    #[test]
    fn test_from_data_increasing() {
        let data = vec![
            TimeSeriesDataPoint::from_int_stamp(1, 1.0),
            TimeSeriesDataPoint::from_int_stamp(2, 2.0),
            TimeSeriesDataPoint::from_int_stamp(3, 3.0),
            TimeSeriesDataPoint::from_int_stamp(4, 4.0),
            TimeSeriesDataPoint::from_int_stamp(3, 5.0),
        ];
        let ts = TimeSeries::from_tsdatapoints(data);
        assert_eq!(ts.is_err(), true);
    }

    #[test]
    fn test_apply() {
        fn multx2(dp: TimeSeriesDataPoint<NaiveDateTime, f64>) -> TimeSeriesDataPoint<NaiveDateTime, f64> {
            TimeSeriesDataPoint::new(
                dp.timestamp,
                2.0 * dp.value
            )
        }
        let values = vec![1.0, 2.5, 4.0];
        let expected_values = vec![2.0, 5.0, 8.0];
        let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(i as i64,0)).collect();
        let index_expected = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(i as i64,0)).collect();
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let ts_expected = TimeSeries::from_vecs(index_expected, expected_values).unwrap();
        let ts_out: TimeSeries<NaiveDateTime, f64> = ts.into_iter().map(multx2).collect();
        assert_eq!(ts_out, ts_expected);
    }
    #[test]
    fn test_resample() {
        let data = vec![
            TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 1 as i64,0), 2.0),
            TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 2 as i64,0), 2.0),
            TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 3 as i64,0), 5.0),
            TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 16 as i64,0), 99.0),
        ];
        let tsin = TimeSeries::from_tsdatapoints(data).unwrap();

        let ts_rounded_up = tsin
            .resample_and_agg(Duration::minutes(15),
                             |dt,dur| timeutils::round_up_to_nearest_duration(dt, dur), 
                             |x| *x.last().unwrap().value);
        let expected = vec![
            TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 15 as i64,0), 5.0),
            TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(60 * 30 as i64,0), 99.0),
        ];
        let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
        assert_eq!(ts_rounded_up, ts_expected);
    }


    #[test]
    fn test_interweave() {
        let data1 = vec![
            TimeSeriesDataPoint::from_int_stamp(1, 1.0),
            TimeSeriesDataPoint::from_int_stamp(2, 2.0),
            TimeSeriesDataPoint::from_int_stamp(3, 3.0),
            TimeSeriesDataPoint::from_int_stamp(4, 4.0),
            TimeSeriesDataPoint::from_int_stamp(5, 5.0),
        ];
        let data2 = vec![
            TimeSeriesDataPoint::from_int_stamp(4, 6.0),
            TimeSeriesDataPoint::from_int_stamp(5, 7.0),
            TimeSeriesDataPoint::from_int_stamp(6, 8.0),
            TimeSeriesDataPoint::from_int_stamp(7, 9.0),
            TimeSeriesDataPoint::from_int_stamp(8, 10.0),
        ];
        let expected = vec![
            TimeSeriesDataPoint::from_int_stamp(1, 1.0),
            TimeSeriesDataPoint::from_int_stamp(2, 2.0),
            TimeSeriesDataPoint::from_int_stamp(3, 3.0),
            TimeSeriesDataPoint::from_int_stamp(4, 4.0),
            TimeSeriesDataPoint::from_int_stamp(5, 5.0),
            TimeSeriesDataPoint::from_int_stamp(6, 8.0),
            TimeSeriesDataPoint::from_int_stamp(7, 9.0),
            TimeSeriesDataPoint::from_int_stamp(8, 10.0),
        ];
        let ts1 = TimeSeries::from_tsdatapoints(data1).unwrap();
        let ts2 = TimeSeries::from_tsdatapoints(data2).unwrap();
        let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
        let ts_merged = ts1.interweave(&ts2,|left,_right| left);
        assert_eq!(ts_merged, ts_expected);
    }
    #[test]
    fn test_merge_asof_lookingback(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];    
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let index2 = vec![2, 4, 5, 7, 8, 10];    
        let ts_join = TimeSeries::from_vecs(index2, values2).unwrap();
        let joinedasof = ts.merge_apply_asof(&ts_join,None,|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::NoRoll);


        let joinedasof_custom = ts.merge_apply_asof(&ts_join,Some(int_utils::merge_asof_prior(1)),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollPrior);

        let joinedasof_custom2 = ts.merge_apply_asof(&ts_join,Some(int_utils::merge_asof_prior(2)),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollPrior);



        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        let expected2 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        let expected3 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
        ];

        let ts_expected1 = TimeSeries::from_tsdatapoints(expected1).unwrap();
        let ts_expected2 = TimeSeries::from_tsdatapoints(expected2).unwrap();
        let ts_expected3 = TimeSeries::from_tsdatapoints(expected3).unwrap();

        assert_eq!(joinedasof, ts_expected1);
        assert_eq!(joinedasof_custom, ts_expected2);
        assert_eq!(joinedasof_custom2, ts_expected3);

        // joinedasof.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other");
        // joinedasof_custom.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other2");
        // joinedasof_custom2.iter().for_each(|x|println!("{:.2?}",x));

    }

    #[test]
    fn test_merge_asof_lookingforward(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];    
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index2 = vec![2, 5, 6, 8, 10];    
        let ts_join = TimeSeries::from_vecs(index2, values2).unwrap();
        let joinedasof = ts.merge_apply_asof(&ts_join,None,|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::NoRoll);


        let joinedasof_custom = ts.merge_apply_asof(&ts_join,Some(int_utils::merge_asof_fwd(1)),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollFollowing);


        let joinedasof_custom2 = ts.merge_apply_asof(&ts_join,Some(int_utils::merge_asof_fwd(2)),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollFollowing);



        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        let expected2 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        let expected3 = vec![
            TimeSeriesDataPoint { timestamp: 1, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(5.00)) },
        ];

        let ts_expected1 = TimeSeries::from_tsdatapoints(expected1).unwrap();
        let ts_expected2 = TimeSeries::from_tsdatapoints(expected2).unwrap();
        let ts_expected3 = TimeSeries::from_tsdatapoints(expected3).unwrap();

        assert_eq!(joinedasof, ts_expected1);
        assert_eq!(joinedasof_custom, ts_expected2);
        assert_eq!(joinedasof_custom2, ts_expected3);

        // joinedasof.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other");
        // joinedasof_custom.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other2");
        // joinedasof_custom2.iter().for_each(|x|println!("{:.2?}",x));

    }

    #[test]
    fn test_naivedatetime_merge_asof_lookingback(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];    
        let ts = TimeSeries::from_vecs(index.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values).unwrap();
        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let index2 = vec![2, 4, 5, 7, 8, 10];    
        let ts_join = TimeSeries::from_vecs(index2.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values2).unwrap();
        let joinedasof = ts.merge_apply_asof(&ts_join,None,|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::NoRoll);


        let joinedasof_custom = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_prior(Duration::seconds(1))),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollPrior);

        let joinedasof_custom2 = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_prior(Duration::seconds(2))),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollPrior);



        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(6.00)) },
        ];

        let expected2 = vec![
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(6.00)) },
        ];

        let expected3 = vec![
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(6.00)) },
        ];

        let ts_expected1 = TimeSeries::from_tsdatapoints(expected1).unwrap();
        let ts_expected2 = TimeSeries::from_tsdatapoints(expected2).unwrap();
        let ts_expected3 = TimeSeries::from_tsdatapoints(expected3).unwrap();

        assert_eq!(joinedasof, ts_expected1);
        assert_eq!(joinedasof_custom, ts_expected2);
        assert_eq!(joinedasof_custom2, ts_expected3);

        // joinedasof.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other");
        // joinedasof_custom.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other2");
        // joinedasof_custom2.iter().for_each(|x|println!("{:.2?}",x));

    }

    #[test]
    fn test_naivedatetime_merge_asof_lookingforward(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];    
        let ts = TimeSeries::from_vecs(index.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values).unwrap();
        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index2 = vec![2, 5, 6, 8, 10];    
        let ts_join = TimeSeries::from_vecs(index2.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values2).unwrap();
        let joinedasof = ts.merge_apply_asof(&ts_join,None,|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::NoRoll);


        let joinedasof_custom = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_fwd(Duration::seconds(1))),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollFollowing);


        let joinedasof_custom2 = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_fwd(Duration::seconds(2))),|a,b| (*a, match b {
            Some(x) => Some(*x),
            None => None
        }), MergeAsofMode::RollFollowing);



        let expected1 = vec![
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(5.00)) },
        ];

        let expected2 = vec![
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, None) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(5.00)) },
        ];

        let expected3 = vec![
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(1,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(2,0), value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(3,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(4,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(5,0), value: (1.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(6,0), value: (1.00, Some(3.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(7,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(8,0), value: (1.00, Some(4.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(9,0), value: (1.00, Some(5.00)) },
            TimeSeriesDataPoint { timestamp: NaiveDateTime::from_timestamp(10,0), value: (1.00, Some(5.00)) },
        ];

        let ts_expected1 = TimeSeries::from_tsdatapoints(expected1).unwrap();
        let ts_expected2 = TimeSeries::from_tsdatapoints(expected2).unwrap();
        let ts_expected3 = TimeSeries::from_tsdatapoints(expected3).unwrap();

        assert_eq!(joinedasof, ts_expected1);
        assert_eq!(joinedasof_custom, ts_expected2);
        assert_eq!(joinedasof_custom2, ts_expected3);

        // joinedasof.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other");
        // joinedasof_custom.iter().for_each(|x|println!("{:.2?}",x));
        // println!("other2");
        // joinedasof_custom2.iter().for_each(|x|println!("{:.2?}",x));

    }
    #[test]
    fn test_left_join(){
        let values : Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let values2 : Vec<f64> = vec![1.0, 2.0, 4.0];
        let index: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
        let index2: Vec<i32> = (0..values2.len()).map(|i| i as i32).collect();
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let ts1 = TimeSeries::from_vecs(index2, values2).unwrap();
        let tsres = ts.cross_apply_left(&ts1,|a,b| (*a, match b { Some(v) => Some(*v), _ => None }));
        let expected = vec![
            TimeSeriesDataPoint { timestamp: 0, value: (1.00, Some(1.00)) },
            TimeSeriesDataPoint { timestamp: 1, value: (2.00, Some(2.00)) },
            TimeSeriesDataPoint { timestamp: 2, value: (3.00, Some(4.0)) },
            TimeSeriesDataPoint { timestamp: 3, value: (4.00, None) },
            TimeSeriesDataPoint { timestamp: 4, value: (5.00, None) },
        ];
        let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
        assert_eq!(ts_expected, tsres)
    }

}
