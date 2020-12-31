//! # Data Streaming Iterators
use std::error::Error;
use std::cmp;
use std::fmt;
use std::hash::Hash;
use std::io::{Read,Cursor};
use serde::{Serialize};
use std::sync::mpsc;
use crate::data_elements::TimeSeriesDataPoint;


pub struct TimeSeriesDataPointStreamer<'a, T: Read, TDate: Hash + Copy + cmp::Eq + cmp::Ord, TDp: fmt::Display + Copy + cmp::PartialEq> {
    source: &'a mut T,
    production_function: fn(&[u8]) -> TimeSeriesDataPoint<TDate,TDp>
}


impl<'a, T: Read, TDate: Hash + Copy + cmp::Eq + cmp::Ord, TDp: fmt::Display + Copy + cmp::PartialEq> Iterator for TimeSeriesDataPointStreamer<'a, T,TDate,TDp> {
    type Item = TimeSeriesDataPoint<TDate,TDp>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = [0; 1024*1024]; //TODO ideally replace this once const generics are available
        let res = self.source.read(&mut buffer);
        let produce_tsdp = self.production_function;
        match res {
            Ok(count) => {
            if count > 0 {
                Some(produce_tsdp(&buffer[..count]))
            } else {
                None
            }
            },
            Err(_e) => None,
        }
    }
}

impl<'a, T: Read, TDate: Hash + Copy + cmp::Eq + cmp::Ord, TDp: fmt::Display + Copy + cmp::PartialEq> TimeSeriesDataPointStreamer<'a, T,TDate,TDp>{
    pub fn new(source: &'a mut T, production_function: fn(&[u8])->TimeSeriesDataPoint<TDate,TDp>) -> TimeSeriesDataPointStreamer<'a, T,TDate,TDp>{
        TimeSeriesDataPointStreamer {
            source,
            production_function,
        }
    }
}

pub struct TimeSeriesDataPointReceiver<'a, TDate: Hash + Copy + cmp::Eq + cmp::Ord, TDp: fmt::Display + Copy + cmp::PartialEq> {
    source: &'a mut mpsc::Receiver<TimeSeriesDataPoint<TDate,TDp>>
}


impl<'a, TDate: Hash + Copy + cmp::Eq + cmp::Ord, TDp: fmt::Display + Copy + cmp::PartialEq> Iterator for TimeSeriesDataPointReceiver<'a, TDate,TDp> {
    type Item = TimeSeriesDataPoint<TDate,TDp>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.source.recv();
        match res {
            Ok(tdp) => Some(tdp),
            Err(_e) => None,
        }
    }
}

impl<'a, TDate: Hash + Copy + cmp::Eq + cmp::Ord, TDp: fmt::Display + Copy + cmp::PartialEq> TimeSeriesDataPointReceiver<'a,TDate,TDp>{
    pub fn new(source: &'a mut mpsc::Receiver<TimeSeriesDataPoint<TDate,TDp>>) -> TimeSeriesDataPointReceiver<'a, TDate,TDp>{
        TimeSeriesDataPointReceiver {
            source,
        }
    }
}



type ProdResult = std::result::Result<std::vec::Vec<u8>, Box<dyn Error>>;
pub struct TimeSeriesBytesStreamer<'a, TDate: Hash + Copy + cmp::Eq + cmp::Ord + Serialize, T: fmt::Display + Copy + cmp::PartialEq + Serialize>{    

    source: &'a mut dyn Iterator<Item=TimeSeriesDataPoint<TDate,T>>,
    production_function: fn(&TimeSeriesDataPoint<TDate,T>) -> ProdResult
}

impl <'a, TDate: Hash + Copy + cmp::Eq + cmp::Ord + Serialize, T: fmt::Display + Copy + cmp::PartialEq + Serialize> Read for TimeSeriesBytesStreamer<'a,TDate,T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error>{
        if let Some(dp) = self.source.next() {
            let func = self.production_function;
            let res = func(&dp);

            if let Ok(bytes) = res{
                let len = bytes.len();
                let mut file = Cursor::new(bytes);
                let _ = file.read(buf);
                Ok(len)
            }
            else{
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "out of data",))
            }
        }
        else{
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "out of data",
            ))
        }
    }   
}


/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDateTime};
    use crate::timeseries::TimeSeries;


    use std::thread;

    #[test]
    fn test_naive_producer_consumer() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let tscopy = ts.clone();
        let (sender, receiver): (mpsc::Sender<TimeSeriesDataPoint<NaiveDateTime,f64>>,mpsc::Receiver<TimeSeriesDataPoint<NaiveDateTime,f64>>) = mpsc::channel();
        thread::spawn(move || {
            ts.into_ordered_iter().for_each( |dp| {
            thread::sleep(std::time::Duration::from_secs(1));
            sender.send(dp).unwrap();
        });});

        let mut channel_reciever = receiver.iter();

        fn prod_func(x: &TimeSeriesDataPoint<NaiveDateTime,f64>) -> Result<Vec<u8>,Box<dyn Error>> {
            let now = std::time::Instant::now();
            println!("{:.2?}",now.elapsed());
            let ser = bincode::serialize(x);
            match ser {
                Ok(ser) => Ok(ser),
                Err(e) => Err(e)                
            }
        }

        let mut streamer =  TimeSeriesBytesStreamer{source: &mut channel_reciever, production_function: prod_func};
        fn gen_dp(x: &[u8] ) -> TimeSeriesDataPoint<NaiveDateTime,f64>{
            println!("{:.2?}",x);
            bincode::deserialize::<TimeSeriesDataPoint<NaiveDateTime,f64>>(x).unwrap()
        }
        
        let consumer = TimeSeriesDataPointStreamer{source:&mut streamer, production_function: gen_dp};
        let res: TimeSeries<NaiveDateTime,f64> = consumer.collect();
        println!("{:.2?}",res);
        assert_eq!(res, tscopy);
    }
    #[test]
    fn test_reciever_consumer() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
        let ts = TimeSeries::from_vecs(index, values).unwrap();
        let tscopy = ts.clone();
        let (sender, mut receiver): (mpsc::Sender<TimeSeriesDataPoint<NaiveDateTime,f64>>,mpsc::Receiver<TimeSeriesDataPoint<NaiveDateTime,f64>>) = mpsc::channel();
        thread::spawn(move || {
            ts.into_ordered_iter().for_each( |dp| {
            thread::sleep(std::time::Duration::from_secs(1));
            sender.send(dp).unwrap();
        });});
        
        let consumer = TimeSeriesDataPointReceiver::new(&mut receiver);
        let res: TimeSeries<NaiveDateTime,f64> = consumer.collect();
        println!("{:.2?}",res);
        assert_eq!(res, tscopy);
    }



}