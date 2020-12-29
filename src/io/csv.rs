use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::cmp;
use std::hash::Hash;

use crate::{data_elements::TimeSeriesDataPoint, timeseries::TimeSeries};


/// Load series from the given CSV file
pub fn read_from_file<TDate,T,TRecord>(file_path: &str, datapoint_gen_func: fn(TRecord)->TimeSeriesDataPoint<TDate,T>) -> Result<TimeSeries<TDate,T>, Box<dyn Error>> 
where 
    TDate: Serialize + Hash + Copy + cmp::Eq + cmp::Ord, 
    T: Copy,
    TRecord: DeserializeOwned + 'static 
{
    let mut rdr = csv::Reader::from_path(file_path)?;
    let mut data: Vec<TimeSeriesDataPoint<TDate,T>> = Vec::new();

    for result in rdr.deserialize() {
        let record: TimeSeriesDataPoint<TDate,T> = datapoint_gen_func(result?);
        data.push(record);
    }

    Ok(TimeSeries::from_tsdatapoints_unchecked(data))
}

pub fn read_from_file_simple<TDate,T>(file_path: &str) -> Result<TimeSeries<TDate,T>, Box<dyn Error>> 
where 
    TDate: DeserializeOwned + 'static  + Serialize + Hash + Copy + cmp::Eq + cmp::Ord, 
    T: DeserializeOwned + 'static  + Copy
{
    read_from_file(file_path,|tsdp|tsdp)
}

/// Save series as CSV file
pub fn write_to_file<TDate,T,TRecord>(file_path: &str, ts: &TimeSeries<TDate,T>, record_gen_func: fn(TimeSeriesDataPoint<TDate,T>) ->TRecord ) -> Result<(), Box<dyn Error>> 
where 
    TDate: Serialize + Hash + Copy + cmp::Eq + cmp::Ord, 
    T: Copy,
    TRecord: Serialize
{
    let mut wtr = csv::Writer::from_path(file_path)?;
    for tsdp in ts.into_iter(){
        wtr.serialize(&record_gen_func(tsdp)).unwrap();
    }
    wtr.flush()?;
    Ok(())
}
/// Simple wrapper to save a timeseries to a csv, does not work when T is a nonprimitive type
pub fn write_to_file_simple<TDate,T>(file_path: &str, ts: &TimeSeries<TDate,T>) -> Result<(), Box<dyn Error>> 
where 
    TDate: Serialize + Hash + Copy + cmp::Eq + cmp::Ord, 
    T: Serialize + Copy
{
    write_to_file(file_path,ts,|tsdp|tsdp)
}



/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use serde::Deserialize;
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_readsimple() {
        let before = Instant::now();
        let ts: Result<TimeSeries<NaiveDateTime,f64>, Box<dyn Error>>  = read_from_file_simple("testdata/large_justdoubles.csv"); //, "%Y-%m-%d %H:%M:%S%z"
        println!("CSV Read Elapsed time: {:.2?}", before.elapsed());
        assert_eq!(ts.unwrap().len(), 999997);
    }
    #[test]
    fn test_read_withstruct() {


        #[derive(Clone,Copy,Deserialize,Serialize)]
        struct SimpleStruct{
            pub intthing: i64,
            pub floatvalue: f64
        };
        #[derive(Clone,Copy,Deserialize,Serialize)]
        struct SimpleStructCSVDTO{
            pub timestamp: NaiveDateTime,
            pub intthing: i64,
            pub floatvalue: f64
        };

        let before = Instant::now();
        let ts: Result<TimeSeries<NaiveDateTime,SimpleStruct>, Box<dyn Error>>  = read_from_file("testdata/large_struct.csv",|sscdto:SimpleStructCSVDTO| TimeSeriesDataPoint::new(sscdto.timestamp,SimpleStruct{intthing:sscdto.intthing,floatvalue:sscdto.floatvalue}) ); //, "%Y-%m-%d %H:%M:%S%z"
        println!("CSV Read Elapsed time: {:.2?}", before.elapsed());
        assert_eq!(ts.unwrap().len(), 999997);
    }
}

