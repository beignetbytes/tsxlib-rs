use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::cmp;
use std::hash::Hash;



use crate::{data_elements::TimeSeriesDataPoint, timeseries::TimeSeries};

pub fn read_from_file<TDate,T>(file_path: &str) -> Result<TimeSeries<TDate,T>, Box<dyn Error>> 
where 
    TDate: DeserializeOwned + 'static + Serialize + Hash + Copy + cmp::Eq + cmp::Ord, 
    T: DeserializeOwned + 'static + Copy 
{
    let path = std::path::Path::new(file_path);
    let file = std::fs::File::open(&path).unwrap();
    let rdr = std::io::BufReader::new(file);
    let data: Vec<TimeSeriesDataPoint<TDate,T>> = serde_json::from_reader(rdr)?;
    Ok(TimeSeries::from_tsdatapoints_unchecked(data).unwrap())
}

pub enum JSONStyle{ Default, Pretty}

pub fn write_to_file<TDate,T>(file_path: &str, ts: &TimeSeries<TDate,T>, jsonstyle: JSONStyle ) -> Result<(), Box<dyn Error>> 
where 
    TDate: Serialize + Hash + Copy + cmp::Eq + cmp::Ord, 
    T: Serialize + Copy,
{
    let vec: Vec<TimeSeriesDataPoint<TDate,T>> = ts.ordered_iter().collect();
    let path = std::path::Path::new(file_path);
    let wtr = &std::fs::File::create(&path)?;
    let res = match jsonstyle {
        JSONStyle::Default => serde_json::to_writer(wtr,&vec),
        JSONStyle::Pretty => serde_json::to_writer_pretty(wtr,&vec),
    };
    match res {
        Ok(_t) => Ok(()),
        Err(res) => Err(Box::new(res)) 
    }
}

/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;
    use serde::Deserialize;

    #[test]
    fn test_read_write() {
        #[derive(Clone,Copy,Deserialize,Serialize)]
        struct SimpleStruct{
            pub intthing: i64,
            pub floatvalue: f64
        };

        let ts: TimeSeries<NaiveDateTime,SimpleStruct> = read_from_file("testdata/large_struct.json").unwrap();
        let _ = write_to_file("testdata/large_struct.json", &ts, JSONStyle::Pretty);
        assert_eq!(500, ts.len());

    }
}