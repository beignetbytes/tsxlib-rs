//! # Apache Parquet IO
use parquet::file::reader::SerializedFileReader;
use std::error::Error;
use std::hash::Hash;
use std::cmp;
use serde::{Serialize};

use crate::timeseries::TimeSeries;
use crate::data_elements::TimeSeriesDataPoint;

/// Load series from the given Parquet file

pub fn read_from_file<TDate: Serialize + Hash + Copy + cmp::Eq + cmp::Ord, T: Copy>(
    file_path: &str,
    datapoint_gen_func: fn(&parquet::record::Row)->TimeSeriesDataPoint<TDate,T>
) -> Result<TimeSeries<TDate,T>, Box<dyn Error>> {

    let path = std::path::Path::new(file_path);
    let file = std::fs::File::open(&path).unwrap();
    let parquet_rdr = SerializedFileReader::new(file).unwrap();
    let mut data: Vec<TimeSeriesDataPoint<TDate,T>> = Vec::new();
    for row in parquet_rdr.into_iter() {
        let record: TimeSeriesDataPoint<TDate,T> = datapoint_gen_func(&row);
        data.push(record);
    }

    Ok(TimeSeries::from_tsdatapoints_unchecked(data))
}


/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use parquet::record::RowAccessor;
    use chrono::NaiveDateTime;

    use crate::data_elements::TimeSeriesDataPoint;
    use crate::timeutils;

    #[test]
    fn test_read() {
        

        fn datapoint_gen_func(row: &parquet::record::Row) -> TimeSeriesDataPoint<NaiveDateTime,f64> {
            let value = row.get_double(1).unwrap();
            let istamp = row.get_timestamp_millis(0).unwrap() as i64;
            let ts = timeutils::naive_datetime_from_millis(istamp);
            TimeSeriesDataPoint::new(ts,value)
        };

        let ts = read_from_file::<NaiveDateTime, f64>("testdata/rand_data.parquet",datapoint_gen_func).unwrap();

        // println!("{:.2?}",tsrres);
        // println!("{:.2?}",ts);

        assert_eq!(999997, ts.len());

    }

}