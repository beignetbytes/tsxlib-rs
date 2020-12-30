#[cfg(feature = "parq")]
fn runnable_with_parq_enabled(){
    use chrono::{NaiveDateTime};
    use parquet::record::RowAccessor;
    use std::time::Instant;
    use tsxlib::timeseries::{TimeSeries};
    use tsxlib::data_elements::TimeSeriesDataPoint;
    use tsxlib::timeseries_iterators::{FromUncheckedIterator};
    use tsxlib::timeutils;
    use tsxlib::io::streaming::{TimeSeriesDataPointReceiver};
    use std::sync::mpsc;
    use std::thread;
    
    fn datapoint_gen_func(row: &parquet::record::Row) -> TimeSeriesDataPoint<NaiveDateTime,f64> {
        let value = row.get_double(1).unwrap();
        let istamp = row.get_timestamp_millis(0).unwrap() as i64;
        let ts = timeutils::naive_datetime_from_millis(istamp);
        TimeSeriesDataPoint::new(ts,value)
    };

    let ts = tsxlib::io::parquet::read_from_file::<NaiveDateTime, f64>("../../../testdata/rand_data.parquet",datapoint_gen_func).unwrap();
    let (sender, mut receiver): (mpsc::Sender<TimeSeriesDataPoint<NaiveDateTime,f64>>,mpsc::Receiver<TimeSeriesDataPoint<NaiveDateTime,f64>>) = mpsc::channel();
    thread::spawn(move || {
        ts.into_ordered_iter().for_each( |dp| {
        sender.send(dp).unwrap();
    });});
    
    let consumer = TimeSeriesDataPointReceiver::new(&mut receiver);
    let before = Instant::now();
    let res: TimeSeries<NaiveDateTime,f64> = consumer.collect_from_unchecked_iter();
    println!("Took {:.2?} to receive a stream of {:.2?}", before.elapsed(),res.len());

}

#[cfg(not(feature = "parq"))]
fn no_parq(){ 
    println!("you need to build with --features \"parq\" to enable this")
}

fn main() {
    #[cfg(feature = "parq")]
    runnable_with_parq_enabled();
    #[cfg(not(feature = "parq"))]
    no_parq();
}
