
#![allow(unused_macros)]
macro_rules! gen_timings { 
    ($test_case:expr, $closure:expr, $niter:expr) => {
        {
            let niter = $niter;
            let mut _durvec = Vec::with_capacity(niter);
            for _ in 0..niter{
                let before = std::time::Instant::now();
                $closure();
                let timing = before.elapsed();
                _durvec.push(timing)
            }
            let _total = _durvec.iter().fold(std::time::Duration::from_nanos(0), |x,y| x + *y );
            let _avg = std::time::Duration::from_secs_f64( _total.as_secs_f64()/(niter as f64));
            let _min = _durvec.iter().min().unwrap();
            let _max = _durvec.iter().max().unwrap();
            eprintln!("{}: total: {:.2?}, avg: {:.2?}, min: {:.2?}, max: {:.2?}", $test_case,_total,_avg,_min,_max);
        }
    }
}


#[cfg(feature = "parq")]
fn runnable_with_parq_enabled(){

    use serde::Serialize;
    use chrono::{NaiveDateTime,Duration};
    use parquet::record::RowAccessor;
    use tsxlib::timeseries::{TimeSeries};
    use tsxlib::data_elements::TimeSeriesDataPoint;
    use tsxlib::timeseries_iterators::{FromUncheckedIterator};
    use tsxlib::timeutils;


    fn datapoint_gen_func(row: &parquet::record::Row) -> TimeSeriesDataPoint<NaiveDateTime,f64> {
        let value = row.get_double(1).unwrap();
        let istamp = row.get_timestamp_millis(0).unwrap() as i64;
        let ndt = timeutils::naive_datetime_from_millis(istamp);
        TimeSeriesDataPoint::new(ndt,value)
    };

    
    gen_timings!("Read Parquet Test", Box::new(|| {tsxlib::io::parquet::read_from_file::<NaiveDateTime, f64>("../../../testdata/rand_data.parquet",datapoint_gen_func).unwrap();}),10);

    let ts: TimeSeries<NaiveDateTime,f64>  = tsxlib::io::parquet::read_from_file::<NaiveDateTime, f64>("../../../testdata/rand_data.parquet",datapoint_gen_func).unwrap();

    
    let tsrres: TimeSeries<NaiveDateTime,f64> = ts.into_iter().map(|x| TimeSeriesDataPoint::new(x.timestamp,x.value * 2.0)).collect_from_unchecked_iter();

    gen_timings!("Map float via value iterator", Box::new(|| {ts.into_iter().map(|x| TimeSeriesDataPoint::new(x.timestamp,x.value * 2.0)).collect_from_unchecked_iter();}),100);
    gen_timings!("Map float via ref iterator", Box::new(|| {ts.iter().map(|x| TimeSeriesDataPoint::new(x.timestamp, x.value * 2.0)).collect_from_unchecked_iter();}),100);
    gen_timings!("Map float via native method", Box::new(|| {ts.map(|x| x * 2.0);}),100);

    gen_timings!("Lag by 1", Box::new(|| {ts.shift(-1).collect_from_unchecked_iter();}),100);

    gen_timings!("Cross Apply (inner join)", Box::new(|| {tsrres.cross_apply_inner(&ts,|x,y| x + y);}),100);
    let lagged: TimeSeries<NaiveDateTime,f64> =  ts.shift(-1).collect_from_unchecked_iter();

    gen_timings!("Cross Apply (inner join) Different Lengths", Box::new(|| {tsrres.cross_apply_inner(&lagged,|x,y| x + y);}),100);

    gen_timings!("Cross Apply (left join) Different Lengths", Box::new(|| {
        tsrres.cross_apply_left(&lagged,|x,y| match y.is_some(){
            true => y.unwrap() + x,
            false => *x
        });
    }),100);

    gen_timings!("Bar Data round up", Box::new(|| {ts.resample_and_agg(Duration::minutes(15), |dt,dur| timeutils::round_up_to_nearest_duration(dt, dur), |x| *x.last().unwrap().value);}),100);
    gen_timings!("Bar Data round down", Box::new(|| {ts.resample_and_agg(Duration::minutes(15), |dt,dur| timeutils::round_down_to_nearest_duration(dt, dur), |x| *x.last().unwrap().value);}),100);
    
    #[derive(Clone,Copy,Serialize,Default)]
    struct SimpleStruct{
        pub timestamp: i64,
        pub floatvalueother: f64,
        pub floatvalue: f64
    };
    #[derive(Clone,Copy,Serialize,Default)]
    struct OtherStruct{
        pub timestamp: i64,
        pub ratio: f64
    };

    let newbase: TimeSeries<NaiveDateTime,SimpleStruct>  = ts.map_with_date(|dt,x| SimpleStruct{timestamp:dt.timestamp_millis(),floatvalue:x*2.0,floatvalueother:*x} );
    fn complicated(x: &SimpleStruct) -> OtherStruct{
        if x.timestamp & 1 == 0 {
            OtherStruct {timestamp:x.timestamp,ratio:x.floatvalue}
        }
        else{
            OtherStruct {timestamp:x.timestamp,ratio:x.floatvalue/x.floatvalueother}
        }

    }

    gen_timings!("Map struct via iterator", Box::new(|| {newbase.iter().map(|x| TimeSeriesDataPoint::new(x.timestamp,complicated(&x.value))).collect_from_unchecked_iter();}),100);
    gen_timings!("Map struct via native method", Box::new(|| {newbase.map(complicated);}),100);

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
