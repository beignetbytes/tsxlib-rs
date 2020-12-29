#![allow(unused_macros)]
macro_rules! n_inner_join {
    // @closure creates a tuple-flattening closure for .map() call. usage:
    // @closure partial_pattern => partial_tuple , rest , of , iterators
    // eg. izip!( @closure ((a, b), c) => (a, b, c) , dd , ee )
    ( @closure $p:pat => $tup:expr ) => {
        |$p| $tup
    };

    // The "b" identifier is a different identifier on each recursion level thanks to hygiene.
    ( @closure $p:pat => ( $($tup:tt)* ) , $_iter:expr $( , $tail:expr )* ) => {
        n_inner_join!(@closure ($p, b) => ( $($tup)*, *b ) $( , $tail )*)
    };

    ( $first:expr $( , $rest:expr )* $(,)* ) => {
        $first
            $(
                .cross_apply_inner($rest,|x,y| (*x,*y))
            )*
            .map(
                n_inner_join!(@closure a => (*a) $( , $rest )*)
            )
    };
}


/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*; 
    use crate::timeseries::TimeSeries;
    use crate::data_elements::TimeSeriesDataPoint;
    
    #[test]
    fn test_tuple_join() {
        let values : Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();

        let values2: Vec<f64> =  values.iter().map(|x| x * 2.0).collect();
        let values3: Vec<f64> = values.iter().map(|x| x * 3.0).collect();
        let values4: Vec<f64> =  values.iter().map(|x| x * 4.0).collect();

        let ts = TimeSeries::from_vecs(index.clone(), values).unwrap();
        let ts1 = TimeSeries::from_vecs(index.clone(), values2).unwrap();
        let ts2 = TimeSeries::from_vecs(index.clone(), values3).unwrap();
        let ts3 = TimeSeries::from_vecs(index, values4).unwrap();
        let tsres = n_inner_join!(ts,&ts1,&ts2,&ts3);
        tsres.into_iter().for_each(|x|println!("{:.2?}",x));

        let expected = vec![
            TimeSeriesDataPoint { timestamp: 0, value: (1.00, 2.00, 3.00, 4.00) },
            TimeSeriesDataPoint { timestamp: 1, value: (2.00, 4.00, 6.00, 8.00) },
            TimeSeriesDataPoint { timestamp: 2, value: (3.00, 6.00, 9.00, 12.00) },
            TimeSeriesDataPoint { timestamp: 3, value: (4.00, 8.00, 12.00, 16.00) },
            TimeSeriesDataPoint { timestamp: 4, value: (5.00, 10.00, 15.00, 20.00) },
        ];

        let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();

        assert_eq!(ts_expected, tsres)
    }

}