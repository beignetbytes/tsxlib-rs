# TSXLIB-RS General Use Timeseries Containers for Rust 
[![Build Status](https://travis-ci.com/beignetbytes/tsxlib-rs.svg?branch=main)](https://travis-ci.com/beignetbytes/tsxlib-rs)
[![codecov](https://codecov.io/gh/beignetbytes/tsxlib-rs/branch/main/graph/badge.svg?token=U2LYGT2LN8)](https://codecov.io/gh/beignetbytes/tsxlib-rs)
[![rust docs](https://docs.rs/tsxlib/badge.svg)](https://docs.rs/tsxlib/latest/tsxlib/)
[![](http://meritbadge.herokuapp.com/tsxlib)](https://crates.io/crates/tsxlib)

# Project Overview

TSXLIB-RS/TSXLIB is a beta stage open source project.

We are still iterating on and evolving the crate. Given that the containers and methods are pretty generic right now we will try to ensure backwards compatibility expected during evolution from version to version. 
However, as with any beta stage project breaking changes may still occur. 

# Goals and Non-Goals

The goal of this project is to provide a general container with robust compile time visibility that you can use to 1.) collect timeseries data and 2.) do efficient map operations on it, right now this comes at the cost of lookup performance.

We deliberately make (very little) assumptions about what the data you will put into the container will be. i.e. it is generic over both data and key. This is to allow you to put in whatever custom time struct you want along with whatever data that you want.

It is on the TODO list to add some basic specialization for primitives, i.e. have a diff() method vs having to put in a UDF on the skip operator for f64's every time to accomplish the same thing.

Conversely, this is not meant to be a generic dataframe-like library.


# Quick Start

Either fork from this repo or add to your projects `Cargo.toml` like so:

```
[dependencies]
tsxlib = { version = "^0.1.0", features = ["parq","json"] }
```

**Note on compatibility**

 If you compile with the parquet IO enabled, i.e. with --features "parq", you will need to be on *nightly* Rust.
 
 All other features work on stable Rust. 
 
 CI runs on stable (with json feature), beta (with json feature), and nightly (with json AND parquet features).

Tested on Rust >=1.48

Once the project stabilizes there will be effort put into maintaining compatibility with prior rust compiler versions

# Examples

Using this library you can:
<br>
Extract points from a timeseries
```
use tsxlib::timeseries::TimeSeries;
use chrono::{NaiveDateTime};

let index = vec![NaiveDateTime::from_timestamp(1,0), NaiveDateTime::from_timestamp(5,0), NaiveDateTime::from_timestamp(10,0)];
let data = vec![1.0, 2.0, 3.0];
let ts = TimeSeries::from_vecs(index, data).unwrap();

assert_eq!(ts.at(NaiveDateTime::from_timestamp(0,0)), None);
assert_eq!(ts.at(NaiveDateTime::from_timestamp(1,0)), Some(1.0));
```
you can also index at or first prior
```
assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(0,0)), None);
assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(1,0)), Some(1.0));
assert_eq!(ts.at_or_first_prior(NaiveDateTime::from_timestamp(4,0)), Some(1.0));
```
or positionally
```
assert_eq!(ts.at_idx_of(1), Some(TimeSeriesDataPoint::new(NaiveDateTime::from_timestamp(5,0), 2.0)));
```
The library also lets you map a function efficiently over a TimeSeries
```
let result = ts.map(|x| x * 2.0);
```
However, you can also use it as an iterator, N.B. collect will check for order and reorder if needed but methods named  "unchecked" will not.
```
let result: TimeSeries<NaiveDateTime,f64> = ts.into_iter().map(|x| TimeSeriesDataPoint::new(x.timestamp,x.value * 2.0)).collect_from_unchecked_iter();
```
This means you can use it with other crates that work as extensions on iterators, i.e. like `rayon`, where it makes sense to multithread a workload
```
let result: TimeSeries<NaiveDateTime,f64> = TimeSeries::from_tsdatapoints(ts.into_iter().par_bridge().map(|x| TimeSeriesDataPoint::new(x.timestamp,x.value * 2.0)).collect::<Vec<TimeSeriesDataPoint<NaiveDateTime, f64>>>()).unwrap();
```
And it also means that you can use native iterator methods to calculate things like a cumulative sum
```
//as a total
let total = ts.into_iter().fold(0.0,|acc,x| acc + x.value);
// as a timeseries
let mut acc = 0.0;
let result: TimeSeries<NaiveDateTime, f64> = ts.into_iter().map(|x| {acc = acc + x.value; TimeSeriesDataPoint::new(x.timestamp,acc) }).collect();
```
Joins/Cross apply operations are also implemented, 
We have Cross Apply Inner:
```
use tsxlib::timeseries::TimeSeries;
use tsxlib::data_elements::TimeSeriesDataPoint;

let values : Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
let values2 : Vec<f64> = vec![1.0, 2.0, 4.0];
let index: Vec<i32> = (0..values.len()).map(|i| i as i32).collect();
let index2: Vec<i32> = (0..values2.len()).map(|i| i as i32).collect();
let ts = TimeSeries::from_vecs(index, values).unwrap();
let ts1 = TimeSeries::from_vecs(index2, values2).unwrap();
let tsres = ts.cross_apply_inner(&ts1,|a,b| (*a,*b));

let expected = vec![
    TimeSeriesDataPoint { timestamp: 0, value: (1.00, 1.00) },
    TimeSeriesDataPoint { timestamp: 1, value: (2.00, 2.00) },
    TimeSeriesDataPoint { timestamp: 2, value: (3.00, 4.00) },
];
let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();

assert_eq!(ts_expected, tsres)
```
We have Cross Apply Left:
```
use tsxlib::timeseries::TimeSeries;
use tsxlib::data_elements::TimeSeriesDataPoint;

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
```
Given that this is a Timeseries focused library, we also have As-Of Apply:
```
let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];    
let ts = TimeSeries::from_vecs(index.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values).unwrap();
let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
let index2 = vec![2, 4, 5, 7, 8, 10];    
let ts_join = TimeSeries::from_vecs(index2.iter().map(|x| NaiveDateTime::from_timestamp(*x,0)).collect(), values2).unwrap();

let result = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_prior(Duration::seconds(1))),|a,b| (*a, match b {
    Some(x) => Some(*x),
    None => None
}), MergeAsofMode::RollPrior);

let expected = vec![
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

let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();

assert_eq!(result, ts_expected);
```
Lastly, you can also easily join multiple series together
```
let ts = TimeSeries::from_vecs(index.clone(), values).unwrap();
let ts1 = TimeSeries::from_vecs(index.clone(), values2).unwrap();
let ts2 = TimeSeries::from_vecs(index.clone(), values3).unwrap();
let ts3 = TimeSeries::from_vecs(index, values4).unwrap();
let tsres = n_inner_join!(ts,&ts1,&ts2,&ts3);

```
Various Timeseries functionalities are generally implemented as Iterators. e.g.
shift...
```
let tslag: TimeSeries<NaiveDateTime,f64> = ts.shift(-1).collect();
```
The language of the APIs is meant to be general so the above means "lag", and
```
let tsfwd: TimeSeries<NaiveDateTime,f64> = ts.shift(1).collect();
```
means "roll forward"

TSXLIB-RS also has a "skip" operator. i.e. if you wanted to implement difference you could write
```
fn change_func(prior: &f64, curr: &f64) -> f64{
    curr - prior
};
let ts_diff: TimeSeries<NaiveDateTime,f64> = ts.skip_apply(1, change_func).collect();
```
Conversely if you wanted to implement percent change you could write
```
fn change_func(prior: &f64, curr: &f64) -> f64{
    (curr - prior)/prior
};
let ts_perc_ch: TimeSeries<NaiveDateTime,f64> = ts.skip_apply(1, change_func).collect();
```
TSXLIB-RS has rolling window operations as well.
They can be implemented using a buffer
```
let values = vec![1.0, 1.0, 1.0, 1.0, 1.0];
let index = (0..values.len()).map(|i| NaiveDateTime::from_timestamp(60 * i as i64,0)).collect();
let ts = TimeSeries::from_vecs(index, values).unwrap();

fn roll_func(buffer: &Vec<f64>) -> f64{
    buffer.iter().sum()
};

let tsrolled: TimeSeries<NaiveDateTime,f64> = ts.apply_rolling(2, roll_func).collect();
```

or via update functions N.B. this will be more efficient in the sense that you wont have to keep the buffer in memory
```
fn update(prior: Option<f64>, next: &f64) -> Option<f64>{
    let v =  match prior.is_some(){
        true => prior.unwrap(),
        false => 0.0
    };
    Some(v + next)
};

fn decrement(next: Option<f64>, prior: &f64) -> Option<f64>{
    let v =  match next.is_some(){
        true => next.unwrap(),
        false => 0.0
    };
    Some(v - prior)
};

let tsrolled: TimeSeries<NaiveDateTime,f64> = ts.apply_updating_rolling(2, update, decrement).collect();
```

TSXLIB-RS supports aggregation on the index as well
```
let result = ts.resample_and_agg(Duration::minutes(15), |dt,dur| timeutils::round_up_to_nearest_duration(dt, dur), |x| *x.last().unwrap().value);
```

For more comprehensive/runnable examples check out the tests and the examples!

# Benchmark Performance

The benchmark that we run here consist of generating a series of 999,997 doubles with a `chrono` NaiveDateTime of millisecond precision as the key. This data is then lagged then joined. Following this it is rounded up/down into bars. in both benchmarks the *last* value is taken (but you can use whatever UDF you want to generate this aggregation). Lastly, we transform the data into a simple struct with the following fields
```
#[derive(Clone,Copy,Serialize,Default)]
struct SimpleStruct{
    pub timestamp: i64,
    pub floatvalueother: f64,
    pub floatvalue: f64
};
```
In the last two benchmarks we transform the struct above to 
```
    #[derive(Clone,Copy,Serialize,Default)]
    struct OtherStruct{
        pub timestamp: i64,
        pub ratio: f64
    };
```

via the following UDF
```
fn complicated(x: &SimpleStruct) -> OtherStruct{
    if x.timestamp & 1 == 0 {
        OtherStruct {timestamp:x.timestamp,ratio:x.floatvalue}
    }
    else{
        OtherStruct {timestamp:x.timestamp,ratio:x.floatvalue/x.floatvalueother}
    }
}

```

This is to simulate a decently realistic use case for the library. It is by no means the asymptotic limit of what it can accomplish.

<br>

**System Specs**
|                 |        |  
|-----------------|------- |
| ***Processor*** | Intel Core i7-9750H @ 2.60GHz     |
| ***RAM***       | 16 gb DDR4   |
<br>

**Results**

Compiled in accordance to the release settings in Cargo.toml.

To run on your machine compile: 

`cargo build --examples --release  --features "parq"`

And then run *benchmark.exe*

| Benchmark                                         | # Reps | Total (s)  | Mean (ms) | Min (ms) | Max (ms) |  
|---------------------------------------------------|------- |------------|-----------|----------|----------|
| Map float via value iterator                      | 100    | 1.72       | 17.20     | 15.88   | 26.41     |
| Map float via ref iterator                        | 100    | 1.20       | 11.96     | 11.21   | 14.54     |
| Map float via native method                       | 100    | 0.53       | 5.30      | 4.91    | 6.60      |
| Lag by 1                                          | 100    | 1.88       | 18.78     | 17.4    | 26.16     |
| Cross Apply (inner join)                          | 100    | 7.88       | 78.78     | 75.9    | 94.27     |
| Cross Apply (inner join) Different Lengths        | 100    | 8.02       | 80.24     | 75.7    | 91.19     |
| Cross Apply (left join) Different Lengths         | 100    | 8.32       | 83.18     | 78.7    | 99.33     |
| Bar Data round up                                 | 100    | 8.09       | 80.89     | 77.7    | 88.17     |
| Bar Data round down                               | 100    | 7.74       | 77.42     | 74.0    | 85.27     |
| Map struct via iterator                           | 100    | 2.33       | 23.29     | 20.5    | 31.42     |
| Map struct via native method: total               | 100    | 1.19       | 11.88     | 11.1    | 14.07     |

<br>

# Features/TODO List


| Feature                                           | Support | Category            | Compiler Option| Rust Version |  
|---------------------------------------------------|---------|---------------------|----------------|--------------|
| Time Filters                                      | ✔      | Core                 |                | >=1.48       |
| Positional Indexing                               | ✔      | Core                 |                | >=1.48       |
| Key Indexing                                      | ✔      | Core                 |                | >=1.48       |
| Shifts                                            | ✔      | Core                 |                | >=1.48       |
| Inner Join (Merge & Hash Join)                    | ✔      | Core                 |                | >=1.48       |
| Left Join (Merge & Hash Join)                     | ✔      | Core                 |                | >=1.48       |
| "As-Of" Join (Merge)                               | ✔      | Core                 |                | >=1.48       |
| Multiple Inner Join                               | ✔      | Core                 |                | >=1.48       |
| Concat/Interweave                                 | ✔      | Core                 |                | >=1.48       |
| Time Aggregation                                  | ✔      | Core                 |                | >=1.48       |
| Time Aggregation Helpers with chrono index        | ✔      | Specializations      |                | >=1.48       |
| Time Aggregation Helpers with int index           | ✔      | Specializations      |                | >=1.48       |
| Closure application (User Defined Functions)      | ✔      | Core                 |                | >=1.48       |
| SIMD Support                                      |        | Core                 |                |  >=1.48      |
| Native Null Filling/Interpolations                |        | Core                 |                |  >=1.48      |
| Buffer Based Moving Window Operations             | ✔      | Core                 |                | >=1.48       |
| Update Based Moving Window Operations             | ✔      | Core                 |                | >=1.48       |
| "Skip" Operations (i.e. diff...etc.)               | ✔      | Core                 |                | >=1.48       |
| Rust iterators                                    | ✔      | Core                 |                | >=1.48       |
| Ordered Rust iterators                            | ✔      | Core                 |                | >=1.48       |
| Streaming iterators                               | ✔      | Core                 |                | >=1.48       |
| CSV IO*                                           | ✔      | IO                   |                | >=1.48       |
| JSON IO*                                          | ✔      | IO                   | "json"         | >=1.48       |
| Parquet IO*                                       | ✔      | IO                   | "parq"         | Nightly     |
| Avro IO                                           |       | IO                   |                | >=1.48       |
| Flatbuffer IO                                     |       | IO                   |                | >=1.48       |
| Apache Kafka IO                                   |       | IO                   |                | >=1.48       |
| Protocol buffer IO                                |       | IO                   |                | >=1.48       |
| Intuitive APIs for primitive value types          |       | Specializations      |                | >=1.48       |
| Native Multithreading                             |       | Core                 |                | >=1.48       |
| Comprehensive Documentation                         |       | Meta                 |                | >=1.48       |
| Test Coverage                                     |       | Meta                 |                | >=1.48       |
| More Examples                                     |       | Meta                 |                | >=1.48       |
<br>

Features marked "*" need additional performance tuning and perhaps a refactoring into a more generic framework. Note that although compatibility is only listed as Rust >=1.48, TSXLIB-RS might work with lower Rust versions as well it just has not been tested.


# License

Licensed under either of

 * [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
 * [MIT license](http://opensource.org/licenses/MIT)

**Contributions**

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.