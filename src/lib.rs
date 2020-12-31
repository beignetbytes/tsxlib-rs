//! # TSXLIB: a General Use Timeseries Container for Rust 
//!
//! The goal of this project is to provide a general container with robust compile time visibility that you can use to 1.) collect timeseries data and 2.) do efficient map operations on it, right now this comes at the cost of lookup performance.
//!
//! We deliberately make (very little) assumptions about what the data you will put into the container will be. i.e. it is generic over both data and key. This is to allow you to put in whatever custom time struct you want along with whatever data that you want.
//!
//! The crate is roughly organized as follows:
//! <br>
//! ***Core Modules***
//! - `tsxlib::timeseries` => This is the core of the module. It has the timeseries struct as well as the implementations of the various methods that you can call on it.
//! - `tsxlib::data_elements` =>  This contains the TimeSeriesDataPoint stuct, as the name would suggest it represents a point on a time series. You can use this to shuttle data around point by point as well as in any custom iterator implentations.
//! - `tsxlib::index` => This module contains the struct that serves as the index for the timeseries container and associated methods.
//! - `tsxlib::timeseries_iterators` => definitions/implementations for various timeseries iterators...i.e. skip/rolling...etc.
//! <br>
//! ***IO Modules***
//! - `tsxlib::io::*` => This module contains free funcs that can implement various IO methods. See the Readme for the implementation status matrix
//! <br>
//! ***Utility Modules***
//! - `tsxlib::timeutils` => this contains utility functions that you can use on chrono datetimes to facilitate the bar-ing of data.
//! - `tsxlib::algo::chrono_utils` => this contains utility functions that you can use on chrono datetimes for the AsOf merge method on the TimeSeries struct.
//! - `tsxlib::algo::int_utils` => this contains utility functions that you can use on ints for the AsOf merge method on the TimeSeries struct.
//! - `tsxlib::algo::macros` => this contains utility macros.
//! <br>
//! ***Internals***
//! - `tsxlib::joins` => This module contains the implementation of the `JoinEngine` struct that implements the join algos that are used by TSXLIB. Both Hash Join and Merge Join are implemented but Merge Join is the one that is used due to its efficiency. In later versions of the crate we might expose hash join as an option
//! <br>
//! **Note on compatibility**
//! 
//!  If you compile with the parquet IO enabled, i.e. with --features "parq", you will need to be on *nightly* Rust.
//!  
//!  All other features work on stable Rust. 
//!  
//!  CI runs on stable (with json feature), beta (with json feature), and nightly (with json AND parquet features).
//! 
//! Tested on Rust >=1.48
//! 
//! Once the project stabilizes there will be effort put into maintaining compatibility with prior rust compiler versions


pub mod joins;
pub mod index;
pub mod io;
pub mod algo;
pub mod data_elements;
pub mod timeseries_iterators;
pub mod timeutils;
pub mod timeseries;
