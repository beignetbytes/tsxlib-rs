//! # IO 
//!
//! This module contains the various IO methods that you can use to IO TimeSeries.
//!
pub mod csv;
pub mod streaming;
//varies feature libaries after this
#[cfg(feature = "parq")]
pub mod parquet;
#[cfg(feature = "json")]
pub mod json;