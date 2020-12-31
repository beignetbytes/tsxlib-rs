//! # Utilities for chrono DateTimes
use std::cmp;

use chrono::{Duration, NaiveDateTime};



fn merge_asof_prior_impl(this: &NaiveDateTime,other: &NaiveDateTime,other_prior: &NaiveDateTime, asoflookback :Duration) -> (cmp::Ordering,i64) {
    let diff = *this - *other_prior;
    match  diff {
        d if d < Duration::nanoseconds(0) && this != other => (cmp::Ordering::Less,0),
        d if d > asoflookback && this != other => (cmp::Ordering::Greater,0),
        d if d <= asoflookback && this != other => (cmp::Ordering::Equal,-1),
        _ => (cmp::Ordering::Equal,0)
    }
}

fn merge_asof_fwd_impl(this: &NaiveDateTime,other: &NaiveDateTime,other_peak: &NaiveDateTime, asoflookfwd :Duration) -> (cmp::Ordering,i64) {
    let diff1 = *other_peak - *this;
    let diff2 = *other - *this;
    let zerodur = Duration::nanoseconds(0);
    let diff = cmp::min(diff1,cmp::max(diff2,zerodur));
    let offset:i64 = if diff == diff2 {0}else{1};
    match  diff {
        d if d < zerodur && this != other => (cmp::Ordering::Greater,0),
        d if d > asoflookfwd && this != other => (cmp::Ordering::Less,0),
        d if d <= asoflookfwd && this != other => (cmp::Ordering::Equal,offset),
        _ => (cmp::Ordering::Equal,0)
    }
}

fn merge_asof_frontend(free_param :Duration, func: fn(&NaiveDateTime,&NaiveDateTime,&NaiveDateTime,Duration)-> (cmp::Ordering,i64)) -> Box<dyn Fn(&NaiveDateTime,&NaiveDateTime,&NaiveDateTime)->(cmp::Ordering,i64)> {
    Box::new(move |this: &NaiveDateTime, other: &NaiveDateTime, other_peak: &NaiveDateTime| func(this, other, other_peak,free_param))
}
/// Implementation fo mergeasof for a given duration lookback for a pair of Timeseries that has a HashableIndex<NaiveDateTime>
pub fn merge_asof_prior(look_back :Duration) -> Box<dyn Fn(&NaiveDateTime,&NaiveDateTime,&NaiveDateTime)->(cmp::Ordering,i64)> {
    merge_asof_frontend(look_back,merge_asof_prior_impl)
}
/// Implementation fo mergeasof for a given duration look-forward for a pair of Timeseries that has a HashableIndex<NaiveDateTime>
pub fn merge_asof_fwd(look_fwd :Duration) -> Box<dyn Fn(&NaiveDateTime,&NaiveDateTime,&NaiveDateTime)->(cmp::Ordering,i64)> {
    merge_asof_frontend(look_fwd,merge_asof_fwd_impl)
}