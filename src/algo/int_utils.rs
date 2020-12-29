use std::cmp;

fn merge_asof_prior_impl(this: &i32,other: &i32,other_prior: &i32, asoflookback :i32) -> (cmp::Ordering,i64) {
    let diff = this - other_prior;
    match  diff {
        d if d < 0 && this != other => (cmp::Ordering::Less,0),
        d if d > asoflookback && this != other => (cmp::Ordering::Greater,0),
        d if d <= asoflookback && this != other => (cmp::Ordering::Equal,-1),
        _ => (cmp::Ordering::Equal,0)
    }
}

fn merge_asof_fwd_impl(this: &i32,other: &i32,other_peak: &i32, asoflookfwd :i32) -> (cmp::Ordering,i64) {
    let diff1 = other_peak - this;
    let diff2 = other - this;
    let diff = cmp::min(diff1,cmp::max(diff2,0));
    let offset:i64 = if diff == diff2 {0}else{1};
    match  diff {
        d if d < 0 && this != other => (cmp::Ordering::Greater,0),
        d if d > asoflookfwd && this != other => (cmp::Ordering::Less,0),
        d if d <= asoflookfwd && this != other => (cmp::Ordering::Equal,offset),
        _ => (cmp::Ordering::Equal,0)
    }
}

fn merge_asof_frontend(free_param :i32, func: fn(&i32,&i32,&i32,i32)-> (cmp::Ordering,i64)) -> Box<dyn Fn(&i32,&i32,&i32)->(cmp::Ordering,i64)> {
    Box::new(move |this: &i32, other: &i32, other_peak: &i32| func(this, other, other_peak,free_param))
}

pub fn merge_asof_prior(look_back :i32) -> Box<dyn Fn(&i32,&i32,&i32)->(cmp::Ordering,i64)> {
    merge_asof_frontend(look_back,merge_asof_prior_impl)
}
pub fn merge_asof_fwd(look_fwd :i32) -> Box<dyn Fn(&i32,&i32,&i32)->(cmp::Ordering,i64)> {
    merge_asof_frontend(look_fwd,merge_asof_fwd_impl)
}