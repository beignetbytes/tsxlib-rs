//! # Utilities for chrono DateTimes
use chrono::{Duration,NaiveDateTime, DateTime, TimeZone, Utc};

///Generate a chrono NaiveDateTime from a i64 value of milliseconds
pub fn naive_datetime_from_millis(istamp:i64)->NaiveDateTime{
    let secs: i64 = istamp/1000;
    let nsecs: u32 = ((istamp % 1000) * 1000000) as u32;
    NaiveDateTime::from_timestamp(secs, nsecs)
}

/// This trait defines the contract for rounding a T via the methods in timeutils
pub trait DurationRoudable<T>{
    fn get_utc_millis_since_epoch(&self) -> i64;
    fn repr_from_utc_millis(&self, utc_milli_stamp: i64) -> T; //need the self here to push the timezone down 
}

impl DurationRoudable<NaiveDateTime> for NaiveDateTime{
    fn get_utc_millis_since_epoch(&self) -> i64{
        self.timestamp_millis()
    }
    fn repr_from_utc_millis(&self, utc_milli_stamp: i64) -> NaiveDateTime {
        naive_datetime_from_millis(utc_milli_stamp)
    }
}

impl<TZInfo: TimeZone> DurationRoudable<DateTime<TZInfo>> for DateTime<TZInfo>{
    fn get_utc_millis_since_epoch(&self) -> i64{
        let ndt = self.naive_utc();
        ndt.timestamp_millis()
    }
    fn repr_from_utc_millis(&self, utc_milli_stamp: i64) -> DateTime<TZInfo> {
        let ndt = naive_datetime_from_millis(utc_milli_stamp);
        let utcdt = DateTime::<Utc>::from_utc(ndt,Utc);
        utcdt.with_timezone(&self.timezone())
    }
}

//SRC:: https://stackoverflow.com/questions/31210357/is-there-a-modulus-not-remainder-function-operation
trait ModuloSignedExt {
    fn modulo(&self, n: Self) -> Self;
}
macro_rules! modulo_signed_ext_impl {
    ($($t:ty)*) => ($(
        impl ModuloSignedExt for $t {
            #[inline]
            fn modulo(&self, n: Self) -> Self {
                (self % n + n) % n
            }
        }
    )*)
}
modulo_signed_ext_impl! { i8 i16 i32 i64 i128 }


//Below are Derived from C# implementations here: https://stackoverflow.com/questions/7029353/how-can-i-round-up-the-time-to-the-nearest-x-minutes/7029464

/// Max precision is milliseconds
/// 
/// Rounds a TDate up to the nearest duration, i.e. if you had a chrono::NaiveDateTime{2020-12-31T12:35:00Z} and you rounding to Duration{15mins} it would return 12:45
pub fn round_up_to_nearest_duration<TDate>(timestamp: &TDate, sample_size :&Duration) -> TDate
where TDate : DurationRoudable<TDate>
{
    let mod_ticks = timestamp.get_utc_millis_since_epoch().modulo(sample_size.num_milliseconds());
    // Orginal impl
    // let delta = match mod_ticks != 0 {
    //     true => sample_size.num_milliseconds()  - mod_ticks,
    //     false => 0
    // };
    let delta = std::cmp::max(sample_size.num_milliseconds()  - mod_ticks,0);
    let rs =  timestamp.get_utc_millis_since_epoch() + delta;
    timestamp.repr_from_utc_millis(rs)
}

/// Max precision is milliseconds
/// 
/// Rounds a TDate down to the nearest duration, i.e. if you had a chrono::NaiveDateTime{2020-12-31T12:35:00Z} and you rounding to Duration{15mins} it would return 12:30
pub fn round_down_to_nearest_duration<TDate>(timestamp: &TDate, sample_size :&Duration) -> TDate
where TDate : DurationRoudable<TDate>
{
    let mod_ticks = timestamp.get_utc_millis_since_epoch().modulo(sample_size.num_milliseconds());
    let rs =  timestamp.get_utc_millis_since_epoch() - mod_ticks;
    timestamp.repr_from_utc_millis(rs)
}

/// Max precision is milliseconds
/// 
/// Rounds a TDate to the nearest duration, i.e. if you had a chrono::NaiveDateTime{2020-12-31T12:35:00Z} and you rounding to Duration{15mins} it would return 12:30
pub fn round_nearest_to_nearest_duration<TDate>(timestamp: &TDate, sample_size :&Duration) -> TDate
where TDate : DurationRoudable<TDate>
{
    let mod_ticks = timestamp.get_utc_millis_since_epoch().modulo(sample_size.num_milliseconds());
    let offset = match mod_ticks > (sample_size.num_milliseconds()/2) {
        true => sample_size.num_milliseconds(),
        false => 0
    };
    let rs =  timestamp.get_utc_millis_since_epoch() + offset - mod_ticks;
    timestamp.repr_from_utc_millis(rs)
}


/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_rounding_up() {

        let date1 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 34, 56, 789);
        let dur = Duration::minutes(1);
        let rounded = round_up_to_nearest_duration(&date1, &dur);
        let exp1 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 35, 0, 0);
        assert_eq!(rounded,exp1);

        let dur2 = Duration::minutes(15);
        let rounded2 = round_up_to_nearest_duration(&date1, &dur2);
        let exp2 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 45, 0, 0);
        assert_eq!(rounded2,exp2);

    }

    #[test]
    fn test_rounding_down() {

        let date1 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 34, 56, 789);
        let dur = Duration::minutes(1);
        let rounded = round_down_to_nearest_duration(&date1, &dur);
        let exp1 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 34, 0, 0);
        assert_eq!(rounded,exp1);

        let dur2 = Duration::minutes(15);
        let rounded2 = round_down_to_nearest_duration(&date1, &dur2);
        let exp2 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 30, 0, 0);
        assert_eq!(rounded2,exp2);

    }

    #[test]
    fn test_rounding_nearest() {

        let date1 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 34, 30, 789);
        let dur = Duration::minutes(1);
        let rounded = round_nearest_to_nearest_duration(&date1, &dur);
        let exp1 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 35, 0, 0);
        assert_eq!(rounded,exp1);

        let dur2 = Duration::minutes(15);
        let rounded2 = round_nearest_to_nearest_duration(&date1, &dur2);
        let exp2 = NaiveDate::from_ymd(2010,12,10).and_hms_milli(12, 30, 0, 0);
        assert_eq!(rounded2,exp2);

    }

}