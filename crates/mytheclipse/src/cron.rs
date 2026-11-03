//! Minimal cron scheduling (feature `lifecycle`).
//!
//! [`CronSchedule`] parses the standard five-field cron expression
//! (`minute hour day-of-month month day-of-week`) and computes next fire
//! times. [`schedule`] drives a [`CronSchedule`] on a Tokio runtime, running a
//! job each time it fires. The implementation is self-contained — no external
//! cron crate — and deliberately ignores time zones (dates are interpreted in
//! UTC).

use std::time::Duration;

use tracing::Instrument;

/// Error parsing a cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronParseError(pub String);

impl std::fmt::Display for CronParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid cron expression: {}", self.0)
    }
}

impl std::error::Error for CronParseError {}

/// Error scheduling a cron job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronError {
    /// The expression did not parse.
    Parse(CronParseError),
}

impl std::fmt::Display for CronError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "cron error: {err}"),
        }
    }
}

impl std::error::Error for CronError {}

/// A simple wall-clock instant used by cron computations.
///
/// Fields follow the chronological order used in cron expressions, resolved to
/// the local (UTC) civil calendar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CronTime {
    /// Year, e.g. `2026`.
    pub year: i32,
    /// Month, 1-12.
    pub month: u32,
    /// Day of month, 1-31.
    pub day: u32,
    /// Hour, 0-23.
    pub hour: u32,
    /// Minute, 0-59.
    pub minute: u32,
    /// Second, 0-59 (always 0 for scheduled fires).
    pub second: u32,
}

/// A parsed cron schedule.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minutes: Vec<u8>,
    hours: Vec<u8>,
    days_of_month: Vec<u8>,
    months: Vec<u8>,
    days_of_week: Vec<u8>,
}

/// A handle to a running cron job; dropping it aborts the task.
#[derive(Debug)]
pub struct CronJob {
    handle: tokio::task::JoinHandle<()>,
}

impl CronJob {
    /// Cancels the scheduled job.
    pub fn cancel(self) {
        self.handle.abort();
    }
}

impl CronSchedule {
    /// Parses the standard five-field cron expression
    /// `minute hour day-of-month month day-of-week`.
    ///
    /// Supported tokens per field: `*`, `*/step`, a literal, a list
    /// (`a,b,c`), an inclusive range (`a-b`), and a ranged step (`a-b/step`).
    /// Day-of-week also accepts `SUN`..`SAT` (Sun=0) names. Day-of-month and
    /// day-of-week are combined with an OR when both are restricted, matching
    /// standard cron semantics.
    pub fn parse(expr: &str) -> Result<Self, CronParseError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(CronParseError(format!(
                "expected exactly 5 fields, got {}",
                fields.len()
            )));
        }
        let minutes = parse_field(fields[0], 0, 59, false)?;
        let hours = parse_field(fields[1], 0, 23, false)?;
        let days_of_month = parse_field(fields[2], 1, 31, false)?;
        let months = parse_field(fields[3], 1, 12, false)?;
        let days_of_week = parse_field(fields[4], 0, 6, true)?;

        Ok(Self {
            minutes,
            hours,
            days_of_month,
            months,
            days_of_week,
        })
    }

    /// Returns the next fire time strictly after `from`, or `None` if the
    /// expression cannot fire within the scan horizon (e.g. Feb 30).
    ///
    /// Jumps to the next month when the month is excluded, to the next day when
    /// only the day is, and to the next hour when only the hour is, so cost
    /// scales with the calendar distance to the match rather than with minutes.
    pub fn next_after(&self, from: CronTime) -> Option<CronTime> {
        let horizon = 5 * 366 * 24 * 60; // minutes in ~5 years
        let mut spent = 0u32;
        // Scan whole minutes (second = 0), starting strictly after `from`.
        let mut act = from.advance_minute();
        while spent < horizon {
            if !self.date_matches(act) {
                act = if self.months.contains(&(act.month as u8)) {
                    act.next_day_start()
                } else {
                    act.next_month_start()
                };
                spent += 24 * 60;
            } else if !self.hours.contains(&(act.hour as u8)) {
                act = act.next_hour_start();
                spent += 60;
            } else if self.minutes.contains(&(act.minute as u8)) {
                return Some(act);
            } else {
                act = act.advance_minute();
                spent += 1;
            }
        }
        None
    }

    /// Whether the month/day part of `t` can match at all.
    fn date_matches(&self, t: CronTime) -> bool {
        let month_ok = self.months.contains(&(t.month as u8));
        let dom_ok = self.days_of_month.contains(&(t.day as u8));
        let dow_ok = self.days_of_week.contains(&(weekday(t) as u8));
        // Standard cron: if both dom and dow are restricted, a match on EITHER
        // is sufficient; if only one is restricted, it must match; if neither,
        // always true.
        let dom_restricted = self.days_of_month.len() < 31;
        let dow_restricted = self.days_of_week.len() < 7;
        let day_ok = match (dom_restricted, dow_restricted) {
            (true, true) => dom_ok || dow_ok,
            (true, false) => dom_ok,
            (false, true) => dow_ok,
            (false, false) => true,
        };
        month_ok && day_ok
    }

    /// The next several fire times, for display/tests.
    pub fn next_five(&self, from: CronTime) -> Vec<CronTime> {
        let mut out = Vec::new();
        let mut cur = from;
        for _ in 0..5 {
            match self.next_after(cur) {
                Some(next) => {
                    out.push(next);
                    cur = next;
                }
                None => break,
            }
        }
        out
    }
}

/// Spawns a task that runs `job` on `expr`'s schedule until cancelled.
///
/// The job is a closure returning a future; each invocation runs in a
/// `mytheclipse_cron_task` tracing span. A fire time that was missed while the
/// job was running is not backlogged — the next scheduled fire is used.
pub fn schedule<F, Fut>(expr: &str, mut job: F) -> Result<CronJob, CronError>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let schedule = CronSchedule::parse(expr).map_err(CronError::Parse)?;
    let expr_owned = expr.to_string();
    let handle = tokio::spawn(async move {
        loop {
            let now = CronTime::now();
            let next = schedule.next_after(now);
            let next = match next {
                Some(next) => next,
                None => {
                    // Unreachable schedule; wait and re-check periodically.
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
            };
            let now_ts = CronTime::now().to_timestamp();
            let until = tokio::time::Instant::now()
                + Duration::from_secs(next.to_timestamp().saturating_sub(now_ts).max(1) as u64);
            tokio::time::sleep_until(until).await;
            let span = tracing::info_span!(
                "mytheclipse_cron_task",
                expr = %expr_owned
            );
            job().instrument(span).await;
        }
    });
    Ok(CronJob { handle })
}

impl CronTime {
    /// The current wall-clock time (UTC), truncated to minutes for firing.
    pub fn now() -> Self {
        // std has no direct civil-date conversion; derive from SystemTime.
        now_utc()
    }

    fn to_timestamp(self) -> i64 {
        days_from_civil(self.year, self.month, self.day) * 86_400
            + (self.hour as i64) * 3_600
            + (self.minute as i64) * 60
            + self.second as i64
    }

    fn advance_minute(self) -> Self {
        let mut t = self.add_seconds(60);
        t.second = 0;
        t
    }

    /// The first minute of the following day.
    fn next_day_start(self) -> Self {
        let day_start = (self.to_timestamp().div_euclid(86_400) + 1) * 86_400;
        from_timestamp(day_start)
    }

    /// The first minute of the following hour.
    fn next_hour_start(self) -> Self {
        let hour_start = (self.to_timestamp().div_euclid(3_600) + 1) * 3_600;
        from_timestamp(hour_start)
    }

    /// The first minute of the following month.
    fn next_month_start(self) -> Self {
        let (year, month) = if self.month == 12 {
            (self.year + 1, 1)
        } else {
            (self.year, self.month + 1)
        };
        CronTime {
            year,
            month,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        }
    }

    fn add_seconds(self, secs: i64) -> Self {
        let ts = self.to_timestamp() + secs;
        from_timestamp(ts)
    }
}

/// Weekday: 0 = Sunday .. 6 = Saturday (matches cron).
fn weekday(t: CronTime) -> u32 {
    // 1970-01-01 was a Thursday (weekday 4).
    let days = days_from_civil(t.year, t.month, t.day);
    // ((days + 4) mod 7) => 0=Sunday
    ((days % 7 + 11) % 7) as u32
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = y as i64 - (m as i64 <= 2) as i64;
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // March=0
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Converts a Unix timestamp (seconds) to a civil [`CronTime`].
fn from_timestamp(ts: i64) -> CronTime {
    let days = ts.div_euclid(86_400);
    let z = days + 719_468; // days since civil epoch
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y } as i32;

    let (h, mi, s) = split_seconds(ts);
    CronTime {
        year: y,
        month: m,
        day: d as u32,
        hour: h,
        minute: mi,
        second: s,
    }
}

fn split_seconds(ts: i64) -> (u32, u32, u32) {
    let rem = ts.rem_euclid(86_400);
    (
        (rem / 3_600) as u32,
        ((rem % 3_600) / 60) as u32,
        (rem % 60) as u32,
    )
}

/// Returns the current UTC civil time plus second accuracy from SystemTime.
fn now_utc() -> CronTime {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    from_timestamp(now.as_secs() as i64)
}

/// Parses a single cron field into a sorted list of allowed values.
fn parse_field(
    field: &str,
    min: u8,
    max: u8,
    allow_names: bool,
) -> Result<Vec<u8>, CronParseError> {
    let mut out = Vec::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(CronParseError(format!("empty field element in `{field}`")));
        }
        // Parse `base` optionally followed by `/step`.
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, parse_num(s, "step")?),
            None => (part, 1),
        };
        // Parse `*`, `a-b`, or literal.
        match range {
            "*" => {
                let mut v = min;
                while v <= max {
                    out.push(v);
                    v += step;
                }
            }
            _ if range.contains('-') => {
                let (a, b) = range
                    .split_once('-')
                    .ok_or_else(|| CronParseError(format!("bad range `{range}`")))?;
                let a = parse_value(a, min, max, allow_names)?;
                let b = parse_value(b, min, max, allow_names)?;
                if a > b {
                    return Err(CronParseError(format!("range start {a} > end {b}")));
                }
                let mut v = a;
                while v <= b {
                    out.push(v);
                    v += step;
                }
            }
            _ => {
                let v = parse_value(range, min, max, allow_names)?;
                out.push(v);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn parse_num(s: &str, what: &str) -> Result<u8, CronParseError> {
    s.parse::<u8>()
        .map_err(|_| CronParseError(format!("invalid {what} `{s}`")))
}

fn parse_value(s: &str, min: u8, max: u8, allow_names: bool) -> Result<u8, CronParseError> {
    let upper = s.to_uppercase();
    // Day-of-week names.
    if allow_names {
        let name = match upper.as_str() {
            "SUN" => Some(0),
            "MON" => Some(1),
            "TUE" => Some(2),
            "WED" => Some(3),
            "THU" => Some(4),
            "FRI" => Some(5),
            "SAT" => Some(6),
            _ => None,
        };
        if let Some(v) = name {
            return Ok(v);
        }
    }
    let v = parse_num(s, "value")?;
    if v < min || v > max {
        return Err(CronParseError(format!(
            "value {v} out of range {min}..{max}"
        )));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(y: i32, m: u32, d: u32, h: u32, mi: u32) -> CronTime {
        CronTime {
            year: y,
            month: m,
            day: d,
            hour: h,
            minute: mi,
            second: 0,
        }
    }

    #[test]
    fn parses_star() {
        let s = CronSchedule::parse("* * * * *").unwrap();
        assert_eq!(s.minutes.len(), 60);
    }

    #[test]
    fn parses_list_and_range() {
        let s = CronSchedule::parse("1,15 9-17 * * 1-5").unwrap();
        assert_eq!(s.minutes, vec![1, 15]);
        assert_eq!(s.hours, (9..=17).collect::<Vec<_>>());
        assert_eq!(s.days_of_week, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn parses_step_and_names() {
        let s = CronSchedule::parse("*/15 * * * MON,WED").unwrap();
        assert_eq!(s.minutes, vec![0, 15, 30, 45]);
        assert_eq!(s.days_of_week, vec![1, 3]);
    }

    #[test]
    fn rejects_too_few_fields() {
        assert!(CronSchedule::parse("* * * *").is_err());
    }

    #[test]
    fn rejects_out_of_range() {
        assert!(CronSchedule::parse("60 * * * *").is_err());
    }

    #[test]
    fn next_after_every_minute() {
        let s = CronSchedule::parse("* * * * *").unwrap();
        let next = s.next_after(t(2026, 8, 28, 10, 30)).unwrap();
        assert_eq!(next, t(2026, 8, 28, 10, 31));
    }

    #[test]
    fn next_after_midnight() {
        let s = CronSchedule::parse("0 0 * * *").unwrap();
        let next = s.next_after(t(2026, 8, 28, 23, 59)).unwrap();
        assert_eq!(next, t(2026, 8, 29, 0, 0));
    }

    #[test]
    fn next_after_weekday_only() {
        // 2026-08-28 is a Friday (5). "0 9 * * MON-FRI" fires next Monday.
        let s = CronSchedule::parse("0 9 * * MON-FRI").unwrap();
        let next = s.next_after(t(2026, 8, 28, 23, 0)).unwrap();
        // Friday 23:00 -> next is Monday 09:00 (Aug 31).
        assert_eq!(next, t(2026, 8, 31, 9, 0));
    }

    #[test]
    fn next_after_leap_year_feb() {
        let s = CronSchedule::parse("0 12 29 2 *").unwrap();
        // Next Feb 29 after 2026 is 2028.
        let next = s.next_after(t(2026, 1, 1, 0, 0)).unwrap();
        assert_eq!(next, t(2028, 2, 29, 12, 0));
    }

    #[test]
    fn impossible_expression_returns_none() {
        let s = CronSchedule::parse("0 0 30 2 *").unwrap(); // Feb 30 never exists
        assert!(s.next_after(t(2026, 1, 1, 0, 0)).is_none());
    }

    #[test]
    fn day_skipping_still_lands_on_the_exact_fire_time() {
        let s = CronSchedule::parse("30 4 15 6 *").unwrap();
        let next = s.next_after(t(2026, 6, 15, 4, 30)).unwrap();
        assert_eq!(next, t(2027, 6, 15, 4, 30));
    }

    #[test]
    fn month_skipping_still_lands_on_the_exact_fire_time() {
        // March -> next January 1st: the scan must jump months, not days.
        let s = CronSchedule::parse("0 0 1 1 *").unwrap();
        let next = s.next_after(t(2026, 3, 7, 12, 0)).unwrap();
        assert_eq!(next, t(2027, 1, 1, 0, 0));
    }

    #[test]
    fn month_skipping_wraps_the_year() {
        let s = CronSchedule::parse("0 0 1 1 *").unwrap();
        let next = s.next_after(t(2026, 12, 31, 23, 59)).unwrap();
        assert_eq!(next, t(2027, 1, 1, 0, 0));
    }

    #[test]
    fn hour_skipping_still_lands_on_the_exact_fire_time() {
        // Only 04:30 fires: the scan must jump hours, not minutes.
        let s = CronSchedule::parse("30 4 * * *").unwrap();
        let next = s.next_after(t(2026, 8, 28, 0, 1)).unwrap();
        assert_eq!(next, t(2026, 8, 28, 4, 30));
    }

    #[test]
    fn next_five_every_hour() {
        let s = CronSchedule::parse("0 * * * *").unwrap();
        let five = s.next_five(t(2026, 8, 28, 9, 45));
        assert_eq!(five.len(), 5);
        assert_eq!(five[0], t(2026, 8, 28, 10, 0));
        assert_eq!(five[1], t(2026, 8, 28, 11, 0));
        assert_eq!(five[4], t(2026, 8, 28, 14, 0));
    }
}
