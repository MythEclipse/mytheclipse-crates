//! A centralized, thread-safe metrics collector (feature `observability`).
//!
//! [`MetricsCollector`] records runtime health statistics — active threads,
//! remaining queue capacity, average task duration, custom counters and gauges
//! — and can render them in the [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)
//! for a scraper, or as a structured [`MetricsSnapshot`] that a downstream adapter
//! can forward (e.g. to OpenTelemetry).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A single histogram bucket: accumulates `count` observations and their
/// total duration for an average.
#[derive(Debug, Clone, Default)]
struct Histogram {
    count: u64,
    total_micros: u128,
}

#[derive(Default)]
struct Inner {
    active_threads: AtomicUsize,
    queue_capacity_total: AtomicUsize,
    queue_capacity_remaining: AtomicUsize,
    task_count: AtomicU64,
    total_task_micros: AtomicU64,
    counters: Mutex<HashMap<String, u64>>,
    gauges: Mutex<HashMap<String, f64>>,
    histograms: Mutex<HashMap<String, Histogram>>,
}

/// A thread-safe handle to a metrics collector.
///
/// Cheap to clone (shared state). Collects data updated from any thread and
/// exports it on demand.
#[derive(Clone, Default)]
pub struct MetricsCollector {
    inner: Arc<Inner>,
}

impl MetricsCollector {
    /// Builds an empty collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that a task ran for `duration`, updating the running task
    /// count and average.
    pub fn record_task(&self, duration: Duration) {
        self.inner.task_count.fetch_add(1, Ordering::AcqRel);
        self.inner
            .total_task_micros
            .fetch_add(duration.as_micros() as u64, Ordering::AcqRel);
    }

    /// Sets the observed number of active worker threads.
    pub fn set_active_threads(&self, n: usize) {
        self.inner.active_threads.store(n, Ordering::Release);
    }

    /// Sets the queue's total capacity.
    pub fn set_queue_capacity(&self, n: usize) {
        self.inner.queue_capacity_total.store(n, Ordering::Release);
    }

    /// Sets the queue's currently remaining capacity.
    pub fn set_queue_remaining(&self, n: usize) {
        self.inner
            .queue_capacity_remaining
            .store(n, Ordering::Release);
    }

    /// Increments a counter by `by`.
    pub fn inc_counter(&self, name: &str, by: u64) {
        let mut counters = self.inner.counters.lock().unwrap();
        // Look up first so the steady-state path does not allocate a new key.
        match counters.get_mut(name) {
            Some(value) => *value += by,
            None => {
                counters.insert(name.to_owned(), by);
            }
        }
    }

    /// Sets a gauge to `value`.
    pub fn set_gauge(&self, name: &str, value: f64) {
        let mut gauges = self.inner.gauges.lock().unwrap();
        match gauges.get_mut(name) {
            Some(slot) => *slot = value,
            None => {
                gauges.insert(name.to_owned(), value);
            }
        }
    }

    /// Records `duration` into the histogram named `name`.
    pub fn observe(&self, name: &str, duration: Duration) {
        let micros = duration.as_micros();
        let mut histos = self.inner.histograms.lock().unwrap();
        match histos.get_mut(name) {
            Some(h) => {
                h.count += 1;
                h.total_micros += micros;
            }
            None => {
                histos.insert(
                    name.to_owned(),
                    Histogram {
                        count: 1,
                        total_micros: micros,
                    },
                );
            }
        }
    }

    /// Total number of recorded tasks.
    pub fn task_count(&self) -> u64 {
        self.inner.task_count.load(Ordering::Acquire)
    }

    /// Average task duration, if any tasks have been recorded.
    pub fn avg_task_duration(&self) -> Option<Duration> {
        let count = self.task_count();
        if count == 0 {
            return None;
        }
        let total = self.inner.total_task_micros.load(Ordering::Acquire);
        Some(Duration::from_micros(total / count))
    }

    /// Number of active worker threads last recorded.
    pub fn active_threads(&self) -> usize {
        self.inner.active_threads.load(Ordering::Acquire)
    }

    /// Takes a structured snapshot of the collector's state.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            active_threads: self.active_threads(),
            queue_capacity_total: self.inner.queue_capacity_total.load(Ordering::Acquire),
            queue_capacity_remaining: self.inner.queue_capacity_remaining.load(Ordering::Acquire),
            task_count: self.task_count(),
            avg_task_duration_micros: self.avg_task_duration().map(|d| d.as_micros() as u64),
            counters: self
                .inner
                .counters
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            gauges: self
                .inner
                .gauges
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            histograms: self
                .inner
                .histograms
                .lock()
                .unwrap()
                .iter()
                .map(|(k, h)| {
                    (
                        k.clone(),
                        HistogramSnapshot {
                            count: h.count,
                            avg_micros: if h.count > 0 {
                                Some((h.total_micros / h.count as u128) as u64)
                            } else {
                                None
                            },
                        },
                    )
                })
                .collect(),
        }
    }

    /// Renders the collector in the Prometheus text exposition format.
    ///
    /// The output is suitable to serve with
    /// `Content-Type: text/plain; version=0.0.4`.
    pub fn export_prometheus(&self) -> String {
        let snap = self.snapshot();

        let mut counters: Vec<_> = snap.counters.into_iter().collect();
        counters.sort_by(|a, b| a.0.cmp(&b.0));
        let mut gauges: Vec<_> = snap.gauges.into_iter().collect();
        gauges.sort_by(|a, b| a.0.cmp(&b.0));
        let mut histos: Vec<_> = snap.histograms.into_iter().collect();
        histos.sort_by(|a, b| a.0.cmp(&b.0));

        // Every line is written straight into the output buffer: formatting into
        // a temporary `String` first would allocate once per line, which is most
        // of what a scrape used to cost.
        let mut out = String::with_capacity(
            BASE_EXPORT_BYTES
                + (counters.len() + gauges.len()) * SERIES_EXPORT_BYTES
                + histos.len() * HISTOGRAM_EXPORT_BYTES,
        );

        out.push_str("# HELP mytheclipse_active_threads Number of active worker threads.\n");
        out.push_str("# TYPE mytheclipse_active_threads gauge\n");
        let _ = writeln!(out, "mytheclipse_active_threads {}", snap.active_threads);
        out.push_str("# HELP mytheclipse_queue_capacity_total Total queue capacity.\n");
        out.push_str("# TYPE mytheclipse_queue_capacity_total gauge\n");
        let _ = writeln!(
            out,
            "mytheclipse_queue_capacity_total {}",
            snap.queue_capacity_total
        );
        out.push_str("# HELP mytheclipse_queue_capacity_remaining Remaining queue capacity.\n");
        out.push_str("# TYPE mytheclipse_queue_capacity_remaining gauge\n");
        let _ = writeln!(
            out,
            "mytheclipse_queue_capacity_remaining {}",
            snap.queue_capacity_remaining
        );
        out.push_str("# HELP mytheclipse_task_count Total tasks recorded.\n");
        out.push_str("# TYPE mytheclipse_task_count counter\n");
        let _ = writeln!(out, "mytheclipse_task_count {}", snap.task_count);
        if let Some(avg) = snap.avg_task_duration_micros {
            out.push_str(
                "# HELP mytheclipse_task_duration_avg Average task duration in microseconds.\n",
            );
            out.push_str("# TYPE mytheclipse_task_duration_avg gauge\n");
            let _ = writeln!(out, "mytheclipse_task_duration_avg {avg}");
        }

        for (name, value) in counters {
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        }

        for (name, value) in gauges {
            let _ = writeln!(out, "# TYPE {name} gauge");
            let _ = writeln!(out, "{name} {value}");
        }

        for (name, h) in histos {
            if let Some(avg) = h.avg_micros {
                let _ = writeln!(out, "# TYPE {name}_count counter");
                let _ = writeln!(out, "{name}_count {}", h.count);
                let _ = writeln!(out, "# TYPE {name}_avg gauge");
                let _ = writeln!(out, "{name}_avg {avg}");
            }
        }

        out
    }
}

/// Rough size of the fixed part of a Prometheus scrape, used to size the
/// output buffer up front.
const BASE_EXPORT_BYTES: usize = 512;
/// Rough size of one counter or gauge (a `# TYPE` line plus a sample line).
const SERIES_EXPORT_BYTES: usize = 64;
/// Rough size of one histogram (four lines).
const HISTOGRAM_EXPORT_BYTES: usize = 128;

/// A structured, serializable view of a [`MetricsCollector`], suitable for
/// forwarding to OpenTelemetry or another backend.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Number of active worker threads last recorded.
    pub active_threads: usize,
    /// Total queue capacity last recorded.
    pub queue_capacity_total: usize,
    /// Remaining queue capacity last recorded.
    pub queue_capacity_remaining: usize,
    /// Total tasks recorded.
    pub task_count: u64,
    /// Average task duration in microseconds, if any.
    pub avg_task_duration_micros: Option<u64>,
    /// Named counters.
    pub counters: HashMap<String, u64>,
    /// Named gauges.
    pub gauges: HashMap<String, f64>,
    /// Named histogram aggregates.
    pub histograms: HashMap<String, HistogramSnapshot>,
}

/// Aggregated view of one histogram.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Number of observations.
    pub count: u64,
    /// Average observation value in microseconds, if any.
    pub avg_micros: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_task_and_average() {
        let m = MetricsCollector::new();
        assert_eq!(m.task_count(), 0);
        assert!(m.avg_task_duration().is_none());

        m.record_task(Duration::from_millis(10));
        m.record_task(Duration::from_millis(20));
        assert_eq!(m.task_count(), 2);
        assert_eq!(m.avg_task_duration().unwrap(), Duration::from_millis(15));
    }

    #[test]
    fn tracks_threads_and_queue() {
        let m = MetricsCollector::new();
        m.set_active_threads(8);
        m.set_queue_capacity(100);
        m.set_queue_remaining(42);
        let snap = m.snapshot();
        assert_eq!(snap.active_threads, 8);
        assert_eq!(snap.queue_capacity_total, 100);
        assert_eq!(snap.queue_capacity_remaining, 42);
    }

    #[test]
    fn custom_counters_and_gauges() {
        let m = MetricsCollector::new();
        m.inc_counter("reqs", 3);
        m.inc_counter("reqs", 2);
        m.set_gauge("temp", 21.5);
        let snap = m.snapshot();
        assert_eq!(snap.counters["reqs"], 5);
        assert_eq!(snap.gauges["temp"], 21.5);
    }

    #[test]
    fn observe_accumulates_histogram() {
        let m = MetricsCollector::new();
        m.observe("latency", Duration::from_millis(100));
        m.observe("latency", Duration::from_millis(300));
        let snap = m.snapshot();
        let h = &snap.histograms["latency"];
        assert_eq!(h.count, 2);
        assert_eq!(h.avg_micros.unwrap(), 200_000);
    }

    #[test]
    fn prometheus_export_contains_lines() {
        let m = MetricsCollector::new();
        m.record_task(Duration::from_millis(5));
        m.set_active_threads(4);
        m.inc_counter("my_reqs", 7);
        m.set_gauge("my_temp", 1.5);
        m.observe("my_lat_ms", Duration::from_millis(12));

        let out = m.export_prometheus();
        assert!(out.contains("# TYPE mytheclipse_task_count counter"));
        assert!(out.contains("mytheclipse_task_count 1"));
        assert!(out.contains("mytheclipse_active_threads 4"));
        assert!(out.contains("# TYPE my_reqs counter"));
        assert!(out.contains("my_reqs 7"));
        assert!(out.contains("# TYPE my_temp gauge"));
        assert!(out.contains("my_temp 1.5"));
        assert!(out.contains("my_lat_ms_count 1"));
        assert!(out.contains("my_lat_ms_avg 12000"));
    }
}
