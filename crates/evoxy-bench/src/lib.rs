//! The NFR-P benchmark substrate — pure, deterministic, dev-only.
//!
//! The live harnesses (`evoxy-extproc/tests/perf.rs`, `perf_module.rs`, `scale.rs`)
//! do the I/O and hand this crate the measured per-request nanoseconds. Everything
//! here is a pure function of those samples: [`LatencySummary`] (nearest-rank
//! percentiles), [`NfrProfile`] (proxy vs. baseline with derived added-latency),
//! [`ScalabilityCurve`] (throughput scaling + tail amplification under load), and
//! the [`judge`]/[`judge_scalability`] gates that turn a profile into a
//! [`Verdict`]. No clock, no network — so it gates the same way on any host, and
//! the emitted JSON is the substrate an operator (or an LLM) reasons over.
#![deny(missing_docs)]
// This is percentile/throughput math over measured nanoseconds: u64→f64 for ratios
// and u64→usize for indexing a same-length slice are inherent and safe here. `to_json`
// on a `Copy` summary reads more naturally by reference.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::wrong_self_convention
)]
// JUSTIFY: one cohesive NFR-P substrate — the latency/profile/curve types, their
// judges, and the property tests that pin the percentile + scaling math read as a
// single narrative; splitting the types from the gates that consume them would
// scatter it.

use std::fmt::Write as _;

/// Nearest-rank percentile summary of a set of latencies (nanoseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencySummary {
    /// Number of samples summarized.
    pub count: u64,
    /// Smallest sample.
    pub min_ns: u64,
    /// Largest sample.
    pub max_ns: u64,
    /// Arithmetic mean.
    pub mean_ns: u64,
    /// Median (p50).
    pub p50_ns: u64,
    /// p90.
    pub p90_ns: u64,
    /// p99.
    pub p99_ns: u64,
}

impl LatencySummary {
    /// Summarize `samples`; `None` if empty (no percentile is defined).
    #[must_use]
    pub fn from_nanos(samples: &[u64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let count = sorted.len() as u64;
        let sum: u128 = sorted.iter().map(|&v| u128::from(v)).sum();
        let mean_ns = u64::try_from(sum / u128::from(count)).unwrap_or(u64::MAX);
        Some(Self {
            count,
            min_ns: sorted[0],
            max_ns: sorted[sorted.len() - 1],
            mean_ns,
            p50_ns: nearest_rank(&sorted, 50),
            p90_ns: nearest_rank(&sorted, 90),
            p99_ns: nearest_rank(&sorted, 99),
        })
    }

    fn to_json(&self) -> String {
        format!(
            "{{\"count\": {}, \"min_ns\": {}, \"max_ns\": {}, \"mean_ns\": {}, \
             \"p50_ns\": {}, \"p90_ns\": {}, \"p99_ns\": {}}}",
            self.count,
            self.min_ns,
            self.max_ns,
            self.mean_ns,
            self.p50_ns,
            self.p90_ns,
            self.p99_ns
        )
    }
}

/// Nearest-rank percentile: rank = ceil(p/100 · n), 1-indexed, clamped to `[1, n]`.
fn nearest_rank(sorted: &[u64], p: u64) -> u64 {
    let n = sorted.len() as u64;
    let rank = (p * n).div_ceil(100).max(1).min(n);
    sorted[(rank - 1) as usize]
}

/// A proxy-vs-baseline latency profile for one workload (the NFR-P A/B).
#[derive(Debug, Clone, Copy)]
pub struct NfrProfile {
    /// Samples per leg.
    pub samples: u64,
    /// Concurrency the profile was gathered at.
    pub concurrency: u32,
    /// Direct-to-upstream baseline.
    pub baseline: LatencySummary,
    /// Through-the-proxy measurement.
    pub proxy: LatencySummary,
    /// Fraction of upstream requests served on a reused connection.
    pub pool_reuse_rate: f64,
    /// Achieved throughput through the proxy (req/s).
    pub throughput_rps: f64,
}

impl NfrProfile {
    /// Added p50 latency the proxy imposes over the baseline (ns, saturating).
    #[must_use]
    pub fn added_p50_ns(&self) -> u64 {
        self.proxy.p50_ns.saturating_sub(self.baseline.p50_ns)
    }

    /// Added p99 latency the proxy imposes over the baseline (ns, saturating).
    #[must_use]
    pub fn added_p99_ns(&self) -> u64 {
        self.proxy.p99_ns.saturating_sub(self.baseline.p99_ns)
    }

    /// Emit the profile as JSON (the LLM/operator-judge substrate).
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\n  \"samples\": {},\n  \"concurrency\": {},\n  \"added_p50_ns\": {},\n  \
             \"added_p99_ns\": {},\n  \"baseline\": {},\n  \"proxy\": {},\n  \
             \"pool_reuse_rate\": {},\n  \"throughput_rps\": {}\n}}",
            self.samples,
            self.concurrency,
            self.added_p50_ns(),
            self.added_p99_ns(),
            self.baseline.to_json(),
            self.proxy.to_json(),
            self.pool_reuse_rate,
            self.throughput_rps
        )
    }
}

/// Gate thresholds for [`judge`] (per-NFR bounds).
#[derive(Debug, Clone, Copy)]
pub struct NfrThresholds {
    /// Max acceptable added p50 (ms) — NFR-P1.
    pub added_p50_ms: f64,
    /// Max acceptable added p99 (ms) — NFR-P2.
    pub added_p99_ms: f64,
    /// Min acceptable pool-reuse rate — NFR-P4.
    pub pool_reuse_floor: f64,
}

impl NfrThresholds {
    /// Provisional bounds pending authoritative per-host calibration.
    #[must_use]
    pub fn provisional() -> Self {
        Self {
            added_p50_ms: 2.0,
            added_p99_ms: 10.0,
            pool_reuse_floor: 0.99,
        }
    }
}

/// One NFR check within a [`Verdict`].
#[derive(Debug, Clone)]
pub struct Finding {
    /// The NFR id (e.g. `NFR-P1`).
    pub nfr: String,
    /// Whether this check passed.
    pub pass: bool,
    /// Human/LLM-readable measured-vs-bound detail.
    pub detail: String,
}

/// The outcome of judging a profile or curve: an overall pass plus per-NFR findings.
#[derive(Debug, Clone)]
pub struct Verdict {
    /// True iff every finding passed.
    pub pass: bool,
    /// The individual checks.
    pub findings: Vec<Finding>,
}

impl Verdict {
    fn from_findings(findings: Vec<Finding>) -> Self {
        let pass = findings.iter().all(|f| f.pass);
        Self { pass, findings }
    }

    /// Emit the verdict as JSON.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut out = format!("{{\n  \"pass\": {},\n  \"findings\": [", self.pass);
        for (i, f) in self.findings.iter().enumerate() {
            let sep = if i == 0 { "" } else { "," };
            let _ = write!(
                out,
                "{sep}\n    {{\n      \"nfr\": \"{}\",\n      \"pass\": {},\n      \"detail\": \"{}\"\n    }}",
                f.nfr, f.pass, f.detail
            );
        }
        out.push_str("\n  ]\n}");
        out
    }
}

/// Judge a profile against `thresholds` into a pass/fail [`Verdict`] (NFR-P1/P2/P4).
#[must_use]
pub fn judge(profile: &NfrProfile, thresholds: &NfrThresholds) -> Verdict {
    let added_p50_ms = profile.added_p50_ns() as f64 / 1e6;
    let added_p99_ms = profile.added_p99_ns() as f64 / 1e6;
    Verdict::from_findings(vec![
        Finding {
            nfr: "NFR-P1".to_owned(),
            pass: added_p50_ms <= thresholds.added_p50_ms,
            detail: format!(
                "added p50 {added_p50_ms:.3} ms vs bound {:.3} ms",
                thresholds.added_p50_ms
            ),
        },
        Finding {
            nfr: "NFR-P2".to_owned(),
            pass: added_p99_ms <= thresholds.added_p99_ms,
            detail: format!(
                "added p99 {added_p99_ms:.3} ms vs bound {:.3} ms",
                thresholds.added_p99_ms
            ),
        },
        Finding {
            nfr: "NFR-P4".to_owned(),
            pass: profile.pool_reuse_rate >= thresholds.pool_reuse_floor,
            detail: format!(
                "pool reuse {:.4} vs floor {:.4}",
                profile.pool_reuse_rate, thresholds.pool_reuse_floor
            ),
        },
    ])
}

/// One point on a [`ScalabilityCurve`]: the latency + throughput at a concurrency.
#[derive(Debug, Clone, Copy)]
pub struct ScalabilityPoint {
    /// The concurrency this point was measured at.
    pub concurrency: u32,
    /// The latency summary at this concurrency.
    pub latency: LatencySummary,
    /// The achieved throughput (req/s) at this concurrency.
    pub throughput_rps: f64,
}

/// A concurrency sweep: points ordered by rising concurrency.
#[derive(Debug, Clone)]
pub struct ScalabilityCurve {
    /// The measured points (ascending concurrency).
    pub points: Vec<ScalabilityPoint>,
}

impl ScalabilityCurve {
    /// Build a curve; `None` unless there are at least two points (a curve needs a
    /// low and a high end to compare).
    #[must_use]
    pub fn new(points: Vec<ScalabilityPoint>) -> Option<Self> {
        if points.len() < 2 {
            return None;
        }
        Some(Self { points })
    }

    fn first(&self) -> &ScalabilityPoint {
        &self.points[0]
    }

    fn last(&self) -> &ScalabilityPoint {
        &self.points[self.points.len() - 1]
    }

    /// How much the p99 tail grew from the lowest to the highest concurrency (×).
    #[must_use]
    pub fn tail_amplification(&self) -> f64 {
        let lo = self.first().latency.p99_ns as f64;
        let hi = self.last().latency.p99_ns as f64;
        if lo <= 0.0 {
            return 1.0;
        }
        hi / lo
    }

    /// How much throughput grew from the lowest to the highest concurrency (×).
    #[must_use]
    pub fn throughput_scaling(&self) -> f64 {
        let lo = self.first().throughput_rps;
        let hi = self.last().throughput_rps;
        if lo <= 0.0 {
            return 0.0;
        }
        hi / lo
    }
}

/// Gate thresholds for [`judge_scalability`].
#[derive(Debug, Clone, Copy)]
pub struct ScalabilityThresholds {
    /// Max acceptable tail amplification (×) — the tail must stay bounded.
    pub max_tail_amplification: f64,
    /// Min acceptable throughput scaling (×) — concurrency must buy work.
    pub min_throughput_scaling: f64,
}

impl ScalabilityThresholds {
    /// Provisional bounds pending authoritative per-host calibration.
    #[must_use]
    pub fn provisional() -> Self {
        Self {
            max_tail_amplification: 10.0,
            min_throughput_scaling: 1.5,
        }
    }
}

/// Judge a scalability curve: the tail stayed bounded and concurrency bought work.
#[must_use]
pub fn judge_scalability(curve: &ScalabilityCurve, thresholds: &ScalabilityThresholds) -> Verdict {
    let tail = curve.tail_amplification();
    let scaling = curve.throughput_scaling();
    Verdict::from_findings(vec![
        Finding {
            nfr: "NFR-P2".to_owned(),
            pass: tail <= thresholds.max_tail_amplification,
            detail: format!(
                "tail amplification {tail:.2}x vs bound {:.2}x",
                thresholds.max_tail_amplification
            ),
        },
        Finding {
            nfr: "NFR-P3".to_owned(),
            pass: scaling >= thresholds.min_throughput_scaling,
            detail: format!(
                "throughput scaling {scaling:.2}x vs floor {:.2}x",
                thresholds.min_throughput_scaling
            ),
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(samples: &[u64]) -> LatencySummary {
        LatencySummary::from_nanos(samples).expect("non-empty")
    }

    #[test]
    fn empty_samples_have_no_summary() {
        assert!(LatencySummary::from_nanos(&[]).is_none());
    }

    #[test]
    fn nearest_rank_percentiles_are_stable() {
        let s = summary(&(1..=100).map(|v| v * 1_000).collect::<Vec<_>>());
        assert_eq!(s.count, 100);
        assert_eq!(s.min_ns, 1_000);
        assert_eq!(s.max_ns, 100_000);
        // nearest-rank: p50 = rank 50, p90 = 90, p99 = 99.
        assert_eq!(s.p50_ns, 50_000);
        assert_eq!(s.p90_ns, 90_000);
        assert_eq!(s.p99_ns, 99_000);
    }

    #[test]
    fn added_latency_is_saturating() {
        let baseline = summary(&[10, 10, 10]);
        let proxy = summary(&[30, 30, 30]);
        let profile = NfrProfile {
            samples: 3,
            concurrency: 1,
            baseline,
            proxy,
            pool_reuse_rate: 1.0,
            throughput_rps: 100.0,
        };
        assert_eq!(profile.added_p50_ns(), 20);
        // A faster proxy than baseline never underflows.
        let inverted = NfrProfile {
            baseline: proxy,
            proxy: baseline,
            ..profile
        };
        assert_eq!(inverted.added_p50_ns(), 0);
    }

    #[test]
    fn judge_passes_within_bounds_and_fails_outside() {
        let ok = NfrProfile {
            samples: 3,
            concurrency: 1,
            baseline: summary(&[1_000_000]),
            proxy: summary(&[1_500_000]), // +0.5 ms
            pool_reuse_rate: 1.0,
            throughput_rps: 100.0,
        };
        assert!(judge(&ok, &NfrThresholds::provisional()).pass);

        let slow = NfrProfile {
            proxy: summary(&[9_000_000]),
            ..ok
        }; // +8 ms p50
        assert!(!judge(&slow, &NfrThresholds::provisional()).pass);
    }

    #[test]
    fn curve_needs_two_points_and_measures_scaling() {
        assert!(ScalabilityCurve::new(vec![]).is_none());
        let p = |c: u32, lat: u64, rps: f64| ScalabilityPoint {
            concurrency: c,
            latency: summary(&[lat]),
            throughput_rps: rps,
        };
        let curve =
            ScalabilityCurve::new(vec![p(1, 1_000, 50.0), p(8, 2_000, 400.0)]).expect("two points");
        assert!((curve.throughput_scaling() - 8.0).abs() < 1e-9);
        assert!((curve.tail_amplification() - 2.0).abs() < 1e-9);
        assert!(judge_scalability(&curve, &ScalabilityThresholds::provisional()).pass);
    }
}

#[cfg(test)]
mod render_tests {
    use super::*;

    /// The JSON renderers carry every field an operator/LLM judge keys on.
    #[test]
    fn profile_and_verdict_render_their_fields() {
        let samples = vec![1_000_000, 2_000_000, 3_000_000];
        let baseline = LatencySummary::from_nanos(&samples).expect("a summary");
        let proxy = LatencySummary::from_nanos(&samples).expect("a summary");
        let profile = NfrProfile {
            samples: 3,
            concurrency: 1,
            baseline,
            proxy,
            pool_reuse_rate: 1.0,
            throughput_rps: 100.0,
        };
        let json = profile.to_json();
        for key in [
            "added_p50_ns",
            "added_p99_ns",
            "baseline",
            "proxy",
            "pool_reuse_rate",
            "throughput_rps",
            "p90_ns",
        ] {
            assert!(json.contains(key), "missing {key} in {json}");
        }

        let verdict = judge(&profile, &NfrThresholds::provisional());
        let vjson = verdict.to_json();
        assert!(vjson.contains("\"pass\""), "{vjson}");
        assert!(vjson.contains("\"findings\""), "{vjson}");
        assert!(vjson.contains("\"nfr\""), "{vjson}");
    }

    /// The scalability ratios guard their zero denominators instead of dividing.
    #[test]
    fn curve_ratios_guard_zero_baselines() {
        let zero = LatencySummary {
            count: 1,
            min_ns: 0,
            max_ns: 0,
            mean_ns: 0,
            p50_ns: 0,
            p90_ns: 0,
            p99_ns: 0,
        };
        let points = vec![
            ScalabilityPoint {
                concurrency: 1,
                latency: zero,
                throughput_rps: 0.0,
            },
            ScalabilityPoint {
                concurrency: 8,
                latency: zero,
                throughput_rps: 100.0,
            },
        ];
        let curve = ScalabilityCurve::new(points).expect("two points");
        assert!((curve.tail_amplification() - 1.0).abs() < f64::EPSILON);
        assert!(curve.throughput_scaling().abs() < f64::EPSILON);
    }
}
