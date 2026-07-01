# evoxy-bench

Dev-only benchmark math. The live latency harnesses do the I/O and hand this crate
the measured per-request nanoseconds; everything here is a pure function of those
samples, with no clock and no network, so it gives the same answer on any host.

`LatencySummary` computes nearest-rank percentiles. `NfrProfile` pairs a
through-the-proxy measurement against a direct baseline and derives the added
latency. `ScalabilityCurve` measures how throughput scales and how the tail grows
across a concurrency sweep. `judge` and `judge_scalability` turn those into a
pass/fail verdict against thresholds, and everything renders to JSON so an operator
or an automated judge can read it.

This crate is not shipped in any artifact. It exists so the perf and scale tests
have a stable, testable substrate.
