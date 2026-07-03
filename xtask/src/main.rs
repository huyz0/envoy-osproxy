//! Workspace automation. Run via `cargo xtask <command>`.
//!
//! Commands:
//!   ci       Full local gate: fmt, clippy, arch, test, doc, budgets.
//!   fmt      Check formatting (`cargo fmt --check`).
//!   clippy   Lint with warnings denied.
//!   test     Run all tests.
//!   doc      Build docs (warnings denied) and run doc tests.
//!   arch     Crate dependency-direction check (downward-only, INV-1).
//!   budgets  Source-file size budgets; overflow needs a `// JUSTIFY`.
//!   bench    Deterministic instruction-count microbenchmarks (needs valgrind).
//!   coverage Line-coverage gate: >=90% lines via cargo-llvm-cov (xtask excluded).
//!   crypto-free  The shipped extension links no wire crypto (FIPS boundary, M6).
//!   module-image Build the dynamic module into a stock Envoy image (needs Docker).

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let cmd = std::env::args().nth(1).unwrap_or_else(|| "ci".to_owned());
    let result = match cmd.as_str() {
        "ci" => run_ci(),
        "fmt" => fmt(),
        "clippy" => clippy(),
        "test" => test(),
        "doc" => doc(),
        "arch" => arch(),
        "budgets" => budgets(),
        "bench" => bench(),
        "coverage" => coverage(),
        "crypto-free" => crypto_free(),
        "module-image" => module_image(),
        other => Err(format!("unknown command: {other}\n{USAGE}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("xtask: {msg}");
            ExitCode::FAILURE
        }
    }
}

const USAGE: &str = "usage: cargo xtask \
    <ci|fmt|clippy|test|doc|arch|budgets|bench|coverage|crypto-free|module-image>";

/// The line-coverage floor the `coverage` gate enforces (percent).
const COVERAGE_FLOOR_LINES: &str = "90";

/// The stock-Envoy image tag we build the module into. Kept equal to the SDK tag
/// pinned in `evoxy-module-sdk/Cargo.toml` (the ABI hash is load-checked).
const MODULE_IMAGE: &str = "evoxy-envoy:v1.37.0";

fn run_ci() -> Result<(), String> {
    fmt()?;
    clippy()?;
    arch()?;
    crypto_free()?;
    test()?;
    doc()?;
    budgets()?;
    println!("\nxtask: all gates passed \u{2713}");
    Ok(())
}

fn fmt() -> Result<(), String> {
    run("cargo", &["fmt", "--all", "--", "--check"])
}

fn clippy() -> Result<(), String> {
    run(
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    )
}

fn test() -> Result<(), String> {
    run("cargo", &["test", "--workspace"])
}

fn doc() -> Result<(), String> {
    run_env(
        "cargo",
        &["doc", "--workspace", "--no-deps"],
        &[("RUSTDOCFLAGS", "-D warnings")],
    )?;
    run("cargo", &["test", "--workspace", "--doc"])
}

fn bench() -> Result<(), String> {
    if which("valgrind").is_none() {
        println!("xtask: valgrind not found; skipping instruction-count benches");
        return Ok(());
    }
    run("cargo", &["bench", "--workspace"])
}

/// The line-coverage gate (>=90%, NFR-T): `cargo llvm-cov` over the workspace,
/// failing the build when line coverage drops under [`COVERAGE_FLOOR_LINES`].
/// `xtask` itself is excluded (build tooling, not shipped code), matching osproxy's
/// convention. Needs `cargo-llvm-cov` installed; a separate command (not in `ci`)
/// because it recompiles the workspace instrumented, which is minutes not seconds.
fn coverage() -> Result<(), String> {
    if which("cargo-llvm-cov").is_none() {
        return Err(
            "coverage: cargo-llvm-cov not found (install: cargo install cargo-llvm-cov)".into(),
        );
    }
    run(
        "cargo",
        &[
            "llvm-cov",
            "--workspace",
            "--summary-only",
            "--ignore-filename-regex",
            "xtask/src",
            "--fail-under-lines",
            COVERAGE_FLOOR_LINES,
        ],
    )
}

/// Build the dynamic module into a stock Envoy image (ADR-004). Self-contained:
/// the build context is the repo root and the engine crates come from crates.io,
/// so no sibling checkout is needed. Requires Docker. The resulting image is what
/// the `perf_module`/`e2e_module` harnesses load.
fn module_image() -> Result<(), String> {
    if which("docker").is_none() {
        return Err("module-image: docker not found on PATH".into());
    }
    let root = workspace_root();
    println!(
        "+ docker build -t {MODULE_IMAGE} (context {})",
        root.display()
    );
    let status = Command::new("docker")
        .args([
            "build",
            "-f",
            "crates/evoxy-module/docker/Dockerfile",
            "-t",
            MODULE_IMAGE,
            ".",
        ])
        .current_dir(&root)
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        println!("module-image: built {MODULE_IMAGE} \u{2713}");
        Ok(())
    } else {
        Err(format!("module-image: docker build failed ({status})"))
    }
}

/// Downward-only crate dependencies (INV-1, docs/08). `evoxy-abi` is the leaf
/// wire model and must not depend on any other internal crate; `evoxy-adapter`
/// may depend on `evoxy-abi` and the reused osproxy brain crates, nothing more.
fn arch() -> Result<(), String> {
    let root = workspace_root();
    // evoxy-abi is the leaf: it must not depend on any other internal crate.
    let abi = read(&root.join("crates/evoxy-abi/Cargo.toml"))?;
    for forbidden in ["evoxy-adapter", "evoxy-route"] {
        if section_before_dev(&abi).contains(forbidden) {
            return Err(format!(
                "arch: evoxy-abi must not depend on {forbidden} (INV-1)"
            ));
        }
    }
    // evoxy-route is a non-runtime dep of evoxy-adapter (adapter stays the pure
    // ctx seam): the adapter must not take a runtime dep on route.
    let adapter = read(&root.join("crates/evoxy-adapter/Cargo.toml"))?;
    if section_before_dev(&adapter).contains("evoxy-route") {
        return Err("arch: evoxy-adapter must not depend on evoxy-route (INV-1)".into());
    }
    println!("arch: dependency direction ok \u{2713}");
    Ok(())
}

/// The shipped extension links **no wire crypto** — Envoy owns the data-plane TLS
/// (ADR-006, the FIPS boundary). We assert every shipped crate's non-dev
/// dependency tree contains no TLS/crypto crate, so a stray `tonic` tls feature or
/// a rustls dep can never silently create a FIPS obligation for our code. Test-only
/// crypto (the mTLS e2e's rcgen/reqwest) is a dev-dependency and excluded.
fn crypto_free() -> Result<(), String> {
    // Crate names (as they appear in `cargo tree`) that would put wire crypto in
    // our binary. `ring v`/`aws-lc` etc. are matched with a trailing space or `-`
    // so we don't false-match unrelated substrings.
    const FORBIDDEN: &[&str] = &[
        "rustls",
        "ring v",
        "aws-lc-rs",
        "aws-lc-sys",
        "openssl",
        "boring",
        "native-tls",
    ];
    const SHIPPED: &[&str] = &[
        "evoxy-abi",
        "evoxy-adapter",
        "evoxy-route",
        "evoxy-filter",
        "evoxy-extproc",
    ];
    for krate in SHIPPED {
        let tree = capture("cargo", &["tree", "-p", krate, "-e", "no-dev"])?;
        for forbidden in FORBIDDEN {
            if tree.contains(forbidden) {
                return Err(format!(
                    "crypto-free: {krate} links `{}` on the data path — the wire is \
                     Envoy's (ADR-006); keep crypto out of the shipped extension",
                    forbidden.trim_end_matches(" v")
                ));
            }
        }
    }
    println!("crypto-free: shipped extension links no wire crypto (Envoy owns TLS) \u{2713}");
    Ok(())
}

/// The manifest text before `[dev-dependencies]`, so a dev-only edge (tests) is
/// not counted as a runtime dependency-direction violation.
fn section_before_dev(manifest: &str) -> &str {
    match manifest.find("[dev-dependencies]") {
        Some(idx) => &manifest[..idx],
        None => manifest,
    }
}

/// Source files over the budget must carry a `// JUSTIFY:` line (docs/08). Keeps
/// modules at a reviewable altitude; an explicit override is a review decision.
fn budgets() -> Result<(), String> {
    const MAX_LINES: usize = 400;
    let root = workspace_root();
    let mut violations = Vec::new();
    for path in rust_sources(&root.join("crates")) {
        let text = read(&path)?;
        let lines = text.lines().count();
        if lines > MAX_LINES && !text.contains("// JUSTIFY") {
            violations.push(format!("  {} ({lines} lines)", path.display()));
        }
    }
    if violations.is_empty() {
        println!("budgets: all source files within {MAX_LINES} lines \u{2713}");
        Ok(())
    } else {
        Err(format!(
            "budgets: files over {MAX_LINES} lines without `// JUSTIFY`:\n{}",
            violations.join("\n")
        ))
    }
}

// ---- helpers ----

fn rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // Skip build outputs (e.g. the excluded module's own `target/`, which
            // holds generated bindings) — only our source counts toward budgets.
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            out.extend(rust_sources(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))
}

fn workspace_root() -> PathBuf {
    // xtask runs from the workspace root under `cargo xtask`.
    std::env::var("CARGO_MANIFEST_DIR")
        .map(|d| {
            PathBuf::from(d)
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default()
        })
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn which(bin: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(bin))
        .find(|candidate| candidate.exists())
}

fn run(bin: &str, args: &[&str]) -> Result<(), String> {
    run_env(bin, args, &[])
}

/// Run a command and return its stdout (for gates that inspect output).
fn capture(bin: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(bin)
        .args(args)
        .output()
        .map_err(|e| format!("failed to spawn `{bin}`: {e}"))?;
    if !output.status.success() {
        return Err(format!("`{bin} {}` failed", args.join(" ")));
    }
    String::from_utf8(output.stdout).map_err(|e| format!("`{bin}` output not utf-8: {e}"))
}

fn run_env(bin: &str, args: &[&str], envs: &[(&str, &str)]) -> Result<(), String> {
    println!("+ {bin} {}", args.join(" "));
    let mut cmd = Command::new(bin);
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to spawn `{bin}`: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{bin} {}` failed ({status})", args.join(" ")))
    }
}
