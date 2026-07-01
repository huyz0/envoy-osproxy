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
//!
//! Mirrors the osproxy sister project's gate (docs/08, docs/09).

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

const USAGE: &str = "usage: cargo xtask <ci|fmt|clippy|test|doc|arch|budgets|bench>";

fn run_ci() -> Result<(), String> {
    fmt()?;
    clippy()?;
    arch()?;
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
