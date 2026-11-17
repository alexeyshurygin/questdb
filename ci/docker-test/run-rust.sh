#!/usr/bin/env bash
#
# Runs the Rust crates' lints and tests inside a rust-tests container, as the CI workflow does, and leaves the
# output and the exit code in /out.

set -u
out=/out
mkdir -p "$out"
jobs="${CARGO_BUILD_JOBS:-2}"
# One target directory for all crates and no debug info: the crates' tests would otherwise take tens of GiB.
export CARGO_TARGET_DIR=/home/qdb/cargo-target
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
code=0
start=$(date +%s)
{
    for crate in core/rust/qdb-core core/rust/qdb-parquet-meta core/rust/qdbr; do
        echo "=== $crate: clippy and fmt"
        (cd "$crate" && cargo clippy -j "$jobs" --all-targets --all-features -- -D warnings && cargo fmt --check) || code=1
    done
    for crate in core/rust/qdb-core core/rust/qdb-parquet-meta core/rust/qdbr; do
        echo "=== $crate: tests"
        (cd "$crate" && RUSTFLAGS="-D warnings" cargo test -j "$jobs" --all-targets --no-fail-fast) || code=1
    done
    if [[ -d core/rust/parquet2/testing/parquet-testing/data ]]; then
        echo "=== core/rust/parquet2: tests"
        (cd core/rust/parquet2 && PARQUET2_IGNORE_PYARROW_TESTS=1 cargo test -j "$jobs" --all-targets --no-fail-fast) || code=1
    else
        echo "=== core/rust/parquet2: skipped, parquet-testing data is missing"
        code=1
    fi
} > "$out/rust.log" 2>&1
end=$(date +%s)
echo "$code" > "$out/exit-code"
echo "$((end - start))" > "$out/seconds"
cat /sys/fs/cgroup/memory.peak > "$out/memory-peak" 2>/dev/null || true
du -sb /home/qdb/cargo-target 2>/dev/null | cut -f1 > "$out/disk-used"
exit "$code"
