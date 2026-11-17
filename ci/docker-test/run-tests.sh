#!/usr/bin/env bash
#
# Runs the test suites on Linux in Docker, in parallel, within the CPUs and memory Docker has free.
#
#   ci/docker-test/run-tests.sh [options]
#
#   --ref REV             revision to test (default HEAD); only committed files reach the images
#   --tests PATTERN       run only these Java tests (a surefire -Dtest pattern), in one container
#   --no-java             skip the Java tests
#   --no-rust             skip the Rust tests and lints
#   --e2e                 also build the server image and run the TLS and replication Docker suites
#   --parallel N          containers at a time (default: what free CPUs and memory allow)
#   --shards N            Java shards (default: 2 per parallel slot, so a free slot takes the next shard)
#   --mem-per-job SIZE    memory limit of each container, RAM disk included (default 12g)
#   --heap-percent N      the test JVM's heap as a share of the container's memory (default 40)
#   --platform P          linux/arm64 or linux/amd64 (default: Docker's native platform)
#   --out DIR             where results go (default ci/docker-test/out/<timestamp>)
#   --reserve SIZE        memory left free in the Docker VM (default: 4g or 10% of it, whichever is more)
#   --wait                when there is no room for one container, wait for it instead of failing
#   --disk-per-job SIZE   free disk each container may need in the Docker VM (default 3g)
#   --test-tmpfs SIZE     RAM disk for the tests' data inside each container, counted in its memory (default 6g;
#                         0 keeps the data on the container's disk)
#
# Memory: each container gets a hard limit with no swap, so a job that needs more is killed (and reported) instead of
# pushing the machine into swap. The number of parallel containers is the smaller of what the free CPUs and the free
# memory allow, where free memory is the Docker VM's total minus what running containers use minus the reserve.
#
# Results: <out>/<job>/ holds each job's surefire reports, Maven or cargo output, exit code, run time and peak memory;
# <out>/summary.txt lists every failing test. Run times feed the next run's shard plan
# (~/.cache/questdb-docker-test/timings.tsv, seeded by ci/docker-test/timings.tsv).

set -euo pipefail

if (( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 3) )); then
    echo "needs bash 4.3 or later (on macOS: brew install bash)" >&2
    exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(git -C "$here" rev-parse --show-toplevel)"

ref=HEAD
tests=""
run_java=1
run_rust=1
run_e2e=0
parallel=""
shards=""
mem_per_job="12g"
heap_percent=40
platform=""
out=""
reserve=""
wait_for_room=0
disk_per_job="3g"
test_tmpfs="6g"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ref) ref="$2"; shift 2 ;;
        --tests) tests="$2"; shift 2 ;;
        --no-java) run_java=0; shift ;;
        --no-rust) run_rust=0; shift ;;
        --e2e) run_e2e=1; shift ;;
        --parallel) parallel="$2"; shift 2 ;;
        --shards) shards="$2"; shift 2 ;;
        --mem-per-job) mem_per_job="$2"; shift 2 ;;
        --heap-percent) heap_percent="$2"; shift 2 ;;
        --platform) platform="$2"; shift 2 ;;
        --out) out="$2"; shift 2 ;;
        --reserve) reserve="$2"; shift 2 ;;
        --wait) wait_for_room=1; shift ;;
        --disk-per-job) disk_per_job="$2"; shift 2 ;;
        --test-tmpfs) test_tmpfs="$2"; shift 2 ;;
        -h|--help) sed -n '3,30p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

# "6g", "512m", "4.721GiB", "718.5MiB", "1.2GB" -> bytes
to_bytes() {
    awk -v v="$1" 'BEGIN {
        n = v + 0; u = tolower(v); sub(/^[0-9.]+/, "", u)
        m = 1
        if (u ~ /^k/) m = 1024; else if (u ~ /^m/) m = 1024 ^ 2; else if (u ~ /^g/) m = 1024 ^ 3; else if (u ~ /^t/) m = 1024 ^ 4
        printf "%.0f", n * m
    }'
}

gib() { awk -v b="$1" 'BEGIN { printf "%.1f GiB", b / 1073741824 }'; }

sha="$(git -C "$repo" rev-parse --short=10 "$ref^{commit}")"
run_id="$(date +%Y%m%d-%H%M%S)-$sha"
out="${out:-$here/out/$run_id}"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
echo "testing $ref ($sha); results in $out"

# --- resources -------------------------------------------------------------------------------------------------
docker_cpus="$(docker info --format '{{.NCPU}}')"
docker_mem="$(docker info --format '{{.MemTotal}}')"
# Memory and CPUs in use: other containers count by what they use now; test containers of other runs of this script
# count by their limits, because a container that has just started uses little of what it will need.
measure_usage() {
    used_mem=0
    used_cpu_pct=0
    local ours
    ours=" $(docker ps -q --no-trunc --filter label=questdb-docker-test | tr '\n' ' ') "
    while read -r id usage cpu; do
        [[ -z "$id" ]] && continue
        if [[ "$ours" == *" $id"* ]]; then
            local limits
            limits="$(docker inspect --format '{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}' "$id")"
            used_mem=$(( used_mem + ${limits%% *} ))
            used_cpu_pct=$(( used_cpu_pct + ${limits##* } / 10000000 ))
        else
            used_mem=$(( used_mem + $(to_bytes "$usage") ))
            used_cpu_pct=$(( used_cpu_pct + ${cpu%%.*} ))
        fi
    done < <(docker stats --no-stream --no-trunc --format '{{.ID}} {{.MemUsage}} {{.CPUPerc}}' | awk '{gsub("%", "", $NF); print $1, $2, $NF}')
}
measure_usage
reserve_bytes="$(to_bytes "${reserve:-4g}")"
ten_percent=$(( docker_mem / 10 ))
if [[ -z "$reserve" && $ten_percent -gt $reserve_bytes ]]; then
    reserve_bytes=$ten_percent
fi
free_mem=$(( docker_mem - used_mem - reserve_bytes ))
free_cpus=$(( docker_cpus - (used_cpu_pct + 99) / 100 ))
job_mem="$(to_bytes "$mem_per_job")"
# Test data lives in each container's writable layer, on the Docker VM's disk; a full disk fails tests with
# SIGBUS and write errors that look like bugs. Free disk is checked once, before the images are built.
free_disk="$(docker run --rm alpine:3.20 df -k / 2>/dev/null | awk 'NR == 2 {printf "%.0f", $4 * 1024}')"
disk_job="$(to_bytes "$disk_per_job")"
by_disk=$(( (free_disk - 4 * 1024 * 1024 * 1024) / disk_job ))
if [[ -z "$parallel" ]]; then
    by_mem=$(( free_mem / job_mem ))
    by_cpu=$(( free_cpus / 2 ))
    parallel=$(( by_mem < by_cpu ? by_mem : by_cpu ))
    parallel=$(( parallel < by_disk ? parallel : by_disk ))
fi
while (( parallel < 1 )); do
    if (( ! wait_for_room )); then
        echo "not enough room in the Docker VM for one job: $(gib "$free_mem") memory free after a $(gib "$reserve_bytes") reserve (a job needs $(gib "$job_mem")), $free_cpus CPUs free, $(gib "$free_disk") disk free (a job needs $(gib "$disk_job") plus 4 GiB)" >&2
        echo "running containers use $(gib "$used_mem"); stop some, lower --mem-per-job, or pass --wait" >&2
        exit 1
    fi
    echo "waiting for room: $(gib "$free_mem") and $free_cpus CPUs free"
    sleep 30
    measure_usage
    free_mem=$(( docker_mem - used_mem - reserve_bytes ))
    free_cpus=$(( docker_cpus - (used_cpu_pct + 99) / 100 ))
    free_disk="$(docker run --rm alpine:3.20 df -k / 2>/dev/null | awk 'NR == 2 {printf "%.0f", $4 * 1024}')"
    by_disk=$(( (free_disk - 4 * 1024 * 1024 * 1024) / disk_job ))
    by_mem=$(( free_mem / job_mem ))
    by_cpu=$(( free_cpus / 2 ))
    parallel=$(( by_mem < by_cpu ? by_mem : by_cpu ))
    parallel=$(( parallel < by_disk ? parallel : by_disk ))
done
cpus_per_job="$(awk -v c="$free_cpus" -v p="$parallel" 'BEGIN { x = c / p; if (x > 4) x = 4; if (x < 1) x = 1; printf "%.1f", x }')"
echo "Docker VM: $docker_cpus CPUs, $(gib "$docker_mem"), $(gib "$free_disk") disk free; other containers use $(gib "$used_mem") and ${used_cpu_pct}% CPU"
echo "plan: $parallel containers at a time, each $(gib "$job_mem") (no swap) and $cpus_per_job CPUs; $(gib "$reserve_bytes") kept free"
{
    echo "ref=$ref sha=$sha parallel=$parallel mem_per_job=$mem_per_job cpus_per_job=$cpus_per_job heap_percent=$heap_percent"
    echo "docker_cpus=$docker_cpus docker_mem=$docker_mem used_mem=$used_mem reserve=$reserve_bytes"
} > "$out/run.conf"

# --- images ----------------------------------------------------------------------------------------------------
ctx="$(mktemp -d "${TMPDIR:-/tmp}/qdb-docker-test.XXXXXX")"
trap 'rm -rf "$ctx"' EXIT
git -C "$repo" archive --format=tar "$sha" | tar -x -C "$ctx"
# the runner's own files come from this checkout, so an older revision can be tested too
mkdir -p "$ctx/ci/docker-test"
cp "$here/Dockerfile" "$here/run-java.sh" "$here/run-rust.sh" "$ctx/ci/docker-test/"
platform_args=()
if [[ -n "$platform" ]]; then
    platform_args=(--platform "$platform")
fi
parquet_sha="$(git -C "$repo" ls-tree "$sha" core/rust/parquet2/testing/parquet-testing | awk '{print $3}')"

lock_dir="${XDG_CACHE_HOME:-$HOME/.cache}/questdb-docker-test/build.lock"
mkdir -p "$(dirname "$lock_dir")"
build() {
    local target="$1" tag="$2"
    # one build at a time: a build takes several GiB and every CPU it can get
    until mkdir "$lock_dir" 2> /dev/null; do
        if [[ -f "$lock_dir/pid" ]] && ! kill -0 "$(cat "$lock_dir/pid")" 2> /dev/null; then
            rm -rf "$lock_dir"
            continue
        fi
        echo "waiting for another image build to finish"
        sleep 20
    done
    echo $$ > "$lock_dir/pid"
    echo "building $tag"
    if ! docker build "${platform_args[@]}" -f "$ctx/ci/docker-test/Dockerfile" --target "$target" \
            --build-arg "PARQUET_TESTING_SHA=$parquet_sha" -t "$tag" "$ctx" > "$out/build-$target.log" 2>&1; then
        echo "image build failed, see $out/build-$target.log" >&2
        tail -30 "$out/build-$target.log" >&2
        rm -rf "$lock_dir"
        exit 1
    fi
    rm -rf "$lock_dir"
}

java_image="questdb-test-java:$sha"
rust_image="questdb-test-rust:$sha"
if (( run_java )); then build java-tests "$java_image"; fi
if (( run_rust )); then build rust-tests "$rust_image"; fi

# --- jobs ------------------------------------------------------------------------------------------------------
container_args=(
    --rm "${platform_args[@]}"
    --cpus "$cpus_per_job"
    --memory "$job_mem" --memory-swap "$job_mem"
    --ulimit nofile=1048576:1048576
    --label questdb-docker-test="$run_id"
)

running=0
launch() {
    local name="$1"; shift
    while (( running >= parallel )); do
        wait -n || true
        running=$(( running - 1 ))
    done
    mkdir -p "$out/$name"
    echo "  start $name"
    ( docker run "${container_args[@]}" --name "qdbtest-$run_id-$name" -v "$out/$name:/out" "$@" \
          > "$out/$name/docker.log" 2>&1 || true
      echo "  done  $name: exit $(cat "$out/$name/exit-code" 2>/dev/null || echo '?')" ) &
    running=$(( running + 1 ))
}

cleanup_containers() {
    docker ps -q --filter "label=questdb-docker-test=$run_id" | xargs -r docker kill > /dev/null 2>&1 || true
}
trap 'cleanup_containers; rm -rf "$ctx"' EXIT
# an interrupted run stops launching and kills its containers, instead of starting the next shard
trap 'echo "interrupted"; exit 130' INT TERM

# The tests' data goes to a RAM disk: on the Docker VM's disk, the fsync and syncfs calls of several containers at once
# leave the tests waiting on I/O most of the time. Upstream's ram-disk-test-root profile picks up QDB_TEST_TMPDIR and
# keeps the on-disk write paths the tests are meant to cover.
heap_percent_of_jvm="$heap_percent"
java_env=(-e "MAVEN_OPTS=-Xmx768m")
if [[ "$test_tmpfs" != "0" ]]; then
    tmpfs_bytes="$(to_bytes "$test_tmpfs")"
    # the heap is a share of what the RAM disk leaves
    heap_percent_of_jvm=$(( heap_percent * (job_mem - tmpfs_bytes) / job_mem ))
    container_args+=(--tmpfs "/home/qdb/qdbtmp:rw,exec,size=$tmpfs_bytes,uid=10001,gid=10001,mode=1777")
    java_env+=(-e "QDB_TEST_TMPDIR=/home/qdb/qdbtmp")
fi
java_env+=(-e "JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=$heap_percent_of_jvm")
cache_dir="${XDG_CACHE_HOME:-$HOME/.cache}/questdb-docker-test"
mkdir -p "$cache_dir"

if (( run_rust )); then
    launch rust -e "CARGO_BUILD_JOBS=${cpus_per_job%.*}" "$rust_image"
fi
if (( run_java )); then
    if [[ -n "$tests" ]]; then
        printf '' > "$out/selection.txt"
        launch java "${java_env[@]}" -v "$out/selection.txt:/home/qdb/selection.txt:ro" "$java_image" \
            /home/qdb/selection.txt "-Dtest=$tests"
    else
        shards="${shards:-$(( parallel * 2 ))}"
        python3 "$here/shards.py" plan --sources "$ctx" --timings "$here/timings.tsv" "$cache_dir/timings.tsv" \
            --shards "$shards" --out "$out/plan"
        while IFS=$'\t' read -r name _ _ _; do
            launch "$name" "${java_env[@]}" -v "$out/plan/$name.txt:/home/qdb/shard.txt:ro" "$java_image" /home/qdb/shard.txt
        done < "$out/plan/order"
    fi
fi
wait || true

if (( run_java )) && [[ -z "$tests" ]]; then
    python3 "$here/shards.py" timings --file "$cache_dir/timings.tsv" "$out" > /dev/null || true
fi

# --- end-to-end suites -----------------------------------------------------------------------------------------
e2e_code=0
if (( run_e2e )); then
    echo "building the server images for the Docker suites"
    if docker build "${platform_args[@]}" -f "$ctx/core/Dockerfile" --target questdb -t "questdb-e2e-base:$sha" "$ctx" > "$out/build-e2e.log" 2>&1 \
        && docker build --build-arg "BASE_IMAGE=questdb-e2e-base:$sha" -t "questdb-e2e:$sha" "$ctx/core/docker-tls" >> "$out/build-e2e.log" 2>&1; then
        for suite in test-tls.sh test-replication.sh; do
            if [[ -f "$ctx/core/docker-tls/$suite" ]]; then
                mkdir -p "$out/e2e"
                if IMAGE="questdb-e2e:$sha" bash "$ctx/core/docker-tls/$suite" > "$out/e2e/${suite%.sh}.log" 2>&1; then
                    echo "e2e $suite: passed"
                else
                    echo "e2e $suite: FAILED, see $out/e2e/${suite%.sh}.log"
                    e2e_code=1
                fi
            fi
        done
    else
        echo "e2e image build failed, see $out/build-e2e.log"
        e2e_code=1
    fi
fi

set +e
python3 "$here/shards.py" summarize "$out" | tee "$out/summary.txt"
code=${PIPESTATUS[0]}
if (( e2e_code != 0 )); then
    code=1
fi
exit "$code"
