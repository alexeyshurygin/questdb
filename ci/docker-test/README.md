# Local test runs on Linux in Docker

`run-tests.sh` runs the test suites on Linux, in parallel Docker containers sized to fit the CPUs and memory that
Docker has free. Use it before pushing: production runs on Linux, and a run on a developer's Mac exercises other
kernel, filesystem and JDK code paths.

```bash
ci/docker-test/run-tests.sh                        # Java and Rust suites at HEAD
ci/docker-test/run-tests.sh --ref ssl_10.2.0       # any committed revision
ci/docker-test/run-tests.sh --e2e                  # also the TLS and replication Docker suites
ci/docker-test/run-tests.sh --no-rust --tests 'TlsServerTest,ReplicationDataTest#testRollover*'
```

Needs bash 4.3 or later, Docker with BuildKit, git and python3.

## What runs

| Job | Content |
|---|---|
| Java shards | every test class of `core`, `utils` and `compat`, as the CI workflow runs them (`ci/qlog.conf` logging) |
| `rust` | clippy and rustfmt, and the tests of `qdb-core`, `qdb-parquet-meta`, `qdbr` and `parquet2` |
| `--e2e` | the server image from `core/Dockerfile`, then `core/docker-tls/test-tls.sh` and `test-replication.sh` |

The images are built from `git archive <ref>`: uncommitted changes and untracked files never reach them. The runner's
own files come from the checkout that runs it, so older revisions can be tested too. Builds keep Maven's repository
and cargo's caches between runs, so a rebuild after a change compiles the code again but downloads nothing.

The test JVMs run as an unprivileged user (root would ignore file permissions that some tests check), with an open
file limit of 1,048,576.

## Resources

Before it starts, the runner reads Docker's CPUs and memory and what the running containers use, and keeps a reserve
free (4 GiB or 10% of Docker's memory, whichever is more; `--reserve`). It then runs as many containers at a time as
both the free memory and the free CPUs allow:

- each container has a hard memory limit (`--mem-per-job`, default 6g) and no swap, so a job that needs more is
  killed and reported instead of pushing the machine into swap;
- the test JVM's heap is a share of that limit (`--heap-percent`, default 40), leaving the rest for QuestDB's native
  memory, the JIT and Maven;
- free CPUs are divided between the containers, at most 4 each.

`--parallel` and `--mem-per-job` override the plan. On Docker Desktop or OrbStack, Docker's memory is the VM's
setting, not the machine's.

## Shards

`shards.py plan` splits the Java test classes into shards of about equal run time, using the run times of earlier
runs (`~/.cache/questdb-docker-test/timings.tsv`, seeded from `timings.tsv`). One shard runs every class the others do
not list, so a new test class, or one no timing file knows, still runs exactly once. There are two shards per
parallel slot by default; a slot that finishes early takes the next shard.

## Results

`ci/docker-test/out/<timestamp>-<sha>/` (or `--out`) holds:

- `summary.txt`: totals and every failing test, the same text the run prints at the end;
- one directory per job with its surefire reports, Maven or cargo output, exit code, run time and peak memory;
- `plan/`: the shard lists; `build-*.log`: the image builds.

The exit code is 0 only when every job finished and no test failed.
