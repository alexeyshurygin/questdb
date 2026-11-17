#!/usr/bin/env bash
#
# Runs one shard of Java test classes inside a java-tests container and leaves its results in /out:
# the surefire reports, the Maven output, the exit code, the run time and the container's peak memory.
#
# Usage: run-java.sh <shard file>
#   The first line of the shard file is "#include" or "#exclude".
#   #include: one fully qualified test class per line; the shard runs exactly these.
#   #exclude: one class path per line (io/questdb/Foo.java); the shard runs every test class except these.
#   Anything else on the command line after the shard file is passed to Maven as is (e.g. -Dtest=FooTest for a
#   single run of chosen tests, with an empty shard file).

set -u

shard_file="$1"
shift
out=/out
mkdir -p "$out"

mode="$(head -1 "$shard_file" 2>/dev/null)"
selection=()
case "$mode" in
    "#include")
        classes="$(tail -n +2 "$shard_file" | grep -v '^[[:space:]]*$' | paste -sd, -)"
        if [[ -z "$classes" ]]; then
            echo 0 > "$out/exit-code"
            exit 0
        fi
        selection=("-Dtest=$classes")
        ;;
    "#exclude")
        tail -n +2 "$shard_file" > /tmp/excludes.txt
        selection=("-Dsurefire.excludesFile=/tmp/excludes.txt")
        ;;
esac

start=$(date +%s)
mvn -B -o -q \
    -pl core,utils,compat \
    org.apache.maven.plugins:maven-surefire-plugin:test \
    "${selection[@]}" \
    -DfailIfNoTests=false \
    -Dsurefire.failIfNoSpecifiedTests=false \
    "-Dout=/home/qdb/src/ci/qlog.conf" \
    "$@" \
    > "$out/maven.log" 2>&1
code=$?
end=$(date +%s)

for module in core utils compat; do
    if [[ -d "$module/target/surefire-reports" ]]; then
        mkdir -p "$out/reports/$module"
        cp -r "$module/target/surefire-reports/." "$out/reports/$module/"
    fi
done
# an hs_err file tells why a JVM crashed
find . -name 'hs_err_pid*.log' -exec cp {} "$out/" \; 2>/dev/null

echo "$((end - start))" > "$out/seconds"
cat /sys/fs/cgroup/memory.peak > "$out/memory-peak" 2>/dev/null || true
df -k / | awk 'NR == 2 {print $4 * 1024}' > "$out/disk-free-at-end"
echo "$code" > "$out/exit-code"
exit "$code"
