#!/bin/bash
# Docker end-to-end suite for primary/replica replication through S3 (MinIO), on the TLS image (see
# core/docker-tls/Dockerfile for how to build it). Needs docker, curl and openssl on the host.
set -e

IMAGE="${IMAGE:-questdb-tls}"
# MinIO no longer publishes community images to Docker Hub; the Bitnami legacy build bundles mc and
# creates the bucket from MINIO_DEFAULT_BUCKETS.
MINIO_IMAGE="${MINIO_IMAGE:-bitnamilegacy/minio:latest}"
PREFIX="qdb-repl-test-$$"
NET="$PREFIX-net"
MINIO="$PREFIX-minio"
NODE_A="$PREFIX-a"
NODE_B="$PREFIX-b"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STORE="s3::bucket=qdb;root=e2e;region=us-east-1;endpoint=http://$MINIO:9000;access_key_id=qdbtest;secret_access_key=qdbtestsecret;"
PASS=0
FAIL=0

# Random high ports on the loopback interface only: a fixed well-known port can belong to something
# else listening on the host (an SSH or SSM tunnel to a real server, for one).
pick_free_port() {
    while :; do
        local port=$((20000 + RANDOM % 20000))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "$port"
            return
        fi
    done
}
PORT_A="$(pick_free_port)"
PORT_B="$(pick_free_port)"
while [ "$PORT_B" = "$PORT_A" ]; do PORT_B="$(pick_free_port)"; done

cleanup() {
    docker rm -f "$NODE_A" "$NODE_B" "$MINIO" >/dev/null 2>&1 || true
    docker volume rm "$NODE_A-vol" "$NODE_B-vol" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

pass() { echo "PASS"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

# q <port> <sql>: runs a statement over HTTPS and prints the JSON response
q() {
    curl -sk --max-time 20 -G "https://127.0.0.1:$1/exec" --data-urlencode "query=$2" 2>&1 || true
}

# count <port> <table>: prints the row count, or nothing
count() {
    q "$1" "SELECT count() FROM '$2'" | sed -n 's/.*"dataset":\[\[\([0-9]*\)\]\].*/\1/p'
}

start_node() { # <name> <port> <role>
    docker run -d --name "$1" --network "$NET" \
        -p "127.0.0.1:$2:9000" \
        -v "$1-vol:/var/lib/questdb" \
        -e QDB_REPLICATION_ROLE="$3" \
        -e QDB_REPLICATION_OBJECT_STORE="$STORE" \
        -e QDB_REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION=1000 \
        -e QDB_REPLICATION_PRIMARY_KEEPALIVE_INTERVAL=2000 \
        -e QDB_REPLICATION_REPLICA_POLL_INTERVAL=500 \
        "$IMAGE" >/dev/null
}

# Before any query, make sure the port is our container: it must present the test certificate.
verify_endpoint() { # <port>
    local expected actual
    expected=$(openssl x509 -in "$SCRIPT_DIR/server.crt" -noout -fingerprint -sha256)
    for _ in $(seq 1 90); do
        actual=$(echo | openssl s_client -connect "127.0.0.1:$1" 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null || true)
        if [ -n "$actual" ]; then
            break
        fi
        sleep 1
    done
    if [ "$actual" != "$expected" ]; then
        echo "the server on 127.0.0.1:$1 does not present the test certificate; refusing to run" >&2
        exit 2
    fi
    for _ in $(seq 1 60); do
        if q "$1" "SELECT 1" | grep -q dataset; then
            return
        fi
        sleep 1
    done
}

await_writable() { # <port>: a primary accepts writes once it caught up and holds the lease
    for _ in $(seq 1 120); do
        if q "$1" "CREATE TABLE IF NOT EXISTS probe (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL" | grep -q '"ddl":"OK"'; then
            return 0
        fi
        sleep 1
    done
    return 1
}

await_count() { # <port> <table> <expected count> <timeout seconds>
    for _ in $(seq 1 $(($4 * 4))); do
        if [ "$(count "$1" "$2")" = "$3" ]; then
            return 0
        fi
        sleep 0.25
    done
    return 1
}

echo "=== QuestDB Docker replication test ==="
docker network create "$NET" >/dev/null
docker run -d --name "$MINIO" --network "$NET" -e MINIO_ROOT_USER=qdbtest -e MINIO_ROOT_PASSWORD=qdbtestsecret \
    -e MINIO_DEFAULT_BUCKETS=qdb "$MINIO_IMAGE" >/dev/null
mc() { # runs the MinIO client inside the MinIO container
    docker exec "$MINIO" sh -c "mc alias set m http://127.0.0.1:9000 qdbtest qdbtestsecret >/dev/null 2>&1 && mc $*"
}
for _ in $(seq 1 90); do
    if mc ls m/qdb >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

start_node "$NODE_A" "$PORT_A" primary
verify_endpoint "$PORT_A"

echo -n "Test 1: primary takes the lease and accepts writes... "
if await_writable "$PORT_A"; then pass; else fail "$(docker logs --tail 30 "$NODE_A" 2>&1)"; fi

echo -n "Test 2: SQL and ILP over HTTPS ingest on the primary... "
q "$PORT_A" "CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, note VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL" >/dev/null
q "$PORT_A" "INSERT INTO trades SELECT timestamp_sequence('2024-01-01', 60_000_000), rnd_symbol('A','B','C'), rnd_double(), rnd_varchar(1, 30, 1) FROM long_sequence(1000)" >/dev/null
ILP=""
for i in $(seq 1 100); do
    ILP="${ILP}trades,sym=ILP price=$i.5,note=\"line $i\" $((1706745600000000000 + i * 1000000000))"$'\n'
done
curl -sk --max-time 20 --data-binary "$ILP" "https://127.0.0.1:$PORT_A/write" >/dev/null || true
if await_count "$PORT_A" trades 1100 30; then pass; else fail "count=$(count "$PORT_A" trades)"; fi

start_node "$NODE_B" "$PORT_B" replica
verify_endpoint "$PORT_B"

echo -n "Test 3: replica converges with the primary... "
if await_count "$PORT_B" trades 1100 60; then pass; else fail "replica count=$(count "$PORT_B" trades)"; fi

echo -n "Test 4: replication lag under 5 seconds... "
START=$(date +%s)
q "$PORT_A" "INSERT INTO trades VALUES ('2024-03-01', 'LAG', 1.0, 'lag probe')" >/dev/null
if await_count "$PORT_B" trades 1101 30; then
    LAG=$(($(date +%s) - START))
    if [ "$LAG" -le 5 ]; then pass; else fail "lag ${LAG}s"; fi
else
    fail "row did not arrive"
fi

echo -n "Test 5: replica refuses writes... "
RESULT=$(q "$PORT_B" "INSERT INTO trades VALUES ('2024-03-02', 'X', 1.0, 'x')")
if echo "$RESULT" | grep -q "replica access is read-only"; then pass; else fail "$RESULT"; fi

echo -n "Test 6: replicas serve identical data... "
A_DATA=$(q "$PORT_A" "SELECT * FROM trades ORDER BY ts, sym" | sed 's/"timings".*//')
B_DATA=$(q "$PORT_B" "SELECT * FROM trades ORDER BY ts, sym" | sed 's/"timings".*//')
if [ -n "$A_DATA" ] && [ "$A_DATA" = "$B_DATA" ]; then pass; else fail "datasets differ"; fi

echo -n "Test 7: the store holds the index and table objects... "
OBJECTS=$(mc ls --recursive m/qdb/e2e/ 2>&1 || true)
if echo "$OBJECTS" | grep -q "_index" && echo "$OBJECTS" | grep -q "/seq/" && echo "$OBJECTS" | grep -q "/wal/"; then pass; else fail "$OBJECTS"; fi

echo -n "Test 8: promotion: stop the primary, restart the replica as primary... "
docker stop -t 60 "$NODE_A" >/dev/null
docker rm "$NODE_A" >/dev/null
docker rm -f "$NODE_B" >/dev/null
start_node "$NODE_B" "$PORT_B" primary
verify_endpoint "$PORT_B"
if await_writable "$PORT_B" && q "$PORT_B" "INSERT INTO trades VALUES ('2024-03-03', 'NEW', 2.0, 'after promotion')" | grep -q '"dml":"OK"' \
    && await_count "$PORT_B" trades 1102 30; then
    pass
else
    fail "$(docker logs --tail 30 "$NODE_B" 2>&1)"
fi

echo -n "Test 9: the former primary rejoins as a replica... "
start_node "$NODE_A" "$PORT_A" replica
verify_endpoint "$PORT_A"
if await_count "$PORT_A" trades 1102 60; then pass; else fail "count=$(count "$PORT_A" trades)"; fi

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
