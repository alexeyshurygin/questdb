#!/bin/bash
# Docker end-to-end suite for primary/replica replication through S3, on the TLS image (see
# core/docker-tls/Dockerfile for how to build it). The moto S3 emulator stands in for S3. Needs docker,
# curl and openssl on the host.
set -e

IMAGE="${IMAGE:-questdb-tls}"
# moto emulates the S3 calls replication relies on (conditional writes with If-Match and If-None-Match,
# ListObjectsV2 with start-after) and, after the IAM bootstrap below, verifies the SigV4 signature of
# every request. Its image ships python and boto3, which the suite uses to set it up and to inspect it.
S3_IMAGE="${S3_IMAGE:-motoserver/moto:latest}"
PREFIX="qdb-repl-test-$$"
NET="$PREFIX-net"
S3="$PREFIX-s3"
NODE_A="$PREFIX-a"
NODE_B="$PREFIX-b"
BUCKET="qdb"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# moto generates the access key at run time; the bootstrap below fills these in
S3_KEY_ID=""
S3_SECRET=""
STORE=""
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
    docker rm -f "$NODE_A" "$NODE_B" "$S3" >/dev/null 2>&1 || true
    docker volume rm "$NODE_A-vol" "$NODE_B-vol" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

pass() { echo "PASS"; PASS=$((PASS + 1)); }
# The failure text can carry container logs: keep the generated secret out of the output.
fail() { echo "FAIL: ${1//"$S3_SECRET"/<secret>}"; FAIL=$((FAIL + 1)); }

# q <port> <sql>: runs a statement over HTTPS and prints the JSON response
q() {
    curl -sk --max-time 20 -G "https://127.0.0.1:$1/exec" --data-urlencode "query=$2" 2>&1 || true
}

# count <port> <table>: prints the row count, or nothing
count() {
    q "$1" "SELECT count() FROM '$2'" | sed -n 's/.*"dataset":\[\[\([0-9]*\)\]\].*/\1/p'
}

# s3 <command> [argument]: runs boto3 inside the moto container. The suite does not publish moto's port
# on the host, so only that container and the QuestDB nodes on the suite's network reach it. boto3 reads
# the access key from the environment.
s3() {
    AWS_ACCESS_KEY_ID="$S3_KEY_ID" AWS_SECRET_ACCESS_KEY="$S3_SECRET" \
        docker exec -i -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY "$S3" python - "$BUCKET" "$@" <<'PY'
import json
import os
import sys

import boto3
from botocore.exceptions import ClientError

bucket, command = sys.argv[1], sys.argv[2]


def client(service, **credentials):
    return boto3.client(service, endpoint_url="http://127.0.0.1:5000", region_name="us-east-1", **credentials)


if command == "bootstrap":
    # moto lets its first three calls through without checking them (INITIAL_NO_AUTH_ACTION_COUNT=3):
    # exactly the ones that create a user, its access key and its policy. From the fourth call on it
    # checks signatures and policies, so the new key already has to sign the bucket creation.
    iam = client("iam", aws_access_key_id="bootstrap", aws_secret_access_key="bootstrap")
    iam.create_user(UserName="qdb")
    key = iam.create_access_key(UserName="qdb")["AccessKey"]
    policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
    iam.put_user_policy(UserName="qdb", PolicyName="s3", PolicyDocument=json.dumps(policy))
    key_id, secret = key["AccessKeyId"], key["SecretAccessKey"]
    client("s3", aws_access_key_id=key_id, aws_secret_access_key=secret).create_bucket(Bucket=bucket)
    print(key_id, secret)
elif command == "ls":  # <key prefix>: prints the keys of the objects under the prefix
    for page in client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=sys.argv[3]):
        for entry in page.get("Contents", []):
            print(entry["Key"])
elif command == "etag":  # <key>: prints the object's ETag, which changes with every write of the object
    print(client("s3").head_object(Bucket=bucket, Key=sys.argv[3])["ETag"])
elif command == "ls-with-wrong-secret":  # prints how moto answers the right key id with a wrong secret
    wrong = client("s3", aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"], aws_secret_access_key="wrong")
    try:
        wrong.list_objects_v2(Bucket=bucket)
        print("accepted")
    except ClientError as e:
        print(e.response["ResponseMetadata"]["HTTPStatusCode"], e.response["Error"]["Code"])
else:
    sys.exit("unknown command: " + command)
PY
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
docker run -d --name "$S3" --network "$NET" -e INITIAL_NO_AUTH_ACTION_COUNT=3 "$S3_IMAGE" >/dev/null
# Wait for moto with a plain TCP connect: an API call would use up one of its three unchecked calls.
for _ in $(seq 1 90); do
    if docker exec "$S3" python -c 'import socket; socket.create_connection(("127.0.0.1", 5000), 1)' 2>/dev/null; then
        break
    fi
    sleep 1
done
if ! CREDENTIALS=$(s3 bootstrap); then
    echo "could not create the access key and the bucket in the $S3_IMAGE container; refusing to run" >&2
    exit 2
fi
S3_KEY_ID="${CREDENTIALS%% *}"
S3_SECRET="${CREDENTIALS#* }"
STORE="s3::bucket=$BUCKET;root=e2e;region=us-east-1;endpoint=http://$S3:5000;access_key_id=$S3_KEY_ID;secret_access_key=$S3_SECRET;"

# Without this the suite would pass just the same against a moto that checks no signatures.
echo -n "Test 1: the S3 emulator refuses a request signed with a wrong secret... "
RESULT=$(s3 ls-with-wrong-secret 2>&1 || true)
if [ "$RESULT" = "403 SignatureDoesNotMatch" ]; then pass; else fail "$RESULT"; fi

start_node "$NODE_A" "$PORT_A" primary
verify_endpoint "$PORT_A"

echo -n "Test 2: primary takes the lease and accepts writes... "
if await_writable "$PORT_A"; then pass; else fail "$(docker logs --tail 30 "$NODE_A" 2>&1)"; fi

echo -n "Test 3: SQL and ILP over HTTPS ingest on the primary... "
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

echo -n "Test 4: replica converges with the primary... "
if await_count "$PORT_B" trades 1100 60; then pass; else fail "replica count=$(count "$PORT_B" trades)"; fi

echo -n "Test 5: replication lag under 5 seconds... "
START=$(date +%s)
q "$PORT_A" "INSERT INTO trades VALUES ('2024-03-01', 'LAG', 1.0, 'lag probe')" >/dev/null
if await_count "$PORT_B" trades 1101 30; then
    LAG=$(($(date +%s) - START))
    if [ "$LAG" -le 5 ]; then pass; else fail "lag ${LAG}s"; fi
else
    fail "row did not arrive"
fi

echo -n "Test 6: replica refuses writes... "
RESULT=$(q "$PORT_B" "INSERT INTO trades VALUES ('2024-03-02', 'X', 1.0, 'x')")
if echo "$RESULT" | grep -q "replica access is read-only"; then pass; else fail "$RESULT"; fi

echo -n "Test 7: replicas serve identical data... "
A_DATA=$(q "$PORT_A" "SELECT * FROM trades ORDER BY ts, sym" | sed 's/"timings".*//')
B_DATA=$(q "$PORT_B" "SELECT * FROM trades ORDER BY ts, sym" | sed 's/"timings".*//')
if [ -n "$A_DATA" ] && [ "$A_DATA" = "$B_DATA" ]; then pass; else fail "datasets differ"; fi

echo -n "Test 8: the store holds the index and table objects... "
OBJECTS=$(s3 ls e2e/ 2>&1 || true)
if echo "$OBJECTS" | grep -q "_index" && echo "$OBJECTS" | grep -q "/seq/" && echo "$OBJECTS" | grep -q "/wal/"; then pass; else fail "$OBJECTS"; fi

echo -n "Test 9: a replica restarted as primary without _migrate_primary stays read-only... "
docker stop -t 60 "$NODE_A" >/dev/null
docker rm "$NODE_A" >/dev/null
docker rm -f "$NODE_B" >/dev/null
INDEX_BEFORE=$(s3 etag e2e/_index 2>&1 || true)
start_node "$NODE_B" "$PORT_B" primary
verify_endpoint "$PORT_B"
REFUSED=""
for _ in $(seq 1 60); do
    if docker logs "$NODE_B" 2>&1 | grep -q "create an empty _migrate_primary file in /var/lib/questdb"; then
        REFUSED=yes
        break
    fi
    sleep 1
done
if [ -n "$REFUSED" ] \
    && q "$PORT_B" "INSERT INTO trades VALUES ('2024-03-03', 'NO', 1.0, 'refused')" | grep -q "read-only" \
    && [ -n "$INDEX_BEFORE" ] && [ "$(s3 etag e2e/_index 2>&1 || true)" = "$INDEX_BEFORE" ]; then
    pass
else
    fail "$(docker logs --tail 30 "$NODE_B" 2>&1)"
fi

echo -n "Test 10: promotion: with _migrate_primary the replica becomes the primary... "
docker rm -f "$NODE_B" >/dev/null
docker run --rm -v "$NODE_B-vol:/var/lib/questdb" --entrypoint /bin/sh "$IMAGE" \
    -c 'touch /var/lib/questdb/_migrate_primary && chmod 666 /var/lib/questdb/_migrate_primary' >/dev/null
start_node "$NODE_B" "$PORT_B" primary
verify_endpoint "$PORT_B"
if await_writable "$PORT_B" && q "$PORT_B" "INSERT INTO trades VALUES ('2024-03-03', 'NEW', 2.0, 'after promotion')" | grep -q '"dml":"OK"' \
    && await_count "$PORT_B" trades 1102 30 \
    && ! docker exec "$NODE_B" test -e /var/lib/questdb/_migrate_primary; then
    pass
else
    fail "$(docker logs --tail 30 "$NODE_B" 2>&1)"
fi

echo -n "Test 11: the former primary rejoins as a replica... "
start_node "$NODE_A" "$PORT_A" replica
verify_endpoint "$PORT_A"
if await_count "$PORT_A" trades 1102 60; then pass; else fail "count=$(count "$PORT_A" trades)"; fi

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
