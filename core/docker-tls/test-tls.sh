#!/bin/bash
# Docker end-to-end suite for the TLS image (see core/docker-tls/Dockerfile for how to build it).
# Needs docker, curl, openssl and psql on the host, and the host ports below free.
set -e

IMAGE="${IMAGE:-questdb-tls}"
CONTAINER_NAME="questdb-tls-test"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASS=0
FAIL=0

# Random high ports on the loopback interface only. Fixed well-known ports can collide with something
# else listening on the host (an SSH or SSM tunnel to a real server, for one), and the tests would then
# run against that server.
pick_free_port() {
    while :; do
        local port=$((20000 + RANDOM % 20000))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "$port"
            return
        fi
    done
}
HTTPS_PORT="${HTTPS_PORT:-$(pick_free_port)}"
PG_PORT="${PG_PORT:-$(pick_free_port)}"
for port in "$HTTPS_PORT" "$PG_PORT"; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        echo "port $port is already in use on 127.0.0.1; refusing to run" >&2
        exit 2
    fi
done

cleanup() {
    docker rm -f "$CONTAINER_NAME" "$CONTAINER_NAME-bad" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== QuestDB Docker TLS Test ==="

# Start container
echo "Starting QuestDB with TLS..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d --name "$CONTAINER_NAME" \
    -p "127.0.0.1:$HTTPS_PORT:9000" \
    -p "127.0.0.1:$PG_PORT:8812" \
    "$IMAGE" >/dev/null

# Before any query, make sure the TLS endpoint is our container: it must present the test certificate.
EXPECTED_FINGERPRINT=$(openssl x509 -in "$SCRIPT_DIR/server.crt" -noout -fingerprint -sha256)
for _ in $(seq 1 60); do
    ACTUAL_FINGERPRINT=$(echo | openssl s_client -connect "127.0.0.1:$HTTPS_PORT" 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null || true)
    if [ -n "$ACTUAL_FINGERPRINT" ]; then
        break
    fi
    sleep 1
done
if [ "$ACTUAL_FINGERPRINT" != "$EXPECTED_FINGERPRINT" ]; then
    echo "the server on 127.0.0.1:$HTTPS_PORT does not present the test certificate; refusing to run" >&2
    echo "expected: $EXPECTED_FINGERPRINT, got: ${ACTUAL_FINGERPRINT:-nothing}" >&2
    exit 2
fi
for _ in $(seq 1 60); do
    if curl -sk --max-time 2 "https://127.0.0.1:$HTTPS_PORT/exec?query=SELECT+1" | grep -q dataset; then
        break
    fi
    sleep 1
done

# Test 1: HTTPS query works
echo -n "Test 1: HTTPS query... "
RESULT=$(curl -sk --max-time 10 "https://127.0.0.1:$HTTPS_PORT/exec?query=SELECT+42+as+val" 2>&1 || true)
if echo "$RESULT" | grep -q '"dataset":\[\[42\]\]'; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 2: Plaintext HTTP fails
echo -n "Test 2: Plaintext HTTP rejected... "
RESULT=$(curl -s --max-time 5 "http://127.0.0.1:$HTTPS_PORT/" 2>&1 || true)
if [ -z "$RESULT" ] || echo "$RESULT" | grep -qi "error\|empty\|reset"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: plaintext should be rejected"
    FAIL=$((FAIL + 1))
fi

# Test 3: TLS certificate is self-signed localhost
echo -n "Test 3: TLS certificate CN=localhost... "
CERT_CN=$(echo | openssl s_client -connect "127.0.0.1:$HTTPS_PORT" 2>/dev/null | openssl x509 -noout -subject 2>/dev/null || true)
if echo "$CERT_CN" | grep -q "CN.*=.*localhost"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $CERT_CN"
    FAIL=$((FAIL + 1))
fi

# Test 4: PGWire TLS query works
echo -n "Test 4: PGWire TLS query... "
RESULT=$(PGPASSWORD=quest psql "host=127.0.0.1 port=$PG_PORT dbname=qdb user=admin sslmode=require" -t -c "SELECT 42 as val" 2>&1 || true)
if echo "$RESULT" | grep -q "42"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 5: PGWire TLS connection uses SSL
echo -n "Test 5: PGWire SSL connection verified... "
RESULT=$(PGPASSWORD=quest psql "host=127.0.0.1 port=$PG_PORT dbname=qdb user=admin sslmode=require" -c "\\conninfo" 2>&1 || true)
if echo "$RESULT" | grep -qi "ssl"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 6: HTTPS CREATE TABLE + INSERT + SELECT
echo -n "Test 6: HTTPS DDL+DML+query... "
curl -sk --max-time 10 "https://127.0.0.1:$HTTPS_PORT/exec?query=CREATE+TABLE+IF+NOT+EXISTS+tls_test(x+INT,+ts+TIMESTAMP)+TIMESTAMP(ts)+PARTITION+BY+DAY" >/dev/null || true
curl -sk --max-time 10 "https://127.0.0.1:$HTTPS_PORT/exec?query=INSERT+INTO+tls_test+VALUES(1,+0),+(2,+1000000)" >/dev/null || true
RESULT=$(curl -sk --max-time 10 "https://127.0.0.1:$HTTPS_PORT/exec?query=SELECT+sum(x)+as+s+FROM+tls_test" 2>&1 || true)
if echo "$RESULT" | grep -q '"dataset":\[\[3\]\]'; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 7: QWP WebSocket upgrade works over TLS (wss)
echo -n "Test 7: QWP ingress WebSocket upgrade over TLS... "
RESULT=$(curl -sk --http1.1 -i -N --max-time 5 \
    -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==" \
    "https://127.0.0.1:$HTTPS_PORT/write/v4" 2>&1 || true)
if echo "$RESULT" | grep -q "101 Switching Protocols" && echo "$RESULT" | grep -q "X-QWP-Version"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 8: QWP egress WebSocket upgrade works over TLS (wss)
echo -n "Test 8: QWP egress WebSocket upgrade over TLS... "
RESULT=$(curl -sk --http1.1 -i -N --max-time 5 \
    -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==" \
    "https://127.0.0.1:$HTTPS_PORT/read/v1" 2>&1 | LC_ALL=C tr -d '\0' || true)
if echo "$RESULT" | grep -q "101 Switching Protocols"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 9: Plaintext QWP WebSocket upgrade is rejected
echo -n "Test 9: Plaintext QWP upgrade rejected... "
RESULT=$(curl -s --http1.1 -i --max-time 5 \
    -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==" \
    "http://127.0.0.1:$HTTPS_PORT/write/v4" 2>&1 || true)
if echo "$RESULT" | grep -q "101 Switching Protocols"; then
    echo "FAIL: plaintext QWP upgrade should be rejected"
    FAIL=$((FAIL + 1))
else
    echo "PASS"
    PASS=$((PASS + 1))
fi

# Test 10: a browser on the server's own https origin may upgrade to QWP over native TLS
echo -n "Test 10: QWP upgrade with same-origin https Origin... "
RESULT=$(curl -sk --http1.1 -i -N --max-time 5 \
    -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==" \
    -H "Origin: https://127.0.0.1:$HTTPS_PORT" \
    "https://127.0.0.1:$HTTPS_PORT/write/v4" 2>&1 || true)
if echo "$RESULT" | grep -q "101 Switching Protocols"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 11: an http origin does not match a TLS connection, so the browser upgrade is refused
echo -n "Test 11: QWP upgrade with http Origin over TLS refused... "
RESULT=$(curl -sk --http1.1 -i -N --max-time 5 \
    -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==" \
    -H "Origin: http://127.0.0.1:$HTTPS_PORT" \
    "https://127.0.0.1:$HTTPS_PORT/write/v4" 2>&1 || true)
if echo "$RESULT" | grep -q "101 Switching Protocols"; then
    echo "FAIL: cross-scheme origin should be refused"
    FAIL=$((FAIL + 1))
else
    echo "PASS"
    PASS=$((PASS + 1))
fi

# Test 12: plaintext PGWire is refused on the TLS listener
echo -n "Test 12: PGWire without TLS refused... "
RESULT=$(PGPASSWORD=quest psql "host=127.0.0.1 port=$PG_PORT dbname=qdb user=admin sslmode=disable" -t -c "SELECT 1" 2>&1 || true)
if echo "$RESULT" | grep -q "request SSL message expected"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 13: a broken TLS configuration stops TlsServerMain with exit code 55 and names the problem
echo -n "Test 13: missing certificate fails startup with exit code 55... "
docker run -d --name "$CONTAINER_NAME-bad" -e QDB_TLS_CERT_PATH=/etc/questdb/tls/missing.crt "$IMAGE" >/dev/null
EXIT_CODE=$(docker wait "$CONTAINER_NAME-bad")
LOGS=$(docker logs "$CONTAINER_NAME-bad" 2>&1)
if [ "$EXIT_CODE" = "55" ] && echo "$LOGS" | grep -q "TLS file does not exist \[key=tls.cert.path, path=/etc/questdb/tls/missing.crt\]"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: exit=$EXIT_CODE logs: $(echo "$LOGS" | tail -5)"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
