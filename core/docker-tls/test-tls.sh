#!/bin/bash
set -e

CONTAINER_NAME="questdb-tls-test"
HTTPS_PORT=19000
PG_PORT=18812
PASS=0
FAIL=0

cleanup() {
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== QuestDB Docker TLS Test ==="

# Start container
echo "Starting QuestDB with TLS..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d --name "$CONTAINER_NAME" \
    -p "$HTTPS_PORT":9000 \
    -p "$PG_PORT":8812 \
    questdb-tls >/dev/null
sleep 6

# Test 1: HTTPS query works
echo -n "Test 1: HTTPS query... "
RESULT=$(curl -sk --max-time 10 "https://localhost:$HTTPS_PORT/exec?query=SELECT+42+as+val" 2>&1)
if echo "$RESULT" | grep -q '"dataset":\[\[42\]\]'; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 2: Plaintext HTTP fails
echo -n "Test 2: Plaintext HTTP rejected... "
RESULT=$(curl -s --max-time 5 "http://localhost:$HTTPS_PORT/" 2>&1 || true)
if [ -z "$RESULT" ] || echo "$RESULT" | grep -qi "error\|empty\|reset"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: plaintext should be rejected"
    FAIL=$((FAIL + 1))
fi

# Test 3: TLS certificate is self-signed localhost
echo -n "Test 3: TLS certificate CN=localhost... "
CERT_CN=$(echo | openssl s_client -connect "localhost:$HTTPS_PORT" 2>/dev/null | openssl x509 -noout -subject 2>/dev/null)
if echo "$CERT_CN" | grep -q "CN.*=.*localhost"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $CERT_CN"
    FAIL=$((FAIL + 1))
fi

# Test 4: PGWire TLS query works
echo -n "Test 4: PGWire TLS query... "
RESULT=$(PGPASSWORD=quest psql "host=localhost port=$PG_PORT dbname=qdb user=admin sslmode=require" -t -c "SELECT 42 as val" 2>&1)
if echo "$RESULT" | grep -q "42"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 5: PGWire TLS connection uses SSL
echo -n "Test 5: PGWire SSL connection verified... "
RESULT=$(PGPASSWORD=quest psql "host=localhost port=$PG_PORT dbname=qdb user=admin sslmode=require" -c "\\conninfo" 2>&1)
if echo "$RESULT" | grep -qi "ssl"; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

# Test 6: HTTPS CREATE TABLE + INSERT + SELECT
echo -n "Test 6: HTTPS DDL+DML+query... "
curl -sk --max-time 10 "https://localhost:$HTTPS_PORT/exec?query=CREATE+TABLE+IF+NOT+EXISTS+tls_test(x+INT,+ts+TIMESTAMP)+TIMESTAMP(ts)+PARTITION+BY+DAY" >/dev/null
curl -sk --max-time 10 "https://localhost:$HTTPS_PORT/exec?query=INSERT+INTO+tls_test+VALUES(1,+0),+(2,+1000000)" >/dev/null
RESULT=$(curl -sk --max-time 10 "https://localhost:$HTTPS_PORT/exec?query=SELECT+sum(x)+as+s+FROM+tls_test" 2>&1)
if echo "$RESULT" | grep -q '"dataset":\[\[3\]\]'; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL: $RESULT"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "Results: $PASS passed, $FAIL failed"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
