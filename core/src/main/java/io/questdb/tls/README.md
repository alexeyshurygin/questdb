# Server-Side TLS Support

## Overview

QuestDB supports server-side TLS for all network protocols: HTTP (full and min),
PostgreSQL wire protocol, and ILP/TCP. TLS is implemented as an overlay module
that wraps plain sockets with Java's SSLEngine, requiring no changes to the
core protocol implementations.

## Architecture

TLS support follows the **overlay pattern**:

1. **`TlsServerMain`** -- the entry point for TLS-enabled QuestDB. It replaces
   the default `ServerMain` and injects a `TlsFactoryProviderFactory` into the
   configuration pipeline.

2. **`TlsFactoryProviderFactory`** -- implements `FactoryProviderFactory` and
   creates `TlsFactoryProvider` instances that return `JavaTlsServerSocketFactory`
   for TLS-enabled protocols and `PlainSocketFactory` for disabled ones.

3. **`TlsFactoryProvider`** -- delegates all non-socket factory methods to the
   default `FactoryProviderImpl` and overrides socket factories per protocol.

4. **`JavaTlsServerSocketFactory` / `JavaTlsServerSocket`** -- wrap plain
   sockets with `SSLEngine` for TLS handshake and encryption.

5. **`PemReader`** -- loads PEM-encoded X.509 certificates and PKCS8 private
   keys to create an `SSLContext`.

The startup guard in `PropServerConfiguration` ensures that if any TLS property
is enabled, the server must have been started via `TlsServerMain`. Starting with
the default `ServerMain` while TLS is enabled produces an actionable error
message.

## Configuration

### Global properties (apply to all protocols unless overridden)

| Property                 | Description              | Default  |
|--------------------------|--------------------------|----------|
| `tls.enabled`            | Enable TLS globally      | `false`  |
| `tls.cert.path`          | Path to certificate PEM  | (none)   |
| `tls.private.key.path`   | Path to private key PEM  | (none)   |

### Per-protocol overrides

Each protocol can override the global settings:

- `http.tls.enabled`, `http.tls.cert.path`, `http.tls.private.key.path`
- `http.min.tls.enabled`, `http.min.tls.cert.path`, `http.min.tls.private.key.path`
- `line.tcp.tls.enabled`, `line.tcp.tls.cert.path`, `line.tcp.tls.private.key.path`
- `pg.tls.enabled`, `pg.tls.cert.path`, `pg.tls.private.key.path`

When a per-protocol property is not set, the global value is used as fallback.
Setting `pg.tls.enabled=false` while `tls.enabled=true` disables TLS for
PostgreSQL wire protocol only.

## PEM File Requirements

- **Certificate**: X.509 format, PEM-encoded. Certificate chains are supported
  (concatenate certificates in order: server cert first, then intermediates).
- **Private key**: Must be in **PKCS8** format (header: `-----BEGIN PRIVATE KEY-----`).
  Both RSA and EC keys are supported.

PKCS1 keys (`-----BEGIN RSA PRIVATE KEY-----`) are **not** supported. Convert
with:

```bash
openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem
```

## Generating Test Certificates

```bash
# Generate a self-signed certificate and PKCS8 private key
openssl req -x509 -newkey rsa:2048 \
  -keyout server-raw.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"

# Convert private key to PKCS8 format
openssl pkcs8 -topk8 -nocrypt -in server-raw.key -out server.key
```

## TLS is Restart-Required

TLS properties are **not** dynamic. Changing TLS configuration requires a full
server restart. The `DynamicPropServerConfiguration` does not include any TLS
`PropertyKey` entries in its dynamic properties set, and the `FactoryProvider`
(which holds the `SSLContext`) is reused across configuration reloads.

## Running

```bash
# Build QuestDB
mvn clean package -DskipTests -P build-web-console

# Create the data directory
mkdir /path/to/db

# Start with TLS support
java -p core/target/questdb-<version>-SNAPSHOT.jar \
  -m io.questdb/io.questdb.tls.TlsServerMain \
  -d /path/to/db
```

Configure TLS in `conf/server.conf`:

```properties
tls.enabled=true
tls.cert.path=/path/to/server.crt
tls.private.key.path=/path/to/server.key
```

## Docker

### Generate certificate

```bash
cd core/docker-tls
openssl req -x509 -newkey rsa:2048 \
  -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

The `-nodes` flag produces a PKCS8-format key directly — no conversion needed.

### Build the Docker image

```bash
# Build QuestDB JAR first
mvn clean package -DskipTests -pl core

# Copy JAR to Docker build context
cp core/target/questdb-*-SNAPSHOT.jar core/docker-tls/questdb.jar

# Build image
cd core/docker-tls
docker build -t questdb-tls .
```

### Run the container

```bash
docker run -d --name questdb-tls \
  -p 9000:9000 \
  -p 8812:8812 \
  -p 9009:9009 \
  questdb-tls
```

### Verify TLS works

```bash
# HTTPS
curl -sk 'https://localhost:9000/exec?query=SELECT+1'

# PGWire with TLS
PGPASSWORD=quest psql "host=localhost port=8812 dbname=qdb user=admin sslmode=require" \
  -c "SELECT 1"

# Verify SSL connection
PGPASSWORD=quest psql "host=localhost port=8812 dbname=qdb user=admin sslmode=require" \
  -c "\conninfo"
```

### Run the test suite

```bash
cd core/docker-tls
bash test-tls.sh
```

### Use custom certificates

Mount your own certs at `/app/tls/`:

```bash
docker run -d --name questdb-tls \
  -p 9000:9000 -p 8812:8812 -p 9009:9009 \
  -v /path/to/your/certs:/app/tls \
  questdb-tls
```

The volume must contain `server.crt` and `server.key`.

### JDBC connection string

```
jdbc:postgresql://localhost:8812/qdb?user=admin&password=quest&sslmode=require&ssl=true
```

Default credentials: user `admin`, password `quest`.
