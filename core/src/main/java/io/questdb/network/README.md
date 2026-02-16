# Server-Side TLS Support

QuestDB supports TLS encryption for all server protocols: HTTP, Min HTTP,
PGWire, and ILP/TCP (InfluxDB Line Protocol over TCP).

## Configuration

### Global TLS (applies to all protocols)

```properties
tls.enabled=true
tls.cert.path=server.crt
tls.private.key.path=server.key
```

### Per-Protocol Override

Each protocol can override the global TLS settings independently:

```properties
# HTTP server
http.tls.enabled=true
http.tls.cert.path=http-server.crt
http.tls.private.key.path=http-server.key

# Min HTTP server (health check endpoint)
http.min.tls.enabled=true
http.min.tls.cert.path=min-http.crt
http.min.tls.private.key.path=min-http.key

# ILP/TCP (InfluxDB Line Protocol)
line.tcp.tls.enabled=true
line.tcp.tls.cert.path=ilp.crt
line.tcp.tls.private.key.path=ilp.key

# PostgreSQL Wire Protocol
pg.tls.enabled=true
pg.tls.cert.path=pg.crt
pg.tls.private.key.path=pg.key
```

Per-protocol settings inherit from the global config. Setting
`pg.tls.enabled=false` explicitly disables TLS for PGWire even when the global
`tls.enabled=true`.

## PEM File Requirements

- **Certificate**: X.509 format, PEM-encoded. Certificate chains are supported.
- **Private key**: PKCS8 format, PEM-encoded (`-----BEGIN PRIVATE KEY-----`).
  Both RSA and EC keys are supported.

PKCS1 format (`-----BEGIN RSA PRIVATE KEY-----`) is not supported. Convert with:

```bash
openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem
```

## Generating Test Certificates

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

The generated key is already in PKCS8 format when using `-nodes`.

## Architecture

- `PemReader` loads PEM cert/key into a Java `SSLContext`
- `JavaTlsServerSocket` wraps a `PlainSocket` with server-mode TLS via
  `SSLEngine`
- `JavaTlsServerSocketFactory` creates TLS socket instances with a shared
  `SSLContext`
- `FactoryProviderImpl` selects plain or TLS socket factories based on
  configuration

TLS configuration changes require a server restart.
