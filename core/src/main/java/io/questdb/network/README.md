# TLS Server Sockets (OSS Overlay)

This package now contains server-side TLS socket support used by QuestDB OSS listeners:

- `JavaTlsServerSocket` — non-blocking TLS socket implementation for server-side `SSLEngine`.
- `JavaTlsServerSocketFactory` — creates per-connection `JavaTlsServerSocket` instances from a shared `SSLContext`.
- `PemReader` — loads PEM certificate chain + PKCS8 private key and builds an `SSLContext`.

## Protocol Coverage

The TLS socket factory is wired for:

- HTTP (`http.tls.*`)
- Min HTTP (`http.min.tls.*`)
- PGWire (`pg.tls.*`)
- ILP/TCP (`line.tcp.tls.*`)

Global defaults can be set with `tls.*` keys and overridden per protocol.

## Key Behavioral Notes

- PGWire plaintext SSLRequest exchange is supported before TLS starts.
- TLS handshakes are non-blocking and EAGAIN-safe (no busy spin on read/write starvation).
- `isMorePlaintextBuffered()` is implemented to avoid read stalls when multiple TLS records are buffered.
- PKCS1 private keys are rejected with a conversion hint. Supported key format is PKCS8 PEM (`BEGIN PRIVATE KEY`).
