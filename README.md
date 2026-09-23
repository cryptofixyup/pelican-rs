# pelican-rs

Sub-microsecond telemetry ingestor for the SAFEWEB // WATCHDOG trust substrate.

## Crates

- `crates/wire` — hardened zero-copy wire decoder (`VersionedTelemetryFrame`), CRC32-C pipeline, no unsafe
- `crates/app` — integration harness

## Commands

```bash
cargo check --workspace
cargo test --workspace
cargo run -p app
```

## Architecture

Validation pipeline: `length → magic/version → CRC32-C → integer decode → float validation → semantic bounds → auth/replay`

CRC32-C provides integrity against accidental corruption. Cryptographic authentication (AES-256-GCM on x86_64, ChaCha20-Poly1305 elsewhere) sits above this layer.
