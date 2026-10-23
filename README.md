# Rack Management Service Rust crate

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

This crate provides shared RMS protobuf wire types plus optional generated client
and server support for the NVIDIA Rack Management Service (RMS).

The Cargo package is named **`librms`**. Use it in Rust services and tools that
need RMS wire types or access to an existing RMS endpoint. This repository does
not run an RMS service or manage hardware on its own.

[Getting started](#getting-started) · [Client example](#read-only-client-example) ·
[Architecture and API reference](#architecture-and-api-reference) ·
[Testing](#testing) · [Contributing and support](#contributing-and-support)

## Overview

The NVIDIA Rack Management Service provides functionality for managing NVIDIA
multi-node NVLINK rack-scale hardware.

## Features

- `client`: generated client APIs plus the TLS, retry, and transport stack.
- `server`: generated `rack_manager_server` service definitions.
- `serde`: serde derives for generated protobuf models.

The default feature set is `client` and `serde` to preserve existing client
consumer behavior. Consumers that only need shared types or server definitions
can use `default-features = false` and enable the smaller feature set they need.

## Getting started

### Prerequisites

- A current stable Rust toolchain with Cargo. `Cargo.toml` uses Rust edition 2024
  but does not declare a minimum supported Rust version (MSRV).
- The Protocol Buffers compiler, `protoc`, on `PATH`, including the standard
  `.proto` include files such as `google/protobuf/timestamp.proto`. `build.rs`
  uses them to generate Rust code from the protobuf schemas at build time. On
  Debian or Ubuntu, install `protobuf-compiler` and `libprotobuf-dev`.
- A native C build toolchain for the TLS dependencies. Additional tools depend
  on the target platform; see the [AWS-LC build requirements](https://aws.github.io/aws-lc-rs/requirements/).
- To run a client: a reachable RMS HTTPS endpoint, its trusted CA certificate,
  and a client certificate and private key accepted by that service.

Check the build tools before you start:

```bash
rustc --version
cargo --version
protoc --version
cc --version
```

### Build from source

```bash
git clone https://github.com/dsx-ai-factory/nv-rms-client.git
cd nv-rms-client
cargo build
```

### Add to a Rust project

Add the Git dependency to your project's existing `[dependencies]` table:

```toml
librms = { git = "https://github.com/dsx-ai-factory/nv-rms-client.git" }
```

To use only the protobuf wire types, disable the default features:

```toml
librms = { git = "https://github.com/dsx-ai-factory/nv-rms-client.git", default-features = false }
```

Add `features = ["server"]` to the latter dependency when you need generated
server definitions. See [Features](#features) for the available combinations.

## Read-only client example

This example requests the server version with `GetVersion`. It does not change
rack configuration. Create a separate application:

```bash
cargo new rms-version
cd rms-version
```

Use this `Cargo.toml`:

```toml
[package]
name = "rms-version"
version = "0.1.0"
edition = "2024"

[dependencies]
librms = { git = "https://github.com/dsx-ai-factory/nv-rms-client.git" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Use this `src/main.rs`:

```rust
use std::time::Duration;

use librms::client::{RetryConfig, RmsApiConfig};
use librms::client_config::{ClientCert, RmsClientConfig};
use librms::{RackManagerApi, RmsApi};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("RMS_URL")?;
    let config = RmsClientConfig {
        root_ca_path: Some(std::env::var("RMS_CA_CERT")?),
        client_cert: Some(ClientCert {
            cert_path: std::env::var("RMS_CLIENT_CERT")?,
            key_path: std::env::var("RMS_CLIENT_KEY")?,
        }),
        enforce_tls: true,
        connect_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    let api_config = RmsApiConfig::new(&url, &config).with_retry_config(RetryConfig {
        retries: 2,
        interval: Duration::from_secs(1),
    });
    let client = RackManagerApi::new(&api_config);
    let response = tokio::time::timeout(Duration::from_secs(30), client.get_version()).await??;
    println!("RMS version: {}", response.version);
    Ok(())
}
```

Set the endpoint and paths to your deployment's PEM files, then run the client:

```bash
export RMS_URL=https://rms.example.com:50051
export RMS_CA_CERT=/path/to/ca.crt
export RMS_CLIENT_CERT=/path/to/client.crt
export RMS_CLIENT_KEY=/path/to/client.key
cargo run
```

Expected output is `RMS version: <server version>`. Missing environment variables,
invalid certificate files, an untrusted server, or a failed connection return an
error. The 30-second timeout bounds the complete call, including connection
setup and readiness retries. `connect_timeout` alone does not limit application
RPC execution.

`enforce_tls: true` requires HTTPS and valid TLS inputs; it does not supply
certificates or trust roots. The example sets the struct fields directly to keep
enforcement enabled. `RmsClientConfig::new` can disable enforcement when the
required certificate inputs are absent. Do not use that fallback for a production
connection that requires verified mTLS.

## Architecture and API reference

The build and API layers are:

```text
proto/rack_manager.proto + proto/rack_manager_v2.proto
    -> build.rs (protoc, prost/tonic generation, client wrappers)
    -> generated files in Cargo's OUT_DIR
    -> src/protos/mod.rs (public V1/V2 models and selected service bindings)
    -> src/client.rs + src/client_config.rs (HTTP/2, TLS, connection retries)
    -> src/client_api.rs (RackManagerApi and the RmsApi trait)
```

| Entry point | Purpose |
| --- | --- |
| [`proto/`](proto/) | V1 and V2 message definitions and RPC contracts. |
| [`librms::protos`](src/protos/mod.rs) | Generated models, clients, server definitions, and convenience wrappers, selected by Cargo features. |
| [`RmsClientConfig`](src/client_config.rs) | Certificate paths, TLS enforcement, connection timeout, and optional observability hooks. |
| [`RmsApiConfig` and `RetryConfig`](src/client.rs) | Endpoint and readiness retry policy. |
| [`RackManagerApi` and `RmsApi`](src/client_api.rs) | Application-facing API used by the example. |
| [`tonic-client-wrapper`](tonic-client-wrapper/README.md) | Shared lazy connection and generated wrapper implementation. |

Generate a local API reference, including all feature-gated items:

```bash
cargo doc --all-features --no-deps --open
```

The generated reference starts at `target/doc/librms/index.html`. Edit the
protobuf schemas or generator when changing generated APIs; generated Rust files
under `OUT_DIR` are build outputs.

## Testing

Run the feature matrix before publishing changes that touch generated protobuf,
client, server, or serde support:

```bash
cargo test --no-default-features
cargo test --no-default-features --features client
cargo test --no-default-features --features server
cargo test --all-features
```

## Caller-owned observability

Transport layers are trusted, caller-supplied code that sits above TLS and sees plaintext
requests and responses. Production deployments are expected to use mTLS.

`RmsClientConfig::transport_layer` accepts an optional `Arc<dyn RmsTransportLayer>`.
Its `layer` method wraps the cloneable HTTP transport before the V1 or V2 tonic client
is constructed. The layer is applied again on connection rebuilds and covers readiness
probes as well as application RPCs. TLS, timeout, pooling, and retry policy remain owned
by librms. With `None` (the default), transport behavior is unchanged.

`RmsClientConfig::rpc_observer` accepts an optional `Arc<dyn RpcObserver>` for decoded
unary calls through generated API wrappers and `RackManagerApi`, including its V2 method.
Observers receive protobuf method/type names, encoded message bodies, client-side UTC
call boundary times as `SystemTime`, and the final tonic status code. Call start precedes
lazy connection setup and readiness retries. Readiness probes do not create separate
observations. The observer's span is entered while polling the call, so a transport layer
can propagate that span's context. No encoding or observer callback occurs with `None` or
when `RpcObserver::enabled` returns false.

Observers receive unredacted payloads; redaction is the observer's responsibility, and
observers must not block. librms does not log the payloads itself. `RpcObserver::start` and
`RpcObservation::finish` return `Result`; an `Err` is logged at `warn` level and never
affects the RPC, which is then unobserved (for `start`) or simply not recorded (for
`finish`). Error status text/details and metadata are deliberately excluded from the
observer API. Implement `Drop` on the returned `RpcObservation` to record calls
cancelled before completion. `FILE_DESCRIPTOR_SET` contains the V1/V2 descriptors for
policy implementations that decode payloads dynamically. Raw tonic clients and streaming
RPCs do not use the decoded observer.

## Contributing and support

Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the Developer Certificate of
Origin (DCO), and pull request guidelines. Run the feature test matrix above
before submitting changes to protobuf generation or feature support.

Use the [issue tracker](https://github.com/dsx-ai-factory/nv-rms-client/issues)
for bugs, usage questions, and feature requests. Include the crate revision,
enabled features, build tool versions, and a minimal reproduction. For client
connection problems, include the error and endpoint scheme, but exclude private
keys, credentials, and sensitive RPC payloads.

Follow the [Code of Conduct](CODE_OF_CONDUCT.md). Report security vulnerabilities
through the private channels in [SECURITY.md](SECURITY.md), rather than a public
issue. These documents describe the current contribution and conduct policies.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
