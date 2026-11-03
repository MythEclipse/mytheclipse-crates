# mytheclipse-tracing

A pre-built `tracing` subscriber with environment filtering for mytheclipse
applications: one call installs a `tracing-subscriber` fmt subscriber driven by
`RUST_LOG`.

## Features

| Feature | Default | Description |
| :--- | :---: | :--- |
| `env` | yes | `TracingLayer` — `tracing-subscriber` fmt subscriber with `EnvFilter`. |

## Usage

```toml
[dependencies]
mytheclipse-tracing = "1"
```

```rust
use mytheclipse_tracing::TracingLayer;

fn main() {
    TracingLayer::install();
    tracing::info!("hello, world!");
}
```

`TracingLayer::install()` sets the global default subscriber, so call it once,
before the first span is created. `RUST_LOG` selects the filter, e.g.
`RUST_LOG=mycrate=debug,info`.

## Scope

This crate installs a subscriber; it does not export spans anywhere. If you need
OTLP/Jaeger/Zipkin, wire `tracing-opentelemetry` (or the equivalent exporter)
yourself and register the layers — an earlier `otel`/`jaeger`/`zipkin` feature
here only carried unused dependencies, so they were removed.
