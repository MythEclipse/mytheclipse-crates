# mytheclipse-tracing

Pre-built tracing infrastructure for mytheclipse applications, wrapping
[`tracing-subscriber`] with sensible defaults and optional OTLP/Jaeger/Zipkin
export.

## Features

| Feature | Default | Description |
| :--- | :---: | :--- |
| `env` | yes | Real: `TracingLayer` — `tracing-subscriber` fmt subscriber with `EnvFilter`. |
| `otel` | no | Stub: `OtelLayer` stores an OTLP endpoint string; no exporter is wired. |
| `jaeger` | no | Placeholder: exposes the same `OtelLayer`; nothing Jaeger-specific. |
| `zipkin` | no | Placeholder: pulls `tracing-subscriber` only, no exporter code. |
| `full` | — | Enables `otel` + `jaeger`. |

## Usage

```toml
[dependencies]
mytheclipse-tracing = "1"
```

### Basic subscriber

```rust
use mytheclipse_tracing::TracingLayer;

fn main() {
    TracingLayer::install();
    tracing::info!("hello, world!");
}
```

### With OTLP export

> `otel` currently provides the `OtelLayer` type and endpoint builder only; no
> OTLP exporter is wired up yet.

```toml
[dependencies]
mytheclipse-tracing = { version = "1", features = ["otel"] }
```

```rust
use mytheclipse_tracing::OtelLayer;

fn main() {
    let layer = OtelLayer::new("http://localhost:4317");
    println!("configured OTLP endpoint: {}", layer.endpoint());
}
```
