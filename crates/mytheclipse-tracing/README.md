# mytheclipse-tracing

One call installs a `tracing` subscriber: `tracing-subscriber` fmt output,
filtered by `RUST_LOG`, written to **stderr**.

## Features

| Feature | Default | Description |
| :--- | :---: | :--- |
| `env` | yes | `TracingLayer` — fmt subscriber with `EnvFilter`. |

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

### What `install()` guarantees

| Behaviour | Detail |
| :--- | :--- |
| Output stream | stderr. stdout is left to your program, so piped output stays clean. |
| Default filter | `info` when `RUST_LOG` is unset. `RUST_LOG` overrides it, e.g. `RUST_LOG=mycrate=debug,info`. |
| Invalid `RUST_LOG` | Falls back to `info` instead of disabling logging. |
| Colour | ANSI escapes only when stderr is a terminal, so redirected logs stay readable. |
| Second call | No-op. `install()` ignores it; `try_install()` returns `Err`. |
| Call site | Once, before the first span. It sets the global subscriber. |

`TracingLayer` is a namespace, not a `tracing_subscriber::Layer`. When you are
composing a subscriber yourself, take the layer instead:

```rust
use mytheclipse_tracing::TracingLayer;
use tracing_subscriber::prelude::*;

let subscriber = tracing_subscriber::registry().with(TracingLayer::layer());
tracing::subscriber::set_global_default(subscriber).unwrap();
```

Use `try_install()` when you need to know whether installation took effect —
for example when the host application may already have set a subscriber:

```rust
use mytheclipse_tracing::TracingLayer;

if let Err(err) = TracingLayer::try_install() {
    eprintln!("tracing already initialised: {err}");
}
```

## Scope

This crate installs a subscriber; it does not export spans anywhere. For
OTLP/Jaeger/Zipkin, wire `tracing-opentelemetry` (or the matching exporter)
yourself and add it as another layer. An earlier `otel`/`jaeger`/`zipkin`
feature set here carried unused dependencies and no exporter, so it was removed.
