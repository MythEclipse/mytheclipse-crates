# mytheclipse-event

A unified events & message bus abstraction so component-to-component (or
service-to-service) communication isn't locked to one transport.

- **In-memory dispatcher** (default) — `tokio::sync::broadcast`-backed
  pub/sub for communication inside a single process (monolith).
- **Distributed broker backends** — RabbitMQ (`amqp`) and NATS (`nats`)
  behind the same `EventBus` trait, so swapping the backend doesn't change
  handler code.
- **Typed convenience** (feature `typed`) — `TypedEventBus` layers JSON
  encoding on top.

## Features

- `mem` (default) — in-memory dispatcher.
- `typed` — `TypedEventBus`, JSON publish/subscribe over any `EventBus` (adds `serde`).
- `amqp` — RabbitMQ via `lapin` (pure Rust AMQP 0.9.1 client).
- `nats` — NATS via `async-nats`.

## Usage

`EventBus` moves `Vec<u8>`, and that is all the default feature set compiles —
no `serde`, no `serde_json`:

```rust
use mytheclipse_event::prelude::*;

let bus = InMemoryEventBus::default();
let mut sub = bus.subscribe("orders").await?;
bus.publish("orders", b"{\"id\":42}".to_vec()).await?;
let payload = sub.recv().await?;
```

With `features = ["typed"]` you get JSON encoding over the same trait:

```rust
use mytheclipse_event::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct OrderCreated { id: u64 }

let bus = TypedEventBus::new(InMemoryEventBus::default());
let mut sub = bus.subscribe::<OrderCreated>("orders").await?;
bus.publish("orders", &OrderCreated { id: 42 }).await?;
let event = sub.recv().await?;
```

Swap `InMemoryEventBus::default()` for `AmqpEventBus::connect(url, "exchange").await?`
or `NatsEventBus::connect(url).await?` to move to a distributed broker without
touching handler code.
