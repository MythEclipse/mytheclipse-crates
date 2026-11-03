# mytheclipse-cache

A unified multi-layer cache abstraction so your app isn't locked to one cache
provider. Combines an in-process **L1** cache with a distributed **L2** cache
(e.g. Redis/Valkey) behind one simple `get`/`set`/`invalidate` API, plus a
**cache-aside / auto-refresh** helper.

## Features

- `l1-memory` (default) — in-process cache, no external service.
- `l2-redis` — Redis/Valkey L2 via `fred`.
- `cache-aside` (default) — read-through cache-aside helper.
- `typed` — `TypedCache`, JSON get/set over any `Cache` (adds `serde`).

## Typed values

`Cache` moves `Vec<u8>`. When you want typed get/set, enable `typed`, which
adds `TypedCache` (JSON encoding via `serde`):

```rust
use mytheclipse_cache::prelude::*;

let cache = TypedCache::new(MemoryCache::new());
cache.set("orders:42", &Order { id: 42 }, None).await?;
let order: Option<Order> = cache.get("orders:42").await?;
```

## Usage

```rust
use mytheclipse_cache::prelude::*;

let cache = MultiLayerCache::new(
    MemoryCache::new(),          // L1
    MemoryCache::new(),          // L2 (use RedisCache in production)
);
cache.set("k", b"v".to_vec(), None).await.unwrap();
let v = cache.get("k").await.unwrap();

// Cache-aside: fill misses from a source of truth.
let aside = CacheAside::new(
    MemoryCache::new(),
    |key| async move { Some(format!("data-for-{key}").into_bytes()) },
);
let _ = aside.get("orders:42").await.unwrap();
```
