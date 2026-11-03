# mytheclipse-config

Type-safe, dynamic configuration: load `.env`, YAML, JSON, or TOML directly
into a typed Rust struct, merge multiple sources with environment variables
taking priority, and optionally hot-reload when the source files change.

## Features

- `env` (default) — `.env` file loading via `dotenvy`.
- `yaml` / `toml` (default) — structured file parsing (`.json` is always available).
- `hot-reload` (default) — watch files and swap in a freshly reloaded value.
- `validation` (default) — validation traits and built-in validators.
- `schema` — JSON Schema generation for config docs and validation.

## Usage

```rust
use std::path::Path;

use mytheclipse_config::ConfigLoader;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    port: u16,
    database_url: String,
}

let config: AppConfig = ConfigLoader::new()
    .merge_file(Path::new("config.yaml"))?
    .merge_env("APP")   // APP_PORT, APP_DATABASE_URL override the file
    .build()?;
```

### Hot-reload

```rust
use std::path::Path;

use mytheclipse_config::DynamicConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig { port: u16 }

let cfg = DynamicConfig::<AppConfig>::watch_files(
    vec!["config.yaml".into()],
    || {
        ConfigLoader::new()
            .merge_file(Path::new("config.yaml"))?
            .build()
    },
)?;

let mut changes = cfg.subscribe();
let live = cfg.clone();
tokio::spawn(async move {
    while changes.recv().await.is_ok() {
        println!("config reloaded: {:?}", live.get());
    }
});
```
