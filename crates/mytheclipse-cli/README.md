# mytheclipse-cli

CLI framework for mytheclipse applications with built-in subcommands:
`serve`, `worker`, `migrate`, `health`, and `version`.

## Features

| Feature | Default | Description |
| :--- | :---: | :--- |
| `clap-derive` | yes | Clap derive macros for argument parsing. |

## Usage

```toml
[dependencies]
mytheclipse-cli = { git = "https://github.com/MythEclipse/mytheclipse-crates", package = "mytheclipse-cli" }
```

The crate lives in the workspace but is **not published to crates.io yet**, so
there is no version to depend on — use a path or git dependency.

```rust
use mytheclipse_cli::{CliApp, Subcommand};

fn main() {
    let app = CliApp::parse();
    match app.command {
        Subcommand::Serve => { /* ... */ }
        Subcommand::Worker { topics } => { /* ... */ }
        Subcommand::Migrate => { /* ... */ }
        Subcommand::Health => { /* ... */ }
        Subcommand::Version => { println!("1.0.0"); }
    }
}
```

`CliApp` derives `clap::Parser`, so `CliApp::parse()` reads `std::env::args`.
Use `CliBuilder::new(name, about).build()` when you need to override the
program name and about text at runtime.
