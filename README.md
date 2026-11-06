# axylph

Axylph will receive sequencing reads and use sylph to produce metagenomic profiles. This repository currently contains development tooling only. It does not yet run a service or produce profiles.

## Development

Use Mise 2026.10.22 or newer. Rust is pinned in `mise.toml` and `rust-toolchain.toml`; keep these versions and the Cargo Rust requirement in sync.

```sh
mise trust
mise install
mise run check
mise run ci-validate
```

`check` runs formatting, Clippy, nextest, documentation checks, and doctests when `src/lib.rs` exists. The initial scaffold has no tests or library target. Build and verification tasks use `Cargo.lock` without updating dependencies.

Use `mise run bc`, `mise run bb`, or `mise run bn` to watch checks, builds, or tests with Bacon. Mise enables mr-boxington for Rust builds. Use `mise run mt` to inspect its activity. These settings do not by themselves prove cache hits.

## Upstream reference

```sh
mise bootstrap repos apply
```

This clones sylph into `.agents/repos/sylph` for source reference. It is not an axylph dependency. Planning notes and reference checkouts remain local and are ignored by version control.
