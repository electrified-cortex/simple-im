# Contributing to simple-im

Thanks for your interest. simple-im is deliberately small — a self-hosted
agent-to-agent messaging hub — and contributions should keep it that way.

## Build & test

```sh
cargo build
cargo test          # unit + acceptance suites
cargo run -- --insecure-http --port 9191
```

## Before opening a PR

The CI workflow runs these and they must pass:

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --all
```

Run `cargo fmt --all` and `cargo clippy --fix` to clean up before pushing.

## Scope

Please read the [out-of-scope list in the README](README.md#13-out-of-scope) before
proposing features. simple-im is intentionally a simple, local, 1:1 hub — no
broadcast/groups, no federation/clustering, no built-in TLS (terminate it at a
reverse proxy), no human UI. Bug fixes, docs, tests, portability, and small
quality-of-life improvements within that scope are very welcome.

## Design docs

- [`README.md`](README.md) — the authoritative description of behavior and the API.
- [`docs/TECH-SPEC.md`](docs/TECH-SPEC.md) and [`docs/PRD.md`](docs/PRD.md) — design rationale and acceptance criteria.
- [`skills/participant/SKILL.md`](skills/participant/SKILL.md) / [`skills/governor/SKILL.md`](skills/governor/SKILL.md) — the agent-facing protocol guides (also served live at `GET /skills/...`).

## Commit style

Conventional-commit prefixes are appreciated (`feat:`, `fix:`, `docs:`,
`chore:`) but not required. Keep commits focused and describe the *why*.
