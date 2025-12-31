# Vendor Dependencies

This folder contains vendored third-party code maintained via git subtree.

## duckdb-rs (git subtree)

Remote:
- `duckdb-rs` -> https://github.com/swanlake-io/duckdb-rs

Common commands:

```bash
# Add subtree (one-time)
git subtree add --prefix vendor/duckdb-rs https://github.com/swanlake-io/duckdb-rs <commit-or-branch> --squash

# Fetch latest upstream refs
git fetch duckdb-rs

# Update subtree to latest from a branch (e.g. main)
git subtree pull --prefix vendor/duckdb-rs duckdb-rs main --squash

# Update subtree to a specific commit
git subtree pull --prefix vendor/duckdb-rs https://github.com/swanlake-io/duckdb-rs <commit> --squash

# Inspect current subtree history
git log --oneline -- vendor/duckdb-rs
```
