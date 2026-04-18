[![codecov](https://codecov.io/github/swanlake-io/swanlake/graph/badge.svg?token=BPEVOE3Z8Y)](https://codecov.io/github/swanlake-io/swanlake)
[![DuckDB](https://img.shields.io/badge/DuckDB-v1.5.2-blue?logo=duckdb)](https://github.com/duckdb/duckdb/releases/tag/v1.5.2)
[![DuckLake](https://img.shields.io/badge/DuckLake-v1.0-blue?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAIAAAD8GO2jAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAIKADAAQAAAABAAAAIAAAAACshmLzAAAFJklEQVRIDb2Wa0hkZRjHz3suc9NxaGl1dUciCwZt64NCkZZsKeii1hQuBrV2IViV8FOkn4I+LGYf2vbDqhQSCoGGkJtaCSoLoRS0Y7ChuaHtrpMzbuk6OzNnZs61//FMZy6dUSvoMAzvOe/z/J738tyIqqrUYU8kEtnc3AwEAtFoFLJ5eXnFxcWlpaX5+fmHqVLsARI8z8/Nzc3MzPh8vmAwGI/HFUWBPE3TNputqKioqqqqubm5rq7O4XDk4hDTHUiSNDY2NjQ0tLq6CgGO4xiGIYQYFHyEMUEQ8LGioqKjo6OtrY1lTZZrYmBjY6Onp2d+fh4KQBvQXANRFLEg7KO/v7+srCxLLNvA4uJiV1cXTtxut2eJHvwai8VwKwMDAzU1NemSGQaWlpba29tDoZDFYkkXOuIYJ+ZyuUZHR6urqw2VlAGcjNfrhav8O7pOhA042OTkpHFWtD6Bc+zt7fX7/f+FDhTUAQEKQJ2cNDAxMQGPPMK5Hx40gAAFYMoA/B2XY+pkupAqCaoQo1QtCFQxjh91YHgCBSCwkNc8d2FhYWVlBbGj4zL+VUWVJYvnKevjXtb9KMWw8u8bwvLXieWvKClBMeZODOcGEI7e0tKiGZiamjINN23JNOtsu2Cvf4PiKOVuhJIl7qHHbE97heVvw592K7t+zUZaABqLA3B6elozgDyDTGB+t7KU/1Kfo+k14eef+Cv90q1lVZaZojJHY7ftyQZiGQ5dfoUSeCUeJTRD4Zf2AAgs4CwufXt7G+klbVYbqmLCUnHaXv+qsHo9dPGsEtomnBWLFX/57t76D2piyFb7wn3vXlXFmLRxLTrxnrIXxAEaEACBBZyG4yMI0/NMUkhVrE+0UgzhJ/s0utWhrZHQhMNVqdEvLog3rqsJnnAOe+2L+ec+RArEd8MAgMACzobDYT1HGnP7AxUg1v2I8see+KuPWDLvn+GUu7/t9TUAiG253pmxlJ+mj7mVndu4M4MDLOCpd2MiOcAaaBYuRCly9tT+O1P4MLE76YLjTMH9aiJKCXGKSqVbQ4V1Op1/vwCI4g7kndvWBzzM8QfFWz7CWg0dmCQOl6t7jDlRovm5REU/75NDgQyZ/bKhwZE6EHsmbqqqCd80sdH2hre0s8VW9EdRcPT2Z95kTpYIK9/zX14OXXqd/+YSYTPyI4DAAs663W7UJuRnlJTUGrEFzpq4diXx48u22ufU2EV+6gN5L4AApvOOOc5053nfloPB8Cfn5TvrFGvZv/l0bQoXACzgLOpqZWXl+vp6diJC+Ijx8HBXQcew48w5a9Xz0k3EgciWnmJPnpACwfDH5+VdP7EXZID/ekFaBRZwzf0RbyZuignNW/yhj85GPutT7gU4T7Xl1LMIEH5meO/9ZuHGohYZOR4AgcWkVg/QKzQ2Nq6trZkXSKQjMUFsTtpVSGhaCe8okV2CDJEWVllWkKs9Hs/s7CyaAW0HaENQJlFXs+SSrwgui52SRWVnU75zEx6pveamQwsoAPVWI5khWltb6+vrEXvmNvAVV4JIBpckVXJJAgIUgLrA/1UyYQ1VdHBwEFUbDpBrdQd/14s+IEZBhnzGftFxjIyMIDoOOqscRqACRahntS0ZBqCLafQE6KKgYBTuHMzkZ4hBGCpQzKJDInUH6RS4wfj4ODZ7lNaxvLy8s7PzH7SOhiXEB+qq3vyiemCZemJHckTYFxYWovltamrC2uHohlbWwHwHWUKofKhNW1tbGGAKCaCkpAR55ijt+59Rt6fIUXrnVgAAAABJRU5ErkJggg==)](https://ducklake.select)

# SwanLake

SwanLake is an Arrow Flight SQL server backed by DuckDB, enabling fast data analytics and ingestion with datalake support.

<!--![SwanLake](swanlake.jpeg)-->
<img src="swanlake.jpeg" width="512" alt="SwanLake">

## Quick Start

First start the SwanLake server:

```bash
# From the swanlake root directory
RUST_LOG=info cargo run --bin swanlake

# Or using Docker
docker run --rm -p 4214:4214  ghcr.io/swanlake-io/swanlake:latest
```

Then run the Rust interactive client example:

```bash
cargo run --bin swanlake-cli --features="cli"
```

## Architecture

- **Arrow Flight SQL Server**: High-performance SQL interface over gRPC for efficient querying.
- **DuckLake**: Extensions for datalake integrations, supporting scalable storage solutions.

## Use Cases

- Building datalakes on Postgres and S3 for unified data access.
- Rapid ingestion of logs, metrics, and streaming data.
- High-speed querying using DuckDB and Arrow for analytics.

## Status Page

SwanLake includes a built-in status page for real-time monitoring of your server. Access it at `http://localhost:4215` (default) to view:

- Active sessions and uptime
- Query and update latency metrics (average, P95, P99)
- Slow queries and recent errors

<!--![Status](status_page.png)-->
<img src="status_page.png" width="512" alt="Status">

Configure the status page using environment variables:
- `SWANLAKE_STATUS_ENABLED` (default: `true`)
- `SWANLAKE_STATUS_HOST` and `SWANLAKE_STATUS_PORT` (default: `0.0.0.0:4215`)

See [CONFIGURATION.md](CONFIGURATION.md) for more details.

## Deployment

SwanLake supports serverless deployment via Docker. Pull the latest image from [GitHub Container Registry](https://github.com/swanlake-io/swanlake/pkgs/container/swanlake).

Customize with environment variables; see [CONFIGURATION.md](CONFIGURATION.md) for details.

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
