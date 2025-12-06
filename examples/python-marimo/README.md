# Marimo + Flight SQL Demo

An interactive [marimo](https://docs.marimo.io/) notebook that talks to a running SwanLake instance over Arrow Flight SQL.

## Run

1. Start SwanLake locally (defaults to `grpc://127.0.0.1:4214`).
2. Install deps in this directory (either `uv sync` to honor `uv.lock` or `pip install 'marimo[sql]>=0.18.0' 'adbc-driver-flightsql>=1.9.0'`).
3. Launch the UI from this folder:
   ```bash
   uv run marimo run swanlake.py   # or: marimo run swanlake.py
   ```

The notebook creates a table, inserts a couple rows, and lists tables using the active connection. Update the `connect(...)` URI in `swanlake.py` if your server runs elsewhere.
