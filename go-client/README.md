# SwanDB Go Client (ADBC Flight SQL)

This is a Go client for testing SwanDB's Flight SQL server using Apache Arrow ADBC.

## Prerequisites

- Go 1.21 or later
- SwanDB Flight SQL server running (default: `127.0.0.1:4214`)

## Setup

1. Install dependencies:

```bash
cd go-client
go mod download
```

2. Make sure the SwanDB server is running:

```bash
# In the project root
cargo run
```

## Running the Client

Run the test client:

```bash
go run main.go
```

The client will automatically connect to `127.0.0.1:4214`. You can override this using environment variables:

```bash
export SWANDB_HOST=localhost
export SWANDB_PORT=4214
go run main.go
```

## What the Client Tests

The test program executes the following scenarios:

1. **Simple Query** - Tests basic SELECT query execution
   ```sql
   SELECT 1 as id, 'test' as name
   ```

2. **DDL Statement** - Tests table creation
   ```sql
   CREATE TABLE test_table (id INTEGER, name VARCHAR)
   ```

3. **DML Statement** - Tests data insertion
   ```sql
   INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
   ```

4. **Query Table** - Tests querying inserted data
   ```sql
   SELECT * FROM test_table ORDER BY id
   ```

5. **Prepared Statement** - Tests the prepared statement flow
   ```sql
   SELECT COUNT(*) as total FROM test_table
   ```

## Expected Output

```
Connecting to Flight SQL server at grpc://127.0.0.1:4214...
✓ Connected successfully!

=== Test 1: Simple Query ===
Executing: SELECT 1 as id, 'test' as name
Schema: schema:
  fields: 2
    - id: type=int32
    - name: type=utf8
Row 0: id=1, name='test'
✓ Query returned 1 rows

=== Test 2: DDL Statement (CREATE TABLE) ===
Executing: CREATE TABLE test_table (id INTEGER, name VARCHAR)
✓ Statement executed, affected rows: 0

=== Test 3: DML Statement (INSERT) ===
Executing: INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
✓ Inserted 3 rows

=== Test 4: Query Inserted Data ===
Executing: SELECT * FROM test_table ORDER BY id
Schema: schema:
  fields: 2
    - id: type=int32
    - name: type=utf8
Row 0: id=1, name='Alice'
Row 1: id=2, name='Bob'
Row 2: id=3, name='Charlie'
✓ Query returned 3 rows

=== Test 5: Prepared Statement Flow ===
Preparing statement: SELECT COUNT(*) as total FROM test_table
✓ Statement prepared successfully
Total count: 3

✓ All tests passed!
```

## Using ADBC in Your Own Code

Here's a minimal example:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/apache/arrow-adbc/go/adbc"
    "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
)

func main() {
    ctx := context.Background()

    // Create driver and connect
    var drv flightsql.Driver
    db, _ := drv.NewDatabase(map[string]string{
        adbc.OptionKeyURI: "grpc://127.0.0.1:4214",
        flightsql.OptionSSLSkipVerify: adbc.OptionValueEnabled,
    })
    defer db.Close()

    conn, _ := db.Open(ctx)
    defer conn.Close()

    // Execute a query
    stmt, _ := conn.NewStatement()
    defer stmt.Close()

    stmt.SetSqlQuery("SELECT 1 as id, 'test' as name")
    reader, _, err := stmt.ExecuteQuery(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Release()

    // Process results
    for reader.Next() {
        record := reader.Record()
        fmt.Printf("Got %d rows\n", record.NumRows())
    }
}
```

## Troubleshooting

### Connection Refused

If you see `connection refused`, make sure:
- The SwanDB server is running (`cargo run` in the project root)
- The host/port are correct (default: `127.0.0.1:4214`)
- No firewall is blocking the connection

### Missing Dependencies

If you see import errors, run:
```bash
go mod tidy
```

### TLS/SSL Errors

The client disables TLS verification for local testing. For production use, remove the `OptionSSLSkipVerify` option and configure proper TLS.

## Next Steps

Based on the test results, you can identify which Flight SQL methods need to be implemented in the server. Common issues you might encounter:

- Missing `GetSqlInfo` - Returns server metadata
- Missing `GetCatalogs/GetSchemas/GetTables` - Returns database metadata
- Missing parameterized queries - Requires binding parameter support
- Missing transactions - Requires `BeginTransaction`, `Commit`, `Rollback`

Add tests for these features as you implement them in the server!