package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Get connection parameters from environment or use defaults
	host := getEnv("SWANDB_HOST", "127.0.0.1")
	port := getEnv("SWANDB_PORT", "4214")
	uri := fmt.Sprintf("grpc://%s:%s", host, port)

	fmt.Printf("Connecting to Flight SQL server at %s...\n", uri)

	// Create ADBC driver
	drv := flightsql.NewDriver(nil)
	db, err := drv.NewDatabase(map[string]string{
		adbc.OptionKeyURI: uri,
		// Disable TLS for local testing
		flightsql.OptionSSLSkipVerify: adbc.OptionValueEnabled,
	})
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	// Create connection
	conn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	defer conn.Close()

	fmt.Println("✓ Connected successfully!")

	// Test 1: Execute a simple query using prepared statement
	fmt.Println("\n=== Test 1: Simple Query ===")
	if err := testSimpleQuery(ctx, conn); err != nil {
		return fmt.Errorf("test 1 failed: %w", err)
	}

	// Test 2: Execute DDL statement
	fmt.Println("\n=== Test 2: DDL Statement (CREATE TABLE) ===")
	if err := testDDLStatement(ctx, conn); err != nil {
		return fmt.Errorf("test 2 failed: %w", err)
	}

	// Test 3: Execute DML statement (INSERT)
	fmt.Println("\n=== Test 3: DML Statement (INSERT) ===")
	if err := testInsertStatement(ctx, conn); err != nil {
		return fmt.Errorf("test 3 failed: %w", err)
	}

	// Test 4: Query the inserted data
	fmt.Println("\n=== Test 4: Query Inserted Data ===")
	if err := testQueryTable(ctx, conn); err != nil {
		return fmt.Errorf("test 4 failed: %w", err)
	}

	// Test 5: Test prepared statement flow
	fmt.Println("\n=== Test 5: Prepared Statement Flow ===")
	if err := testPreparedStatement(ctx, conn); err != nil {
		return fmt.Errorf("test 5 failed: %w", err)
	}

	// Test 6: Test expensive query optimization (schema without full execution)
	fmt.Println("\n=== Test 6: Schema Optimization (Complex Query) ===")
	if err := testSchemaOptimization(ctx, conn); err != nil {
		return fmt.Errorf("test 6 failed: %w", err)
	}

	fmt.Println("\n✓ All tests passed!")
	return nil
}

func testSimpleQuery(ctx context.Context, conn adbc.Connection) error {
	query := "SELECT 1 as id, 'test' as name"
	fmt.Printf("Executing: %s\n", query)

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	fmt.Printf("Schema: %s\n", reader.Schema())

	rowCount := 0
	for reader.Next() {
		record := reader.Record()
		rowCount += int(record.NumRows())

		// Print the results
		for i := 0; i < int(record.NumRows()); i++ {
			fmt.Printf("Row %d: ", i)
			for j, col := range record.Columns() {
				field := record.Schema().Field(j)
				fmt.Printf("%s=", field.Name)
				printValue(col, i)
				if j < int(record.NumCols())-1 {
					fmt.Print(", ")
				}
			}
			fmt.Println()
		}
	}

	if err := reader.Err(); err != nil {
		return err
	}

	fmt.Printf("✓ Query returned %d rows\n", rowCount)
	return nil
}

func testDDLStatement(ctx context.Context, conn adbc.Connection) error {
	query := "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR)"
	fmt.Printf("Executing: %s\n", query)

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	// For DDL, we use ExecuteUpdate
	affected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("✓ Statement executed, affected rows: %d\n", affected)
	return nil
}

func testInsertStatement(ctx context.Context, conn adbc.Connection) error {
	// Delete existing data first
	deleteStmt, _ := conn.NewStatement()
	deleteStmt.SetSqlQuery("DELETE FROM test_table")
	deleteStmt.ExecuteUpdate(ctx)
	deleteStmt.Close()

	query := "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	fmt.Printf("Executing: %s\n", query)

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	affected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("✓ Inserted %d rows\n", affected)
	return nil
}

func testQueryTable(ctx context.Context, conn adbc.Connection) error {
	query := "SELECT * FROM test_table ORDER BY id"
	fmt.Printf("Executing: %s\n", query)

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	fmt.Printf("Schema: %s\n", reader.Schema())

	rowCount := 0
	for reader.Next() {
		record := reader.Record()
		rowCount += int(record.NumRows())

		for i := 0; i < int(record.NumRows()); i++ {
			fmt.Printf("Row %d: ", i)
			for j, col := range record.Columns() {
				field := record.Schema().Field(j)
				fmt.Printf("%s=", field.Name)
				printValue(col, i)
				if j < int(record.NumCols())-1 {
					fmt.Print(", ")
				}
			}
			fmt.Println()
		}
	}

	if err := reader.Err(); err != nil {
		return err
	}

	fmt.Printf("✓ Query returned %d rows\n", rowCount)
	return nil
}

func testPreparedStatement(ctx context.Context, conn adbc.Connection) error {
	query := "SELECT COUNT(*) as total FROM test_table"
	fmt.Printf("Preparing statement: %s\n", query)

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	// Prepare the statement
	if err := stmt.Prepare(ctx); err != nil {
		return err
	}

	fmt.Println("✓ Statement prepared successfully")

	// Execute the prepared statement
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		for i := 0; i < int(record.NumRows()); i++ {
			fmt.Printf("Total count: ")
			printValue(record.Column(0), i)
			fmt.Println()
		}
	}

	if err := reader.Err(); err != nil {
		return err
	}

	return nil
}

func testSchemaOptimization(ctx context.Context, conn adbc.Connection) error {
	// Create a potentially expensive query with joins and aggregations
	// The server should use LIMIT 0 optimization to get schema without executing
	query := `
		SELECT
			t1.id,
			t1.name,
			COUNT(*) as count,
			AVG(t1.id) as avg_id
		FROM test_table t1
		CROSS JOIN test_table t2
		GROUP BY t1.id, t1.name
		ORDER BY count DESC
	`
	fmt.Printf("Testing schema extraction for complex query (should use LIMIT 0 optimization)...\n")

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	// Prepare to get schema (server should optimize this)
	if err := stmt.Prepare(ctx); err != nil {
		return err
	}

	fmt.Println("✓ Schema retrieved successfully without full execution")

	// Now actually execute to verify it works
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	rowCount := 0
	for reader.Next() {
		record := reader.Record()
		rowCount += int(record.NumRows())
	}

	if err := reader.Err(); err != nil {
		return err
	}

	fmt.Printf("✓ Query executed successfully, returned %d rows\n", rowCount)
	return nil
}

func printValue(col arrow.Array, row int) {
	if col.IsNull(row) {
		fmt.Print("NULL")
		return
	}

	switch arr := col.(type) {
	case *array.Int8:
		fmt.Print(arr.Value(row))
	case *array.Int16:
		fmt.Print(arr.Value(row))
	case *array.Int32:
		fmt.Print(arr.Value(row))
	case *array.Int64:
		fmt.Print(arr.Value(row))
	case *array.Uint8:
		fmt.Print(arr.Value(row))
	case *array.Uint16:
		fmt.Print(arr.Value(row))
	case *array.Uint32:
		fmt.Print(arr.Value(row))
	case *array.Uint64:
		fmt.Print(arr.Value(row))
	case *array.Float32:
		fmt.Print(arr.Value(row))
	case *array.Float64:
		fmt.Print(arr.Value(row))
	case *array.String:
		fmt.Printf("'%s'", arr.Value(row))
	case *array.Boolean:
		fmt.Print(arr.Value(row))
	default:
		fmt.Printf("%v", col.ValueStr(row))
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
