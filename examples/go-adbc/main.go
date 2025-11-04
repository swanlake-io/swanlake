package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

var schema = `
CREATE TABLE IF NOT EXISTS person (
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR
);

CREATE TABLE IF NOT EXISTS place (
    country VARCHAR,
    city VARCHAR NULL,
    telcode INTEGER
)`

type Person struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string
}

type Place struct {
	Country string
	City    sql.NullString
	TelCode int
}

func main() {
	ctx := context.Background()

	// Connect to SwanDB
	conn, err := connect(ctx, "grpc://localhost:4214")
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	fmt.Println("Connected to SwanDB successfully!")

	// Exec schema
	if err := executeStatement(ctx, conn, schema); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	// Transaction handled via Commit/Rollback

	// Inserts with param binding (using prepared statements and Bind)
	if err := insertPerson(ctx, conn, "Jason", "Moiron", "jmoiron@jmoiron.net"); err != nil {
		conn.Rollback(ctx)
		log.Fatalf("Failed to insert: %v", err)
	}
	if err := insertPerson(ctx, conn, "John", "Doe", "johndoeDNE@gmail.net"); err != nil {
		conn.Rollback(ctx)
		log.Fatalf("Failed to insert: %v", err)
	}
	if err := insertPlace(ctx, conn, "United States", sql.NullString{String: "New York", Valid: true}, 1); err != nil {
		conn.Rollback(ctx)
		log.Fatalf("Failed to insert: %v", err)
	}
	if err := insertPlace(ctx, conn, "Hong Kong", sql.NullString{Valid: false}, 852); err != nil {
		conn.Rollback(ctx)
		log.Fatalf("Failed to insert: %v", err)
	}
	if err := insertPlace(ctx, conn, "Singapore", sql.NullString{Valid: false}, 65); err != nil {
		conn.Rollback(ctx)
		log.Fatalf("Failed to insert: %v", err)
	}

	// Named insert with struct (simulate named binding)
	jane := Person{FirstName: "Jane", LastName: "Citizen", Email: "jane.citzen@example.com"}
	if err := insertPersonNamed(ctx, conn, jane); err != nil {
		conn.Rollback(ctx)
		log.Fatalf("Failed to insert: %v", err)
	}

	// Commit
	// if err := conn.Commit(ctx); err != nil {
	// 	log.Fatalf("Failed to commit: %v", err)
	// }

	// Autocommit assumed default

	// Select all people
	people, err := selectPeople(ctx, conn, "SELECT * FROM swandb.person ORDER BY first_name ASC")
	if err != nil {
		log.Fatalf("Failed to query people: %v", err)
	}
	if len(people) >= 2 {
		jason, john := people[0], people[1]
		fmt.Printf("%#v\n%#v\n", jason, john)
	} else {
		log.Fatalf("Expect at least 2 people, got %d", len(people))
	}

	// Select single person (simulate QueryRow)
	singlePerson, err := selectPeople(ctx, conn, "SELECT * FROM person WHERE first_name = ?", "Jason")
	if err != nil {
		log.Fatalf("Failed to get person: %v", err)
	}
	var jason Person
	if len(singlePerson) > 0 {
		jason = singlePerson[0]
		fmt.Printf("%#v\n", jason)
	}

	// Select places
	places, err := selectPlaces(ctx, conn, "SELECT * FROM place ORDER BY telcode ASC")
	if err != nil {
		log.Fatalf("Failed to query places: %v", err)
	}
	if len(places) >= 3 {
		usa, singsing, honkers := places[0], places[1], places[2]
		fmt.Printf("%#v\n%#v\n%#v\n", usa, singsing, honkers)
	}

	// Iterate places
	fmt.Println("\n=== Iterating Through Rows ===")
	if err := iteratePlaces(ctx, conn); err != nil {
		log.Fatalln(err)
	}

	// Named insert (simulate :name)
	if err := executeNamed(ctx, conn, "INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)",
		map[string]interface{}{"first": "Bin", "last": "Smuth", "email": "bensmith@allblacks.nz"}); err != nil {
		log.Fatalf("Failed to insert: %v", err)
	}

	// Named select
	bins, err := selectPeopleNamed(ctx, conn, "SELECT * FROM person WHERE first_name = :fn", map[string]interface{}{"fn": "Bin"})
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	if len(bins) > 0 {
		fmt.Printf("Found: %#v\n", bins[0])
	}

	// Named select with struct
	jasons, err := selectPeopleNamed(ctx, conn, "SELECT * FROM person WHERE first_name = :first_name", jason)
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	if len(jasons) > 0 {
		fmt.Printf("Found Jason: %#v\n", jasons[0])
	}

	// Batch insert with structs (simulate named)
	personStructs := []Person{
		{FirstName: "Ardie", LastName: "Savea", Email: "asavea@ab.co.nz"},
		{FirstName: "Sonny Bill", LastName: "Williams", Email: "sbw@ab.co.nz"},
		{FirstName: "Ngani", LastName: "Laumape", Email: "nlaumape@ab.co.nz"},
	}
	for _, p := range personStructs {
		if err := insertPersonNamed(ctx, conn, p); err != nil {
			log.Fatalf("Failed to insert: %v", err)
		}
	}

	// Batch insert with maps
	personMaps := []map[string]interface{}{
		{"first_name": "Ardie2", "last_name": "Savea2", "email": "asavea2@ab.co.nz"},
		{"first_name": "Sonny Bill2", "last_name": "Williams2", "email": "sbw2@ab.co.nz"},
		{"first_name": "Ngani2", "last_name": "Laumape2", "email": "nlaumape2@ab.co.nz"},
	}
	for _, m := range personMaps {
		if err := executeNamed(ctx, conn, "INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", m); err != nil {
			log.Fatalf("Failed to insert: %v", err)
		}
	}

	fmt.Println("\n✅ All operations completed successfully!")
}

func connect(ctx context.Context, endpoint string) (adbc.Connection, error) {
	drv := flightsql.NewDriver(nil)
	db, err := drv.NewDatabase(map[string]string{
		adbc.OptionKeyURI:             endpoint,
		flightsql.OptionSSLSkipVerify: adbc.OptionValueEnabled,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// if opts, ok := conn.(adbc.PostInitOptions); ok {
	// 	if err := opts.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
	// 		conn.Close()
	// 		return nil, fmt.Errorf("failed to disable autocommit: %w", err)
	// 	}
	// }

	return conn, nil
}

func executeStatement(ctx context.Context, conn adbc.Connection, sql string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func insertPerson(ctx context.Context, conn adbc.Connection, first, last, email string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)"); err != nil {
		return err
	}

	if err := stmt.Prepare(ctx); err != nil {
		return err
	}

	// Create Arrow record for binding
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "first_name", Type: arrow.BinaryTypes.String},
		{Name: "last_name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendString(first)
	builder.Field(1).(*array.StringBuilder).AppendString(last)
	builder.Field(2).(*array.StringBuilder).AppendString(email)

	record := builder.NewRecord()
	defer record.Release()

	if err := stmt.Bind(ctx, record); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func insertPlace(ctx context.Context, conn adbc.Connection, country string, city sql.NullString, telcode int) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	sql := "INSERT INTO place (country, telcode) VALUES (?, ?)"
	if city.Valid {
		sql = "INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)"
	}
	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	if err := stmt.Prepare(ctx); err != nil {
		return err
	}

	mem := memory.DefaultAllocator
	fields := []arrow.Field{
		{Name: "country", Type: arrow.BinaryTypes.String},
	}
	if city.Valid {
		fields = append(fields, arrow.Field{Name: "city", Type: arrow.BinaryTypes.String})
	}
	fields = append(fields, arrow.Field{Name: "telcode", Type: arrow.PrimitiveTypes.Int64})

	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendString(country)
	idx := 1
	if city.Valid {
		builder.Field(idx).(*array.StringBuilder).AppendString(city.String)
		idx++
	}
	builder.Field(idx).(*array.Int64Builder).Append(int64(telcode))

	record := builder.NewRecord()
	defer record.Release()

	if err := stmt.Bind(ctx, record); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func insertPersonNamed(ctx context.Context, conn adbc.Connection, p Person) error {
	return executeNamed(ctx, conn, "INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", p)
}

func executeNamed(ctx context.Context, conn adbc.Connection, sql string, params interface{}) error {
	// Simulate named binding by replacing :name with values
	replacedSQL := sql
	switch p := params.(type) {
	case map[string]interface{}:
		for k, v := range p {
			replacedSQL = strings.ReplaceAll(replacedSQL, ":"+k, fmt.Sprintf("'%v'", v))
		}
	case Person:
		replacedSQL = strings.ReplaceAll(replacedSQL, ":first_name", fmt.Sprintf("'%s'", p.FirstName))
		replacedSQL = strings.ReplaceAll(replacedSQL, ":last_name", fmt.Sprintf("'%s'", p.LastName))
		replacedSQL = strings.ReplaceAll(replacedSQL, ":email", fmt.Sprintf("'%s'", p.Email))
	}
	return executeStatement(ctx, conn, replacedSQL)
}

func selectPeople(ctx context.Context, conn adbc.Connection, sql string, params ...interface{}) ([]Person, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, err
	}

	if err := stmt.Prepare(ctx); err != nil {
		return nil, err
	}

	if len(params) > 0 {
		mem := memory.DefaultAllocator
		fields := make([]arrow.Field, len(params))
		for i := range params {
			fields[i] = arrow.Field{Name: fmt.Sprintf("param%d", i), Type: arrow.BinaryTypes.String}
		}
		schema := arrow.NewSchema(fields, nil)
		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()

		for i, param := range params {
			builder.Field(i).(*array.StringBuilder).AppendString(fmt.Sprintf("%v", param))
		}

		record := builder.NewRecord()
		defer record.Release()

		if err := stmt.Bind(ctx, record); err != nil {
			return nil, err
		}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	return scanPeople(reader)
}

func selectPeopleNamed(ctx context.Context, conn adbc.Connection, sql string, params interface{}) ([]Person, error) {
	var paramList []interface{}
	replacedSQL := sql
	switch p := params.(type) {
	case map[string]interface{}:
		for k, v := range p {
			replacedSQL = strings.ReplaceAll(replacedSQL, ":"+k, "?")
			paramList = append(paramList, v)
		}
	case Person:
		replacedSQL = strings.ReplaceAll(replacedSQL, ":first_name", "?")
		paramList = append(paramList, p.FirstName)
	}
	return selectPeople(ctx, conn, replacedSQL, paramList...)
}

func selectPlaces(ctx context.Context, conn adbc.Connection, sql string) ([]Place, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	return scanPlaces(reader)
}

func iteratePlaces(ctx context.Context, conn adbc.Connection) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT * FROM place"); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		places, err := recordToPlaces(record)
		if err != nil {
			return err
		}
		for _, place := range places {
			fmt.Printf("%#v\n", place)
		}
	}

	return reader.Err()
}

func scanPeople(reader array.RecordReader) ([]Person, error) {
	var people []Person

	for reader.Next() {
		record := reader.Record()
		persons, err := recordToPeople(record)
		if err != nil {
			return nil, err
		}
		people = append(people, persons...)
	}

	if err := reader.Err(); err != nil {
		return nil, err
	}

	return people, nil
}

func recordToPeople(record arrow.Record) ([]Person, error) {
	var people []Person

	firstNameCol := record.Column(0).(*array.String)
	lastNameCol := record.Column(1).(*array.String)
	emailCol := record.Column(2).(*array.String)

	for i := 0; i < int(record.NumRows()); i++ {
		people = append(people, Person{
			FirstName: firstNameCol.Value(i),
			LastName:  lastNameCol.Value(i),
			Email:     emailCol.Value(i),
		})
	}

	return people, nil
}

func scanPlaces(reader array.RecordReader) ([]Place, error) {
	var places []Place

	for reader.Next() {
		record := reader.Record()
		plcs, err := recordToPlaces(record)
		if err != nil {
			return nil, err
		}
		places = append(places, plcs...)
	}

	if err := reader.Err(); err != nil {
		return nil, err
	}

	return places, nil
}

func recordToPlaces(record arrow.Record) ([]Place, error) {
	var places []Place

	countryCol := record.Column(0).(*array.String)
	cityCol := record.Column(1).(*array.String)
	telcodeCol := record.Column(2)

	for i := 0; i < int(record.NumRows()); i++ {
		place := Place{
			Country: countryCol.Value(i),
		}

		if cityCol.IsNull(i) {
			place.City = sql.NullString{Valid: false}
		} else {
			place.City = sql.NullString{String: cityCol.Value(i), Valid: true}
		}

		switch tc := telcodeCol.(type) {
		case *array.Int32:
			place.TelCode = int(tc.Value(i))
		case *array.Int64:
			place.TelCode = int(tc.Value(i))
		default:
			return nil, fmt.Errorf("unexpected telcode type: %T", telcodeCol)
		}

		places = append(places, place)
	}

	return places, nil
}
