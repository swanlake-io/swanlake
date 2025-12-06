package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/jmoiron/sqlx"
)

var schema = `
use swanlake;

DROP TABLE IF EXISTS person;
DROP TABLE IF EXISTS place;

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
	// this Pings the database trying to connect
	// use sqlx.Open() for sql.Open() semantics
	db, err := sqlx.Connect("flightsql", "uri=grpc://localhost:4214")
	if err != nil {
		log.Fatalln(err)
	}

	// exec the schema or fail; multi-statement Exec behavior varies between
	// database drivers;  pq will exec them all, sqlite3 won't, ymmv
	db.MustExec(schema)

	tx := db.MustBegin()
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jason", "Moiron", "jmoiron@jmoiron.net")
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "John", "Doe", "johndoeDNE@gmail.net")
	tx.MustExec("INSERT INTO place (country, city, telcode) VALUES ($1, $2, $3)", "United States", "New York", 1)
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Hong Kong", 852)
	tx.MustExec("INSERT INTO place (country, telcode) VALUES ($1, $2)", "Singapore", 65)
	// Flight SQL only supports positional parameters ($1, $2, $3), not named parameters
	tx.MustExec("INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)", "Jane", "Citizen", "jane.citzen@example.com")
	tx.Commit()

	// Multi-row insert in a single Exec to capture the Flight SQL driver's parameter shaping.
	db.MustExec("DROP TABLE IF EXISTS multi_row_shape")
	db.MustExec("CREATE TABLE multi_row_shape (a INT, b INT)")
	if _, err := db.Exec("INSERT INTO multi_row_shape (a, b) VALUES ($1, $2), ($3, $4)", 10, 20, 30, 40); err != nil {
		log.Fatalf("Multi-row insert failed: %v", err)
	}
	type pair struct {
		A int `db:"a"`
		B int `db:"b"`
	}
	var pairs []pair
	if err := db.Select(&pairs, "SELECT a, b FROM multi_row_shape ORDER BY a"); err != nil {
		log.Fatalf("Failed to read multi_row_shape: %v", err)
	}
	if len(pairs) != 2 || pairs[0].A != 10 || pairs[0].B != 20 || pairs[1].A != 30 || pairs[1].B != 40 {
		log.Fatalf("Unexpected data in multi_row_shape: %#v", pairs)
	}

	// Query the database, storing results in a []Person (wrapped in []interface{})
	people := []Person{}
	db.Select(&people, "SELECT * FROM person ORDER BY first_name ASC")
	if len(people) != 3 {
		log.Fatalf("Expected 3 people, got %d", len(people))
	}
	jason, john := people[0], people[1]

	fmt.Printf("%#v\n%#v\n", jason, john)
	// Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}
	// Person{FirstName:"John", LastName:"Doe", Email:"johndoeDNE@gmail.net"}

	if jason.FirstName != "Jane" || jason.LastName != "Citizen" {
		log.Fatalf("Expected Jane Citizen, got %v %v", jason.FirstName, jason.LastName)
	}
	if john.FirstName != "Jason" || john.LastName != "Moiron" {
		log.Fatalf("Expected Jason Moiron, got %v %v", john.FirstName, john.LastName)
	}

	// You can also get a single result, a la QueryRow
	// Note: Using literal value instead of parameter due to ADBC driver limitation with db.Get
	jason = Person{}
	err = db.Get(&jason, "SELECT * FROM person WHERE first_name = 'Jason'")
	if err != nil {
		log.Fatalf("Failed to get Jason: %v", err)
	}
	fmt.Printf("1234 %#v\n", jason)
	// Person{FirstName:"Jason", LastName:"Moiron", Email:"jmoiron@jmoiron.net"}

	if jason.FirstName != "Jason" || jason.LastName != "Moiron" || jason.Email != "jmoiron@jmoiron.net" {
		log.Fatalf("Expected Jason Moiron with correct email, got %#v", jason)
	}

	// if you have null fields and use SELECT *, you must use sql.Null* in your struct
	places := []Place{}
	err = db.Select(&places, "SELECT * FROM place ORDER BY telcode ASC")
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(places) != 3 {
		log.Fatalf("Expected 3 places, got %d", len(places))
	}
	usa, singsing, honkers := places[0], places[1], places[2]

	fmt.Printf("%#v\n%#v\n%#v\n", usa, singsing, honkers)
	// Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
	// Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}
	// Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}

	if usa.TelCode != 1 || usa.Country != "United States" || !usa.City.Valid || usa.City.String != "New York" {
		log.Fatalf("USA data incorrect: %#v", usa)
	}
	if singsing.TelCode != 65 || singsing.Country != "Singapore" {
		log.Fatalf("Singapore data incorrect: %#v", singsing)
	}
	if honkers.TelCode != 852 || honkers.Country != "Hong Kong" {
		log.Fatalf("Hong Kong data incorrect: %#v", honkers)
	}

	// Loop through rows using only one struct
	place := Place{}
	rows, err := db.Queryx("SELECT * FROM place")
	if err != nil {
		log.Fatalf("Failed to query places: %v", err)
	}
	placeCount := 0
	for rows.Next() {
		err := rows.StructScan(&place)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("%#v\n", place)
		placeCount++
	}
	if placeCount != 3 {
		log.Fatalf("Expected to iterate 3 places, got %d", placeCount)
	}
	// Place{Country:"United States", City:sql.NullString{String:"New York", Valid:true}, TelCode:1}
	// Place{Country:"Hong Kong", City:sql.NullString{String:"", Valid:false}, TelCode:852}
	// Place{Country:"Singapore", City:sql.NullString{String:"", Valid:false}, TelCode:65}

	// Flight SQL only supports positional parameters, not named parameters
	_, err = db.Exec(`INSERT INTO person (first_name,last_name,email) VALUES ($1,$2,$3)`,
		"Bin", "Smuth", "bensmith@allblacks.nz")
	if err != nil {
		log.Fatalf("Failed to insert Bin: %v", err)
	}

	// Query using literal value - ADBC driver has issues with Queryx parameters
	rows, err = db.Queryx(`SELECT * FROM person WHERE first_name='Bin'`)
	if err != nil {
		log.Fatalf("Failed to query Bin: %v", err)
	}
	foundBin := false
	for rows.Next() {
		var p Person
		if err := rows.StructScan(&p); err != nil {
			log.Fatalf("Failed to scan Bin: %v", err)
		}
		if p.FirstName == "Bin" && p.LastName == "Smuth" {
			foundBin = true
		}
	}
	if !foundBin {
		log.Fatal("Failed to find Bin Smuth")
	}

	// Query using literal value - ADBC driver has issues with Queryx parameters
	rows, err = db.Queryx(`SELECT * FROM person WHERE first_name='Jason'`)

	// batch insert with individual inserts (Flight SQL doesn't support batch named params)
	personStructs := []Person{
		{FirstName: "Ardie", LastName: "Savea", Email: "asavea@ab.co.nz"},
		{FirstName: "Sonny Bill", LastName: "Williams", Email: "sbw@ab.co.nz"},
		{FirstName: "Ngani", LastName: "Laumape", Email: "nlaumape@ab.co.nz"},
	}

	for _, p := range personStructs {
		_, err = db.Exec(`INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)`,
			p.FirstName, p.LastName, p.Email)
		if err != nil {
			log.Fatalf("Failed batch insert with structs: %v", err)
		}
	}

	// Verify the batch insert worked
	var count int

	err = db.Get(&count, "SELECT COUNT(*) FROM person WHERE last_name = 'Savea'")
	if err != nil || count != 1 {
		log.Fatalf("Failed to verify Savea insert (after structs): count=%d, err=%v", count, err)
	}

	// batch insert with maps using positional parameters
	personMaps := []map[string]interface{}{
		{"first_name": "Ardie", "last_name": "Savea", "email": "asavea@ab.co.nz"},
		{"first_name": "Sonny Bill", "last_name": "Williams", "email": "sbw@ab.co.nz"},
		{"first_name": "Ngani", "last_name": "Laumape", "email": "nlaumape@ab.co.nz"},
	}

	for _, m := range personMaps {
		_, err = db.Exec(`INSERT INTO person (first_name, last_name, email) VALUES ($1, $2, $3)`,
			m["first_name"], m["last_name"], m["email"])
		if err != nil {
			log.Fatalf("Failed batch insert with maps: %v", err)
		}
	}

	// Verify we now have 2 Savea records (one from structs, one from maps)
	err = db.Get(&count, "SELECT COUNT(*) FROM person WHERE last_name = 'Savea'")
	if err != nil || count != 2 {
		log.Fatalf("Failed to verify final Savea count: count=%d, err=%v", count, err)
	}

	// Verify all inserts - should have 10 total people now
	// 3 (tx: Jason, John, Jane) + 1 (Bin) + 3 (structs) + 3 (maps duplicates) = 10 total
	err = db.Get(&count, "SELECT COUNT(*) FROM person")
	if err != nil {
		log.Fatalf("Failed to count people: %v", err)
	}
	if count != 10 {
		log.Fatalf("Expected 10 people total (including duplicates), got %d", count)
	}

	fmt.Println("✅ All assertions passed!")
}
