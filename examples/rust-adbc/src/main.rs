use anyhow::{anyhow, Context, Result};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use swanlake_client::FlightSQLClient;
use tracing::info;

#[derive(Debug)]
struct Person {
    first_name: String,
    last_name: String,
    email: String,
}

#[derive(Debug)]
struct Place {
    country: String,
    city: Option<String>,
    telcode: i32,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().compact().init();

    let endpoint = "grpc://localhost:4214";
    let mut client = FlightSQLClient::connect(endpoint)?;
    info!("Connected to SwanLake successfully!");

    // Exec schema
    execute_statement(&mut client, SCHEMA)?;

    // Batch inserts with param binding (using prepared statements and Bind)
    let people_to_insert = vec![
        Person {
            first_name: "Jason".to_string(),
            last_name: "Moiron".to_string(),
            email: "jmoiron@jmoiron.net".to_string(),
        },
        Person {
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            email: "johndoeDNE@gmail.net".to_string(),
        },
        Person {
            first_name: "Jane".to_string(),
            last_name: "Citizen".to_string(),
            email: "jane.citzen@example.com".to_string(),
        },
    ];
    insert_people(&mut client, people_to_insert)?;

    let places_to_insert = vec![
        Place {
            country: "United States".to_string(),
            city: Some("New York".to_string()),
            telcode: 1i32,
        },
        Place {
            country: "Hong Kong".to_string(),
            city: None,
            telcode: 852i32,
        },
        Place {
            country: "Singapore".to_string(),
            city: None,
            telcode: 65i32,
        },
    ];
    insert_places(&mut client, places_to_insert)?;

    // Select all people
    let people = select_people(
        &mut client,
        "SELECT * FROM person ORDER BY first_name ASC",
        vec![],
    )?;
    if people.len() >= 2 {
        info!(person = ?people[0], "person row 1");
        info!(person = ?people[1], "person row 2");
    } else {
        return Err(anyhow!(
            "Expect at least 2 people, got {count}",
            count = people.len()
        ));
    }

    // Select single person (simulate QueryRow)
    let single_person = select_people(
        &mut client,
        "SELECT * FROM person WHERE first_name = ?",
        vec!["Jason".to_string()],
    )?;
    if !single_person.is_empty() {
        let jason = &single_person[0];
        info!(person = ?jason, "single person row");
    }

    // Select places
    let places = select_places(&mut client, "SELECT * FROM place ORDER BY telcode ASC")?;
    if places.len() >= 3 {
        info!(place = ?places[0], "place row 1");
        info!(place = ?places[1], "place row 2");
        info!(place = ?places[2], "place row 3");
    }

    // Iterate places
    info!("=== Iterating Through Rows ===");
    iterate_places(&mut client)?;

    // Additional batch insert
    let additional_people = vec![
        Person {
            first_name: "Bin".to_string(),
            last_name: "Smuth".to_string(),
            email: "bensmith@allblacks.nz".to_string(),
        },
        Person {
            first_name: "Ardie".to_string(),
            last_name: "Savea".to_string(),
            email: "asavea@ab.co.nz".to_string(),
        },
        Person {
            first_name: "Sonny Bill".to_string(),
            last_name: "Williams".to_string(),
            email: "sbw@ab.co.nz".to_string(),
        },
        Person {
            first_name: "Ngani".to_string(),
            last_name: "Laumape".to_string(),
            email: "nlaumape@ab.co.nz".to_string(),
        },
        Person {
            first_name: "Ardie2".to_string(),
            last_name: "Savea2".to_string(),
            email: "asavea2@ab.co.nz".to_string(),
        },
        Person {
            first_name: "Sonny Bill2".to_string(),
            last_name: "Williams2".to_string(),
            email: "sbw2@ab.co.nz".to_string(),
        },
        Person {
            first_name: "Ngani2".to_string(),
            last_name: "Laumape2".to_string(),
            email: "nlaumape2@ab.co.nz".to_string(),
        },
    ];
    insert_people(&mut client, additional_people)?;

    info!("All operations completed successfully");

    Ok(())
}

const SCHEMA: &str = r"
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
)";

fn execute_statement(client: &mut FlightSQLClient, sql: &str) -> Result<()> {
    client.update(sql)?;
    Ok(())
}

fn insert_people(client: &mut FlightSQLClient, people: Vec<Person>) -> Result<()> {
    if people.is_empty() {
        return Ok(());
    }
    // Create Arrow record for binding multiple rows
    let first_names: Vec<&str> = people.iter().map(|p| p.first_name.as_str()).collect();
    let last_names: Vec<&str> = people.iter().map(|p| p.last_name.as_str()).collect();
    let emails: Vec<&str> = people.iter().map(|p| p.email.as_str()).collect();

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(first_names)),
            std::sync::Arc::new(StringArray::from(last_names)),
            std::sync::Arc::new(StringArray::from(emails)),
        ],
    )?;
    client.update_with_record_batch(
        "INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)",
        batch,
    )?;
    Ok(())
}

fn insert_places(client: &mut FlightSQLClient, places: Vec<Place>) -> Result<()> {
    if places.is_empty() {
        return Ok(());
    }
    let countries: Vec<&str> = places.iter().map(|p| p.country.as_str()).collect();
    let cities: Vec<Option<&str>> = places.iter().map(|p| p.city.as_deref()).collect();
    let telcodes: Vec<i32> = places.iter().map(|p| p.telcode).collect();

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, true),
        Field::new("telcode", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(countries)),
            std::sync::Arc::new(StringArray::from(cities)),
            std::sync::Arc::new(Int32Array::from(telcodes)),
        ],
    )?;
    client.update_with_record_batch(
        "INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)",
        batch,
    )?;
    Ok(())
}

fn select_people(
    client: &mut FlightSQLClient,
    sql: &str,
    params: Vec<String>,
) -> Result<Vec<Person>> {
    let result = if params.is_empty() {
        client.query(sql)?
    } else {
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "param",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![std::sync::Arc::new(StringArray::from(params))])?;
        client.query_with_param(sql, batch)?
    };
    let mut people = Vec::new();
    for batch in &result.batches {
        let first_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for first_name")?;
        let last_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for last_name")?;
        let emails = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for email")?;
        for i in 0..batch.num_rows() {
            people.push(Person {
                first_name: first_names.value(i).to_string(),
                last_name: last_names.value(i).to_string(),
                email: emails.value(i).to_string(),
            });
        }
    }
    Ok(people)
}

fn select_places(client: &mut FlightSQLClient, sql: &str) -> Result<Vec<Place>> {
    let result = client.query(sql)?;
    let mut places = Vec::new();
    for batch in &result.batches {
        let countries = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for country")?;
        let cities = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for city")?;
        let telcodes = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("expected Int32Array for telcode")?;
        for i in 0..batch.num_rows() {
            places.push(Place {
                country: countries.value(i).to_string(),
                city: if cities.is_null(i) {
                    None
                } else {
                    Some(cities.value(i).to_string())
                },
                telcode: telcodes.value(i),
            });
        }
    }
    Ok(places)
}

fn iterate_places(client: &mut FlightSQLClient) -> Result<()> {
    let result = client.query("SELECT * FROM place")?;
    for batch in &result.batches {
        let countries = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for country")?;
        let cities = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("expected StringArray for city")?;
        let telcodes = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("expected Int32Array for telcode")?;
        for i in 0..batch.num_rows() {
            let place = Place {
                country: countries.value(i).to_string(),
                city: if cities.is_null(i) {
                    None
                } else {
                    Some(cities.value(i).to_string())
                },
                telcode: telcodes.value(i),
            };
            info!(place = ?place, "iterated place row");
        }
    }
    Ok(())
}
