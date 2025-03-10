package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	_ "modernc.org/sqlite" // import sqlite
)

func main() {
	// create file for sqlite database
	db, err := sql.Open("sqlite", "movies.db")
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	//defer to close it once main function is done
	defer db.Close()

	// run create tables to create the tables
	createTables(db)

	// insert csv files into the database
	if err := data_into_movies(db, "IMDb/IMDB-movies.csv"); err != nil {
		log.Fatalf("Error populating movies: %v", err)
	}
	if err := data_into_actors(db, "IMDb/IMDB-actors.csv"); err != nil {
		log.Fatalf("Error populating actors: %v", err)
	}
	if err := data_into_roles(db, "IMDb/IMDB-roles.csv"); err != nil {
		log.Fatalf("Error populating roles: %v", err)
	}
	// run sample query to confirm everythin is working
	test_query(db)
}

// set up tables with the correct columns
func createTables(db *sql.DB) {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS movies (
			id INTEGER PRIMARY KEY,
			name TEXT,
			year INTEGER,
			rank REAL
		);`,
		`CREATE TABLE IF NOT EXISTS actors (
			id INTEGER PRIMARY KEY,
			first_name TEXT,
			last_name TEXT,
			gender TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS roles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			actor_id INTEGER,
			movie_id INTEGER,
			role TEXT,
			FOREIGN KEY(movie_id) REFERENCES movies(id),
			FOREIGN KEY(actor_id) REFERENCES actors(id)
		);`,
	}

	// create empty tables
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			log.Fatalf("Error: %v", err)
		}
	}
}

// data_into_movies reads IMDB-movies.csv and populates the movies table
func data_into_movies(db *sql.DB, filename string) error {
	//open csv files
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error %s: %v", filename, err)
	}
	defer file.Close()
	//handle formatting issues
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	// Read header
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("error  %s: %v", filename, err)
	}
	// prepare db to insert records
	stmt, err := db.Prepare("INSERT INTO movies (id, name, year, rank) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}
	defer stmt.Close()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error %s: %v", filename, err)
	}
	// insert records into the roles table
	for _, record := range records {
		if len(record) != 4 {
			continue // skip invalid record
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			return fmt.Errorf("error: %v", err)
		}
		name := record[1]
		year, err := strconv.Atoi(record[2])
		if err != nil {
			return fmt.Errorf("error: %v", err)
		}
		var rank interface{}
		if record[3] == "NULL" {
			rank = nil
		} else {
			rankParsed, err := strconv.ParseFloat(record[3], 64)
			if err != nil {
				return fmt.Errorf("error converting movie rank: %v", err)
			}
			rank = rankParsed
		}

		if _, err := stmt.Exec(id, name, year, rank); err != nil {
			return fmt.Errorf("error: %v", err)
		}
	}
	return nil
}

// data_into_actors reads IMDB-actors.csv and populates the actors table
func data_into_actors(db *sql.DB, filename string) error {
	//open csv files
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", filename, err)
	}
	defer file.Close()
	// handle formatting
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	// Read header
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("error %s: %v", filename, err)
	}
	// prepare db to insert records
	stmt, err := db.Prepare("INSERT INTO actors (id, first_name, last_name, gender) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}
	defer stmt.Close()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error %s: %v", filename, err)
	}
	// insert records into the roles table
	for _, record := range records {
		if len(record) != 4 {
			continue // skip invalid record
		}
		id, err := strconv.Atoi(record[0])
		first_name := record[1]
		last_name := record[2]
		gender := record[3]
		if err != nil {
			return fmt.Errorf("error: %v", err)
		}

		if _, err := stmt.Exec(id, first_name, last_name, gender); err != nil {
			return fmt.Errorf("error: %v", err)
		}
	}
	return nil
}

// data_into_roles reads IMDB-roles.csv and populates the roles table
func data_into_roles(db *sql.DB, filename string) error {
	//open files
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error %s: %v", filename, err)
	}
	defer file.Close()
	//handle some weird formatting in the csv file
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	// Read header
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("header error from %s: %v", filename, err)
	}
	//prep db to insert records
	stmt, err := db.Prepare("INSERT INTO roles (actor_id, movie_id, role) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}
	defer stmt.Close()

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("reading records error  %s: %v", filename, err)
	}

	// insert records into the roles table
	for _, record := range records {
		if len(record) != 3 {
			continue // skip invalid record
		}
		actor_id, err := strconv.Atoi(record[0])
		if err != nil {
			return fmt.Errorf("error: %v", err)
		}
		movieID, err := strconv.Atoi(record[1])
		if err != nil {
			return fmt.Errorf("error: %v", err)
		}
		role := record[2]

		if _, err := stmt.Exec(actor_id, movieID, role); err != nil {
			return fmt.Errorf("error: %v", err)
		}
	}
	return nil
}

// test_query joins roles, movies, and actors
func test_query(db *sql.DB) {
	fmt.Println("\ntest query")
	//create a query to understand all roles, movies, and actors
	query := `
		SELECT m.name, a.first_name, a.last_name, r.role
		FROM roles r
		JOIN movies m ON r.movie_id = m.id
		JOIN actors a ON r.actor_id = a.id
		ORDER BY m.name;
	`
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error executing JOIN: %v", err)
	}
	defer rows.Close()

	//print out all rows from the query
	for rows.Next() {
		var title, firstName, lastName, role string
		if err := rows.Scan(&title, &firstName, &lastName, &role); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		fmt.Printf("Movie: %s, Actor: %s %s, Role: %s\n", title, firstName, lastName, role)
	}
}
