package main

import (
	"database/sql"
	"log"
	"testing"

	_ "modernc.org/sqlite"
)

func test_query_count(db *sql.DB) int {
	query := `
		SELECT COUNT(*)
		FROM roles r
		JOIN movies m ON r.movie_id = m.id
		JOIN actors a ON r.actor_id = a.id;
	`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatalf("Error executing count query: %v", err)
	}
	return count
}

func TestQueryMovies(t *testing.T) {
	// Open the database
	db, err := sql.Open("sqlite", "movies.db")
	if err != nil {
		t.Fatalf("Error opening db: %v", err)
	}
	defer db.Close()

	// Get total number of movies
	var movieCount int
	err = db.QueryRow("SELECT COUNT(*) FROM movies").Scan(&movieCount)
	if err != nil {
		t.Fatalf("Error querying movies: %v", err)
	}

	// Check to see if there are movies in the DB
	if movieCount == 0 {
		t.Errorf("no movies found")
	} else {
		t.Logf("Movies count: %d", movieCount)
	}

	// Get and log the count from the join query
	joinCount := test_query_count(db)
	t.Logf("Join query returned %d rows", joinCount)
}
