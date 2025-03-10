# Week 10 Assignment: Building a Personal Movie Database with Go

## Overview

This project was created for week 10 in MSDS 431 and is focused on creating a local movie database application built using Go and SQLite. It uses IMDb data files from Northwestern University to create, populate, and query a relational database

## Project Functionality 
The application initializes the database by creating the required tables if they do not exist. It then pulls in data from the CSV files and executres a join to demonstrate the functionality. Please note that this file takes a long time to run. I have slightly modified to code to not load all the databases so it will run faster.

## Project Structure

- **main.go**: Contains the Go code that connects to the SQLite database, creates the necessary tables, populates them with sample data (from CSV files in a full implementation), and executes a sample JOIN query.
- **movies.db**: SQLite database file created by the application.
- **README.md**: Documentation detailing setup, usage, and potential enhancements.

## Setup and Installation

1. **Download Data Files:**
   - Download the IMDb archive from Northwestern University's IMDb Data Files
   - Extract the comma-delimited text files. For this project, focus on `IMDB-movies.csv`, `IMDB-actors.csv`, and`IMDB-roles.csv`.

2. **Define the Database Schema:**
   - movies table: name, year, and rank
   - actors table: id, first_name, last_name
   - roles table: id (autoincrement), actor_id, movie)id. This table links to the `movies` table via `movie_id` and to the `actors` table via `actor_id`, and includes a `role` column describing the character played.

3. **Install sqlite:**
    - ru: go get modernc.org/sqlite


4. **Running the Application:**
   - Run the Go program with: go run main.go
   - The application will create the database, create tables, populate data, and display query results

