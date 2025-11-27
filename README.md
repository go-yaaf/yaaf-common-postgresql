# GO-YAAF Postgres DB Middleware
![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-redis/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-redis/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-redis/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-redis?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-redis)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-redis)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-redis?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-redis)
![License](https://img.shields.io/dub/l/vibe-d.svg)

## Overview

Postgresql object database implementation of `yaaf-common` IDatabase interface

## About
This library is the Postgresql concrete implementation of the ORM layer defined in the `IDatabase` interface in `yaaf-common` library.
This implementation refers to the object database concepts only, the underlying database includes table per domain model entity,
each table has only two fields: `id` (of type string) and `data` (of type jsonb).
Domain model entities are stored as Json documents (which are indexed by json keys) hence the database is used like a document storage (similar to Elasticsearch, MongoDB, Couchbase and more)

This library is built around the [Pure Go Postgres driver for database/sql](https://github.com/lib/pq)

## Features

*   Connect to PostgresSQL database with a simple connection string.
*   Full implementation of `yaaf-common` IDatabase and IDatastore interfaces.
*   CRUD and Bulk operations for entities.
*   A simple query language to query the database.
*   DDL operations to create/drop tables and indexes.
*   Support for Cloud SQL Proxy.
*   Pub/Sub mechanism to publish entity changes.

## Getting Started

### Prerequisites

*   Go 1.23 or higher
*   Postgres SQL database

### Installation

To add the library to your project, run the following command:

```bash
go get -v -t github.com/go-yaaf/yaaf-common-postgresql
```

### Usage

#### Connecting to the Database

To connect to a Postgres SQL database, create a new `PostgresDatabase` instance using a connection string.

```go
package main

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
)

func main() {
	// Connection string format: postgresql://user:password@host:port/database_name
	uri := "postgresql://user:password@localhost:5432/test_db"
	db, err := postgresql.NewPostgresDatabase(uri)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Ping the database to ensure a connection is established (5 retries, 2 seconds interval)
	if err := db.Ping(5, 2); err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected to the database!")
}
```

#### Defining an Entity

Entities are structs that implement the `yaaf-common.Entity` interface.

```go
package main

import "github.com/go-yaaf/yaaf-common/entity"

type Hero struct {
	entity.BaseEntity
	Name    string `json:"name"`
	Power   string `json:"power"`
	Friends []string `json:"friends"`
}

func (h *Hero) TABLE() string { return "heroes" }

// NewHero is a factory method to create new instance
func NewHero() entity.Entity {
	return &Hero{
		BaseEntity: entity.NewBaseEntity(),
		Name:       "",
		Power:      "",
		Friends:    make([]string, 0),
	}
}
// NewHeroWithParams is a factory method to create new instance with initial parameters
func NewHeroWithParams(id, name, power string, friends []string) entity.Entity {
	hero := NewHero()
	hero.(*Hero).Id = id
	hero.(*Hero).Name = name
	hero.(*Hero).Power = power
	hero.(*Hero).Friends = friends
	return hero
}
```

#### Basic CRUD Operations

**Create**

```go
hero := NewHeroWithParams("1", "Superman", "Flight", []string{"Batman", "Wonder Woman"})
if _, err := db.Insert(hero); err != nil {
    // Handle error
}
```

**Read**

```go
retrievedHero, err := db.Get(NewHero, "1")
if err != nil {
    // Handle error
}
```

**Update**

```go
retrievedHero, err := db.Get(NewHero, "1")
if err != nil {
    // Handle error
	return
}

// Change attributes and update the database
retrievedHero.(*Hero).Name = "New Superman"
retrievedHero.(*Hero).Power = "Super Strength"
if updated, err := db.Update(retrievedHero); err != nil {
    // Handle error
} else {
	fmt.Println("hero updated", updated.ID(), updated.NAME())

}
```

**Delete**

```go
if err := db.Delete(NewHero, "1"); err != nil {
    // Handle error
}
```

#### Bulk Operations

**Bulk Insert**

```go
heroes := []entity.Entity{
    NewHeroWithParams("2", "Batman", "Rich", []string{"Superman", "Robin"}),
    NewHeroWithParams("3", "Wonder Woman", "Super Strength", []string{"Superman", "Steve Trevor"}),
}
if _, err := db.BulkInsert(heroes); err != nil {
    // Handle error
}
```

**Bulk Update**

```go
// All entities must be of the same type
if _, err := db.BulkUpdate(heroesToUpdate); err != nil {
    // Handle error
}
```

**Bulk Delete**

```go
idsToDelete := []string{"2", "3"}
if _, err := db.BulkDelete(NewHero, idsToDelete); err != nil {
    // Handle error
}
```

#### Querying

This library provides a simple query builder to perform queries on the database.

```go
// Find all heroes with the power 'Super Strength'
query := db.Query(NewHero).Eq(database.F("power").Eq("Super Strength"))
results, err := query.Find()
if err != nil {
    // Handle error
}

for _, item := range results {
    hero := item.(*Hero)
    fmt.Printf("Found hero: %s\n", hero.Name)
}
```

#### DDL Operations

**Create Table and Indexes**

```go

// This DDL create a table name: heroes and index the json fields: "name" and "power"
ddl := map[string][]string{
    "heroes": {"name", "power"},
}
if err := db.ExecuteDDL(ddl); err != nil {
    // Handle error
}
```

**Drop Table**

```go
if err := db.DropTable("heroes"); err != nil {
    // Handle error
}
```

## Running Tests

To run the tests for this library, you'll need a running Postgres SQL instance. The tests use the following connection string by default: `postgresql://user:password@localhost:5432/test_db`.

You can run the tests with the following command:

```bash
go test -v ./...
```
