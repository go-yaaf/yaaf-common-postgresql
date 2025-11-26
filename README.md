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

*   Go 1.18 or higher
*   PostgreSQL database

### Installation

To add the library to your project, run the following command:

```bash
go get -v -t github.com/go-yaaf/yaaf-common-postgresql
```

### Usage

#### Connecting to the Database

To connect to a PostgreSQL database, create a new `PostgresDatabase` instance using a connection string.

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

	// Ping the database to ensure a connection is established
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

func NewHero(id, name, power string, friends []string) *Hero {
	return &Hero{
		BaseEntity: entity.NewBaseEntity(id),
		Name:       name,
		Power:      power,
		Friends:    friends,
	}
}
```

#### Basic CRUD Operations

**Create**

```go
hero := NewHero("1", "Superman", "Flight", []string{"Batman", "Wonder Woman"})
if _, err := db.Insert(hero); err != nil {
    // Handle error
}
```

**Read**

```go
factory := func() entity.Entity { return &Hero{} }
retrievedHero, err := db.Get(factory, "1")
if err != nil {
    // Handle error
}
```

**Update**

```go
heroToUpdate := retrievedHero.(*Hero)
heroToUpdate.Power = "Super Strength"
if _, err := db.Update(heroToUpdate); err != nil {
    // Handle error
}
```

**Delete**

```go
if err := db.Delete(factory, "1"); err != nil {
    // Handle error
}
```

#### Bulk Operations

**Bulk Insert**

```go
heroes := []entity.Entity{
    NewHero("2", "Batman", "Rich", []string{"Superman", "Robin"}),
    NewHero("3", "Wonder Woman", "Super Strength", []string{"Superman", "Steve Trevor"}),
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
if _, err := db.BulkDelete(factory, idsToDelete); err != nil {
    // Handle error
}
```

#### Querying

This library provides a simple query builder to perform queries on the database.

```go
// Find all heroes with the power 'Super Strength'
query := db.Query(factory).Eq("power", "Super Strength")
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

To run the tests for this library, you'll need a running PostgreSQL instance. The tests use the following connection string by default: `postgresql://user:password@localhost:5432/test_db`.

You can run the tests with the following command:

```bash
go test -v ./...
```
