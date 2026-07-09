package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"

	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
)

func TestEmptyOperator(t *testing.T) {
	skipCI(t)

	// Provide the connection string via the TEST_DB_URI environment variable, e.g.
	//   export TEST_DB_URI="postgres://user:pwd@host:5432/dbname"
	dbURI := os.Getenv("TEST_DB_URI")
	if dbURI == "" {
		t.Skip("TEST_DB_URI not set")
	}
	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	out, total, er := db.Query(NewUser).MatchAll(
		database.F("name").IsEmpty(),
		database.F("type").Eq("1"),
	).Find()
	require.NoError(t, er)
	fmt.Println(total)
	for k, v := range out {
		fmt.Println(k, "->", v)
	}
	fmt.Println("Done")
}
