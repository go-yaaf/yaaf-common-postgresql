package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
)

func TestEmptyOperator(t *testing.T) {
	skipCI(t)

	dbURI := fmt.Sprintf("postgres://user:pwd@host:5432/postgres")
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