package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecuteQuery(t *testing.T) {
	skipCI(t)

	dbURI := fmt.Sprintf("postgres://postgres:passw0rd@localhost:5432/postgres")
	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	// sql := "select data->>'name' as name, count(*) as cnt from features_group group by name"
	sql := "select data->>'name', count(*) from features_group group by data->>'name'"

	list, err := db.ExecuteQuery(sql)
	require.NoError(t, err)

	fmt.Println(list)
}
