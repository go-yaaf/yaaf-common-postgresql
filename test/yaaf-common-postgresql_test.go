package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGroupQuery(t *testing.T) {
	skipCI(t)

	dbURI := fmt.Sprintf("postgres://user:pwd@host:5432/postgres")
	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	field := "type"
	out, total, er := db.Query(NewDevice).GroupCount(field)
	require.NoError(t, er)
	fmt.Println(total)
	for k, v := range out {
		fmt.Println(k, "->", v)
	}
	fmt.Println("Done")
}

func TestInQueryFilter(t *testing.T) {
	skipCI(t)

	dbURI := fmt.Sprintf("postgres://user:pwd@host:5432/postgres")

	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	accountId := "demo"
	//streamIds := []string{"demo-1", "demo-2"}
	enabled := []bool{true}

	//streamIds

	if streams, _, er := db.Query(NewStream).
		MatchAll(
			database.Filter("accountId").Eq(accountId),
			//database.Filter("id").In(streamIds).If(len(streamIds) > 0),
			database.Filter("enabled").In(enabled).If(len(enabled) > 0),
		).Find(); er != nil {
		fmt.Println("error", er)
	} else {
		for _, stream := range streams {
			fmt.Println(stream.ID())
		}
	}
	fmt.Println("Done")
}
