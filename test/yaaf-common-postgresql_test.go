package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestGroupQuery(t *testing.T) {
	skipCI(t)

	dbURI := fmt.Sprintf("")
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

	dbURI := fmt.Sprintf("")

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

func TestTableName(t *testing.T) {
	skipCI(t)

	tableName := "insight-{accountId}-{YYYY}.{MM}"

	newName := getTableName(tableName)
	fmt.Println(newName)
}

func getTableName(table string, keys ...string) (tblName string) {

	tblName = table

	//if len(keys) == 0 {
	if strings.Contains(tblName, "-{") {
		idx := strings.Index(tblName, "{")
		return tblName[:idx-1]
	} else {
		return tblName
	}
}

func TestContainsQuery(t *testing.T) {
	skipCI(t)

	dbURI := fmt.Sprintf("")

	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	from := 20241208000000
	to := 20250107075327
	userId := "ke2j26cq"
	accountId := "51608267"

	result, total, er := db.Query(NewBooking).
		MatchAll(
			database.F("placementOn").Gt(from),
			database.F("placementOn").Lt(to)).
		MatchAny(
			database.F("requestedFor").Contains(userId),
		).
		Sort("placementOn").
		Limit(1000).
		Find(accountId)

	require.NoError(t, er)
	require.NotNil(t, result)
	fmt.Println(total)

	fmt.Println("Done")
}

func TestInSubQuery(t *testing.T) {
	skipCI(t)

	// dbURI := fmt.Sprintf("postgres://user:pwd@host:5432/postgres")
	dbURI := "postgres://postgres:dOn7cE1p55m63h4I@34.147.69.116:5432/bookmev2"

	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	accountId := "51608267"
	search := "אור"

	subQuery := db.Query(NewMember).Filter(database.F("accountId").Eq(accountId))
	result, total, er := db.Query(NewUser).
		Filter(database.F("name").Like(search)).
		Filter(database.F("id").InSubQuery("userId", subQuery)).
		Limit(100).
		Find(accountId)

	require.NoError(t, er)
	require.NotNil(t, result)
	fmt.Println(total)

	fmt.Println("Done")
}

func TestFlagsQuery(t *testing.T) {
	skipCI(t)

	dbURI := ""

	db, err := postgresql.NewPostgresDatabase(dbURI)
	require.NoError(t, err)

	result, total, er := db.Query(NewUser).
		Filter(database.F("roles").WithFlag(16)).
		Limit(100).
		Find("")

	require.NoError(t, er)
	require.NotNil(t, result)
	fmt.Println(total)

	fmt.Println("Done")
}
