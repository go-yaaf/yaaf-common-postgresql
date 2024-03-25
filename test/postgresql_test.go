// Integration tests of Postgresql object database implementations
//

package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
	"time"
)

const (
	dbName = "test_db"
	dbUser = "root"
	dbPwd  = "password"
	dbPort = "5432"

	containerName = "test-postgresql"
)

// PostgresqlTestSuite creates a Postgres container with data for a suite of database tests and release it when done
type PostgresqlTestSuite struct {
	suite.Suite
	containerID string
}

func TestDbTestSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	suite.Run(t, new(PostgresqlTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *PostgresqlTestSuite) SetupSuite() {

	// Create command to run postgresql container
	err := utils.DockerUtils().CreateContainer("postgres:15").
		Name(containerName).
		Port("5432", dbPort).
		Var("POSTGRES_USER", dbUser).
		Var("POSTGRES_PASSWORD", dbPwd).
		Var("POSTGRES_DB", dbName).
		Label("env", "test").
		Run()

	assert.Nil(s.T(), err)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *PostgresqlTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer(containerName)
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *PostgresqlTestSuite) createSUT() database.IDatabase {

	dbURI := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s", dbUser, dbPwd, dbPort, dbName)
	sut, err := postgresql.NewPostgresDatabase(dbURI)
	if err != nil {
		panic(any(err))
	}

	if err := sut.Ping(5, 5); err != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	// Create schema
	schema := make(map[string][]string)
	schema["hero"] = []string{"name"}
	err = sut.ExecuteDDL(schema)
	assert.Nil(s.T(), err)

	// Insert test data
	if affected, err := sut.BulkUpsert(list_of_heroes); err != nil {
		panic(any(err))
	} else {
		fmt.Println(affected)
	}
	return sut
}

// TestDatabaseSet operation
func (s *PostgresqlTestSuite) TestDatabaseSet() {
	sut := s.createSUT()
	list, er := sut.List(NewHero, []string{"1", "2", "3", "4"})
	assert.Nil(s.T(), er)
	assert.Equal(s.T(), 4, len(list))

	group, total, err := sut.Query(NewHero).GroupCount("key")

	assert.Nil(s.T(), err)
	assert.NotEqual(s.T(), total, 0)
	assert.NotEqual(s.T(), len(group), 0)

}

func TestPgxConnectivity(t *testing.T) {
	uri := "postgres://postgres:$hield1I2O3T@localhost:5432/postgres?application_name\\=iprep"

	_ = os.Setenv("DB_CONNECTION_NAME", "shieldiot-staging:europe-west3:pulse-staging-postgres")
	db, err := postgresql.NewPostgresStore(uri)

	if err != nil {
		t.Fatal(err)
	}

	hero1 := list_of_heroes[0]
	hero2 := list_of_heroes[1]

	hero11 := Hero{
		BaseEntity: entity.BaseEntity{
			Id:        uuid.New().String(),
			CreatedOn: entity.Timestamp(time.Now().Unix()),
			UpdatedOn: entity.Timestamp(time.Now().Unix()),
		},
		Key:  1,
		Name: "name 1",
	}
	hero22 := Hero{
		BaseEntity: entity.BaseEntity{
			Id:        uuid.New().String(),
			CreatedOn: entity.Timestamp(time.Now().Unix()),
			UpdatedOn: entity.Timestamp(time.Now().Unix()),
		},
		Key:  2,
		Name: "name 2",
	}
	if _, err := db.Insert(hero11); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert(hero22); err != nil {
		t.Fatal(err)
	}

	var (
		total int64
		res   []entity.Entity
	)
	res, total, err = db.Query(NewHero).MatchAll(database.QueryFilter(
		database.F("id").In(hero11.ID(), hero22.ID()),
	)).Find()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(res, total)

	heros, err := db.List(NewHero, []string{hero11.ID(), hero22.ID()})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(heros[0].ID())
	_, err = db.BulkDelete(NewHero, []string{hero1.ID(), hero2.ID(), hero11.ID(), hero22.ID()})

	t.Log(db)

}
