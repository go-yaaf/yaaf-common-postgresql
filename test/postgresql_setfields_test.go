// Integration tests of Postgresql object database implementations
//

package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
	"time"
)

// PostgresqlTestSuite creates a Postgres container with data for a suite of database tests and release it when done
type PostgresqlSetFieldsTestSuite struct {
	suite.Suite
	containerID string
}

func TestPostgresqlSetFieldsTestSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	suite.Run(t, new(PostgresqlSetFieldsTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *PostgresqlSetFieldsTestSuite) SetupSuite() {

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
func (s *PostgresqlSetFieldsTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer(containerName)
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *PostgresqlSetFieldsTestSuite) createSUT() database.IDatabase {

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

// TestDatabaseSetString operation
func (s *PostgresqlSetFieldsTestSuite) TestDatabaseSetString() {
	sut := s.createSUT()

	// Print list before update
	fmt.Println("BEFORE")
	s.printList(sut)

	// Create changeset to change names
	cs := make(map[string]any)
	cs["1"] = "1111111"
	cs["2"] = "2222222"
	cs["3"] = "3333333"
	cs["4"] = "4444444"
	cs["5"] = "5555555"
	cs["6"] = "6666666"
	cs["7"] = "7777777"
	cs["8"] = "8888888"
	cs["9"] = "9999999"

	if affected, err := sut.BulkSetFields(NewHero, "name", cs); err != nil {
		fmt.Println("error", err.Error())
		return
	} else {
		fmt.Println()
		fmt.Println("AFTER:", affected)
	}
	// Print list after update

	s.printList(sut)
}

// TestDatabaseSetNumber operation
func (s *PostgresqlSetFieldsTestSuite) TestDatabaseSetNumber() {
	sut := s.createSUT()

	// Print list before update
	fmt.Println("BEFORE")
	s.printList(sut)

	// Create changeset to change names
	cs := make(map[string]any)
	cs["1"] = entity.Now()
	cs["2"] = entity.Now()
	cs["3"] = entity.Now()
	cs["4"] = entity.Now()
	cs["5"] = entity.Now()
	cs["6"] = entity.Now()
	cs["7"] = entity.Now()
	cs["8"] = entity.Now()
	cs["9"] = entity.Now()

	if affected, err := sut.BulkSetFields(NewHero, "createdOn", cs); err != nil {
		fmt.Println("error", err.Error())
		return
	} else {
		fmt.Println()
		fmt.Println("AFTER:", affected)
	}
	// Print list after update

	s.printList(sut)
}

// Print list of heroes
func (s *PostgresqlSetFieldsTestSuite) printList(db database.IDatabase) {

	cb := func(in entity.Entity) (out entity.Entity) {
		fmt.Println(in)
		return in
	}
	// Print list of heroes
	_, _, _ = db.Query(NewHero).Apply(cb).Find()
}
