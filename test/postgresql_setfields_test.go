// Integration tests of Postgresql object database implementations
//

package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	"github.com/go-yaaf/yaaf-common/database"
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
