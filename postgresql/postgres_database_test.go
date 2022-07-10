// Copyright 2022. Motty Cohen
//
// Integration tests of Postgresql object database implementations
//
package postgresql

import (
	"fmt"
	"github.com/mottyc/yaaf-common-docker/docker"
	"github.com/mottyc/yaaf-common/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// PostgresqlTestSuite creates a Postgres container with data for a suite of database tests and release it when done
type PostgresqlTestSuite struct {
	suite.Suite
	containerID string
	dockerCLI   *docker.DockerClient
}

func TestDbTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresqlTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *PostgresqlTestSuite) SetupSuite() {

	// Create docker client
	var err error
	s.dockerCLI, err = docker.NewDockerClient()
	if err != nil {
		panic(any(err))
	}

	s.containerID, err = s.dockerCLI.CreateContainer("postgres:14").
		Name("test-postgresql-14").
		Port("5432", "5432").
		Var("POSTGRES_USER", "root").
		Var("POSTGRES_PASSWORD", "password").
		Var("POSTGRES_DB", "test_db").
		Label("env", "test").
		Run()
	assert.Nil(s.T(), err)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *PostgresqlTestSuite) TearDownSuite() {
	if len(s.containerID) > 0 {
		_ = s.dockerCLI.RemoveContainer(s.containerID)
	}
}

// createSUT creates the system-under-test which is postgreSQL implementation of IDatabase
func (s *PostgresqlTestSuite) createSUT() database.IDatabase {

	sut, err := NewPostgresStore("postgres://root:password@localhost:5432/test_db")
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
	if affected, err := sut.BulkInsert(list_of_heroes); err != nil {
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
}
