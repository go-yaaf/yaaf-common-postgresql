// Integration tests of Postgresql object database implementations
//

package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	rds "github.com/go-yaaf/yaaf-common-redis/redis"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/messaging"
	"github.com/go-yaaf/yaaf-common/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// PostgresqlTestSuite creates a Postgres container with data for a suite of database tests and release it when done
type PostgresqlWithBusTestSuite struct {
	suite.Suite
	containerID string
	processed   int32
}

func TestDbWithBusTestSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	suite.Run(t, new(PostgresqlWithBusTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *PostgresqlWithBusTestSuite) SetupSuite() {

	// Create command to run postgresql container
	err := utils.DockerUtils().CreateContainer("postgres:15").
		Name("test-database").
		Port("5432", "5432").
		Var("POSTGRES_USER", dbUser).
		Var("POSTGRES_PASSWORD", dbPwd).
		Var("POSTGRES_DB", dbName).
		Label("env", "test-database").
		Run()

	assert.Nil(s.T(), err)

	// Create command to run Redis container
	err = utils.DockerUtils().CreateContainer("redis:7").
		Name("test-redis").
		Port("6379", "6379").
		Label("env", "test-redis").
		Run()

	assert.Nil(s.T(), err)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *PostgresqlWithBusTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer("test-database")
	assert.Nil(s.T(), err)

	err = utils.DockerUtils().StopContainer("test-redis")
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *PostgresqlWithBusTestSuite) createSUT() database.IDatabase {

	busURI := fmt.Sprintf("redis://localhost:6379")
	bus := s.createMessageBus(busURI)

	dbURI := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s", dbUser, dbPwd, dbPort, dbName)
	sut, err := postgresql.NewPostgresDatabaseWithMessageBus(dbURI, bus)
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

	return sut
}

func (s *PostgresqlWithBusTestSuite) createMessageBus(uri string) messaging.IMessageBus {
	bus, err := rds.NewRedisMessageBus(uri)
	if err != nil {
		panic(any(err))
	}

	if err := bus.Ping(5, 5); err != nil {
		fmt.Println("error pinging redis", err.Error())
		panic(any(err))
	}

	s.createSubscriber(bus)

	// subscribe
	//if subName, err := bus.Subscribe(messaging.NewEntityMessage, changeLogger, "change-logger", "ENTITY"); err != nil {
	//	fmt.Println("error subscribing to bus", err.Error())
	//	panic(any(err))
	//} else {
	//	fmt.Println("Subscriber: ", subName)
	//}
	return bus
}

func (s *PostgresqlWithBusTestSuite) createSubscriber(bus messaging.IMessageBus) {
	changeLogger := func(message messaging.IMessage) bool {
		sessionId := message.SessionId()
		addressee := message.Addressee()
		fmt.Println("Got Message:", message.Topic(), message.OpCode(), addressee, sessionId, addressee)
		atomic.AddInt32(&s.processed, 1)
		return true
	}

	// subscribe
	if subName, err := bus.Subscribe(messaging.NewEntityMessage, changeLogger, "change-logger", "ENTITY*"); err != nil {
		fmt.Println("error subscribing to bus", err.Error())
		panic(any(err))
	} else {
		fmt.Println("Subscriber: ", subName)
	}
}

//func changeLogger(message messaging.IMessage) bool {
//	fmt.Println("Got Message:", message.Topic(), message.OpCode(), message.SessionId(), message.Addressee())
//	return true
//}

// TestDatabaseSet operation
func (s *PostgresqlWithBusTestSuite) TestDatabaseInsert() {
	sut := s.createSUT()

	// Insert test dat
	for _, hero := range list_of_heroes {
		if added, err := sut.Insert(hero); err != nil {
			fmt.Println("fail insert hero", err.Error())
		} else {
			fmt.Println(added.ID(), "hero added")
		}
		time.Sleep(time.Second)
	}

	fmt.Println("Total messages processed: ", s.processed)

}
