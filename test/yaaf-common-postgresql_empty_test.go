package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/go-yaaf/yaaf-common-postgresql/postgresql"
	. "github.com/go-yaaf/yaaf-common/entity"
)

type User struct {
	BaseEntityEx
	Name   string `json:"name"`   // User name
	Email  string `json:"email"`  // User email
	Mobile string `json:"mobile"` // User mobile phone number (for notification and validation)
	Type   int    `json:"type"`   // User type: UNDEFINED | SYSADMIN | SUPPORT | USER
	Status int    `json:"status"` // User status: UNDEFINED | PENDING | ACTIVE |  BLOCKED | SUSPENDED
}

func (u *User) TABLE() string { return "user" }
func (u *User) NAME() string  { return u.Name }

// NewUser is a factory method to create new instance
func NewUser() Entity {
	return &User{BaseEntityEx: BaseEntityEx{CreatedOn: Now(), UpdatedOn: Now(), Props: make(Json)}}
}

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
