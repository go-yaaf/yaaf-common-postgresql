// Data model and testing data

package test

import (
	"fmt"
	. "github.com/go-yaaf/yaaf-common/entity"
)

// region Heroes Test Model --------------------------------------------------------------------------------------------
type Hero struct {
	BaseEntity
	Key  int    `json:"key"`  // Key
	Name string `json:"name"` // Name
}

func (a Hero) TABLE() string { return "hero" }
func (a Hero) NAME() string  { return a.Name }

func NewHero() Entity {
	return &Hero{}
}

func NewHero1(id string, key int, name string) Entity {
	return &Hero{
		BaseEntity: BaseEntity{Id: id, CreatedOn: Now() + Timestamp(key), UpdatedOn: Now()},
		Key:        key,
		Name:       name,
	}
}

func (a Hero) String() string {
	return fmt.Sprintf("%d [%s] key:%d, name: %s", a.CreatedOn, a.Id, a.Key, a.Name)
}

var list_of_heroes = []Entity{
	NewHero1("1", 1, "Ant man"),
	NewHero1("2", 2, "Aqua man"),
	NewHero1("3", 3, "Asterix"),
	NewHero1("4", 4, "Bat Girl"),
	NewHero1("5", 5, "Bat Man"),
	NewHero1("6", 6, "Bat Woman"),
	NewHero1("7", 7, "Black Canary"),
	NewHero1("8", 8, "Black Panther"),
	NewHero1("9", 9, "Captain America"),
	NewHero1("10", 10, "Captain Marvel"),
	NewHero1("11", 11, "Cat Woman"),
	NewHero1("12", 12, "Conan the Barbarian"),
	NewHero1("13", 13, "Daredevil"),
	NewHero1("14", 14, "Doctor Strange"),
	NewHero1("15", 15, "Elektra"),
	NewHero1("16", 16, "Ghost Rider"),
	NewHero1("17", 17, "Green Arrow"),
	NewHero1("18", 18, "Green Lantern"),
	NewHero1("19", 19, "Hawkeye"),
	NewHero1("20", 20, "Hellboy"),
	NewHero1("21", 21, "Iron Man"),
	NewHero1("22", 22, "Robin"),
	NewHero1("23", 23, "Spider Man"),
	NewHero1("24", 24, "Supergirl"),
	NewHero1("25", 25, "Superman"),
	NewHero1("26", 26, "Thor"),
	NewHero1("27", 27, "The Wasp"),
	NewHero1("28", 28, "Wolverine"),
	NewHero1("29", 29, "Wonder Woman"),
	NewHero1("30", 30, "X-Man"),
}

// endregion

// region Device Test Model --------------------------------------------------------------------------------------------

type Device struct {
	BaseEntity
}

func (u *Device) TABLE() string { return "device" }
func NewDevice() Entity         { return &Device{} }

// endregion

// region Stream Test Model --------------------------------------------------------------------------------------------

type Stream struct {
	BaseEntity
}

func (u *Stream) TABLE() string { return "stream" }
func NewStream() Entity         { return &Stream{} }

// endregion

// region Booking Test Model --------------------------------------------------------------------------------------------

type Booking struct {
	BaseEntityEx
	AccountId    string    `json:"accountId"`    // Account ID
	PlacementOn  Timestamp `json:"placementOn"`  // When to ask for placement [Epoch milliseconds Timestamp]
	RequestedFor []string  `json:"requestedFor"` // User Ids registered for the placement

}

func (a Booking) TABLE() string { return "booking_{key}" }
func (a Booking) KEY() string   { return a.AccountId }
func (a Booking) NAME() string  { return a.Id }

func NewBooking() Entity { return &Booking{} }

// endregion

// region User Test Model --------------------------------------------------------------------------------------------

type User struct {
	BaseEntityEx
	Name   string   `json:"name"`   // User name
	Email  string   `json:"email"`  // User email
	Mobile string   `json:"mobile"` // User mobile phone number (for notification and validation)
	Type   int      `json:"type"`   // User type: UNDEFINED | SYSADMIN | SUPPORT | USER
	Roles  int      `json:"roles"`  // User roles flags
	Groups []string `json:"groups"` // User permissions groups
	Status int      `json:"status"` // User status: UNDEFINED | PENDING | ACTIVE |  BLOCKED | SUSPENDED
}

func (u *User) TABLE() string { return "user" }
func (u *User) NAME() string  { return u.Name }
func (u *User) KEY() string   { return u.Id }

// NewUser is a factory method to create new instance
func NewUser() Entity {
	return &User{BaseEntityEx: BaseEntityEx{CreatedOn: Now(), UpdatedOn: Now(), Props: make(Json)}, Groups: make([]string, 0)}
}

// endregion

// region Member Test Model --------------------------------------------------------------------------------------------

type Member struct {
	BaseEntityEx
	UserId      string    `json:"userId"`      // User Id
	AccountId   string    `json:"accountId"`   // Account Id
	AccountRole int       `json:"accountRole"` // Account role: UNDEFINED | ADMIN | STAFF | MEMBER | TEAM | PARA | GUEST
	Status      int       `json:"status"`      // Member status: UNDEFINED | PENDING | ACTIVE | FROZEN
	MemberSince Timestamp `json:"memberSince"` // Member in the club since [Epoch milliseconds Timestamp]
	Expiration  Timestamp `json:"expiration"`  // Membership expiration [Epoch milliseconds Timestamp]
}

func (u *Member) TABLE() string { return "member" }
func (u *Member) ID() string    { return fmt.Sprintf("%s@%s", u.UserId, u.AccountId) }
func (u *Member) KEY() string   { return u.AccountId }
func (u *Member) NAME() string  { return u.UserId }

func (u *Member) GetId() string { return fmt.Sprintf("%s@%s", u.UserId, u.AccountId) }

// NewMember factory method to create new instance
func NewMember() Entity {
	return &Member{BaseEntityEx: BaseEntityEx{CreatedOn: Now(), UpdatedOn: Now(), Props: make(Json)}}
}

// endregion
