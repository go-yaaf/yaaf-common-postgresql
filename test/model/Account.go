package model

import (
	. "github.com/go-yaaf/yaaf-common/entity"
)

// Account entity is a billing account in the system
// @Entity: account
type Account struct {
	BaseEntityEx
	Name          string  `json:"name"`          // Account name
	EnName        string  `json:"enName"`        // Account english name
	Type          int     `json:"type"`          // Account type:  STUDENT | PRIVATE | BUSINESS ...
	Description   string  `json:"description"`   // Account description
	Status        int     `json:"status"`        // Account status: UNDEFINED | ACTIVE | INACTIVE | BLOCKED | SUSPENDED
	Phone         string  `json:"phone"`         // Office / Landline phone
	Mobile        string  `json:"mobile"`        // Mobile phone
	Email         string  `json:"email"`         // Email address
	Address       Address `json:"address"`       // Address
	CampaignId    string  `json:"campaignId"`    // Campaign Id
	CurrentCredit float64 `json:"currentCredit"` // Current account credit
	BaseRate      float64 `json:"baseRate"`      // Base rate for billing
	Discount      int     `json:"discount"`      // Permanent discount (percentage)
	InvoiceId     string  `json:"invoiceId"`     // Green invoice Id
}

func (a *Account) TABLE() string { return "account" }
func (a *Account) NAME() string  { return a.Name }

// NewAccount is a factory method to create a new instance
func NewAccount() Entity {
	return &Account{BaseEntityEx: BaseEntityEx{CreatedOn: Now(), UpdatedOn: Now(), Id: GUID(), Props: make(Json)}, BaseRate: 1.0}
}

// Address model represents an address
// @Data
type Address struct {
	Street  string `json:"street"`  // Street address
	City    string `json:"city"`    // City
	State   string `json:"state"`   // State (if applicable)
	ZipCode string `json:"zipCode"` // Local zip code (postal cod)
	Country string `json:"country"` // Country name
}
