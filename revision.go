package main

type Metadata struct {
	Id string `json:"id"`
}

type Owner struct {
	PersonId string `json:"PersonId"`
}

type Owners struct {
	PrimaryOwner    Owner   `json:"PrimaryOwner"`
	SecondaryOwners []Owner `json:"SecondaryOwners"`
}

type VehicleRegistrationData struct {
	Id                         string  `json:"Id"`
	VIN                        string  `json:"VIN"`
	LicensePlateNumber         string  `json:"LicensePlateNumber"`
	State                      string  `json:"State"`
	City                       string  `json:"City"`
	PendingPenaltyTicketAmount float64 `json:"PendingPenaltyTicketAmount"`
	ValidFromDate              string  `json:"ValidFromDate"`
	ValidToDate                string  `json:"ValidToDate"`
	Owners                     Owners  `json:"Owners"`
}

type VehicleRegistrationRevision struct {
	Data     VehicleRegistrationData `json:"data"`
	Metadata Metadata                `json:"metadata"`
}

type PersonData struct {
	Id        string `json:"Id"`
	FirstName string `json:"FirstName"`
	LastName  string `json:"LastName"`
	DOB       string `json:"DOB"`
	GovId     string `json:"GovId"`
	GovIdType string `json:"GovIdType"`
	Address   string `json:"Address"`
}

type PersonRevision struct {
	Data     PersonData `json:"data"`
	Metadata Metadata   `json:"metadata"`
}

type DriversLicenseData struct {
	Id                 string `json:"Id"`
	LicensePlateNumber string `json:"LicensePlateNumber"`
	LicenseType        string `json:"LicenseType"`
	ValidFromDate      string `json:"ValidFromDate"`
	ValidToDate        string `json:"ValidToDate"`
	PersonIdFk         string `json:"PersonId"`
}

type DriversLicenseRevision struct {
	Data     DriversLicenseData `json:"data"`
	Metadata Metadata           `json:"metadata"`
}

type VehicleData struct {
	Id          string `json:"Id"`
	VIN         string `json:"VIN"`
	VehicleType string `json:"Type"`
	Year        int    `json:"Year"`
	Make        string `json:"Make"`
	Model       string `json:"Model"`
	Color       string `json:"Color"`
}

type VehicleRevision struct {
	Data     VehicleData `json:"data"`
	Metadata Metadata    `json:"metadata"`
}

type LicensePlateNumberMappingData struct {
	LicensePlateNumber      string `json:"LicensePlateNumber"`
	VehicleRegistrationIdFk string `json:"VehicleRegistrationIdFk"`
	DriversLicenseIdFk      string `json:"DriversLicenseIdFk"`
}

type VinMappingData struct {
	VIN                     string `json:"VIN"`
	VehicleRegistrationIdFk string `json:"VehicleRegistrationIdFk"`
	VehicleIdFk             string `json:"VehicleIdFk"`
}

type VehicleOwnershipData struct {
	VehicleRegistrationIdFk string `json:"VehicleRegistrationIdFk"`
	PersonIdFk              string `json:"PersonIdFk"`
	IsPrimaryOwner          bool   `json:"IsPrimaryOwner"`
}
