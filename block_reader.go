package main

import (
	"encoding/json"
)

type Statement struct {
	Statement string `json:"statement"`
}

type TransactionInfo struct {
	Statements []Statement `json:"statements"`
}

type Block struct {
	TransactionId   string          `json:"transactionId"`
	TransactionInfo TransactionInfo `json:"transactionInfo"`
}

type VehicleRegistrationBlock struct {
	TransactionId   string                        `json:"transactionId"`
	TransactionInfo TransactionInfo               `json:"transactionInfo"`
	Revisions       []VehicleRegistrationRevision `json:"revisions"`
}

type PersonBlock struct {
	TransactionId   string           `json:"transactionId"`
	TransactionInfo TransactionInfo  `json:"transactionInfo"`
	Revisions       []PersonRevision `json:"revisions"`
}

type DriversLicenseBlock struct {
	TransactionId   string                   `json:"transactionId"`
	TransactionInfo TransactionInfo          `json:"transactionInfo"`
	Revisions       []DriversLicenseRevision `json:"revisions"`
}

type VehicleBlock struct {
	TransactionId   string            `json:"transactionId"`
	TransactionInfo TransactionInfo   `json:"transactionInfo"`
	Revisions       []VehicleRevision `json:"revisions"`
}

type BlockUnmarshaler struct {
	Data interface{}
}

func (u *BlockUnmarshaler) UnmarshalJSON(b []byte) error {
	vehicleRegistrationBlock := &VehicleRegistrationBlock{
		TransactionInfo: TransactionInfo{
			Statements: make([]Statement, 0),
		},
		Revisions: make([]VehicleRegistrationRevision, 0),
	}
	err := json.Unmarshal(b, vehicleRegistrationBlock)

	if err == nil {
		if len(vehicleRegistrationBlock.Revisions) > 0 {
			rev := vehicleRegistrationBlock.Revisions[0]
			if rev.Data.VIN != "" && rev.Data.LicensePlateNumber != "" {
				u.Data = vehicleRegistrationBlock
				return nil
			}
		}
	}

	if _, ok := err.(*json.UnmarshalTypeError); err != nil && !ok {
		return err
	}

	personBlock := &PersonBlock{
		TransactionInfo: TransactionInfo{
			Statements: make([]Statement, 0),
		},
		Revisions: make([]PersonRevision, 0),
	}
	err = json.Unmarshal(b, personBlock)

	if err == nil {
		if len(personBlock.Revisions) > 0 {
			rev := personBlock.Revisions[0]
			if rev.Data.GovId != "" {
				u.Data = personBlock
				return nil
			}
		}
	}

	if _, ok := err.(*json.UnmarshalTypeError); err != nil && !ok {
		return err
	}

	driversLicenseBlock := &DriversLicenseBlock{
		TransactionInfo: TransactionInfo{
			Statements: make([]Statement, 0),
		},
		Revisions: make([]DriversLicenseRevision, 0),
	}
	err = json.Unmarshal(b, driversLicenseBlock)

	if err == nil {
		if len(driversLicenseBlock.Revisions) > 0 {
			rev := driversLicenseBlock.Revisions[0]
			if rev.Data.LicensePlateNumber != "" {
				u.Data = driversLicenseBlock
				return nil
			}
		}
	}

	if _, ok := err.(*json.UnmarshalTypeError); err != nil && !ok {
		return err
	}

	vehicleBlock := &VehicleBlock{
		TransactionInfo: TransactionInfo{
			Statements: make([]Statement, 0),
		},
		Revisions: make([]VehicleRevision, 0),
	}
	err = json.Unmarshal(b, vehicleBlock)

	if err == nil {
		if len(vehicleBlock.Revisions) > 0 {
			rev := vehicleBlock.Revisions[0]
			if rev.Data.VIN != "" && rev.Data.Make != "" {
				u.Data = vehicleBlock
				return nil
			}
		}
	}

	// abort if we have an error other than the wrong type
	if _, ok := err.(*json.UnmarshalTypeError); err != nil && !ok {
		return err
	}

	block := &Block{
		TransactionInfo: TransactionInfo{
			Statements: make([]Statement, 0),
		},
	}

	err = json.Unmarshal(b, block)
	if err != nil {
		return err
	}

	u.Data = block
	return nil
}

func ReadBlock(b []byte) (interface{}, error) {
	bu := &BlockUnmarshaler{}
	err := json.Unmarshal(b, bu)
	if err != nil {
		return nil, err
	}
	return bu.Data, nil
}
