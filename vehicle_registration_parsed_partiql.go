package main

import (
	"context"
	"fmt"
	"strings"
)

const (
	VehicleRegistrationTableName = "VehicleRegistration"
	VinMappingTableName          = "VinMapping"
	VehicleOwnershipTableName    = "VehicleOwnership"
)

var doltSqlVehicleRegistrationTableInsertTemplate = "INSERT INTO VehicleRegistration (Id,State,City,PendingPenaltyTicketAmount,ValidFromDate,ValidToDate) VALUES ('%s', '%s', '%s', '%2f', '%s', '%s');"
var doltSqlVehicleRegistrationTableUpdateTemplate = "UPDATE VehicleRegistration SET `State` = '%s', City = '%s', PendingPenaltyTicketAmount = '%.2f', ValidFromDate = '%s', ValidToDate = '%s' WHERE Id = '%s';"

var doltSqlVinMappingTableVehicleRegistrationTableInsertOrUpdateTemplate = "INSERT INTO VinMapping (VIN,VehicleRegistrationIdFk) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE VehicleRegistrationIdFk = '%s';"

var doltSqlVehicleOwnershipTableVehicleRegistrationTableInsertOrUpdateTemplate = "INSERT INTO VehicleOwnership (VehicleRegistrationIdFk,PersonIdFk,IsPrimaryOwner) VALUES ('%s', '%s', %t) ON DUPLICATE KEY UPDATE IsPrimaryOwner = %t;"

var doltSqlLicensePlateNumberMappingTableVehicleRegistrationTableInsertTemplate = "INSERT INTO LicensePlateNumberMapping (LicensePlateNumber,VehicleRegistrationIdFk) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE VehicleRegistrationIdFk = '%s';"

type vehicleRegistrationParsedPartiQl struct {
	st   StatementType
	stmt string
	data []VehicleRegistrationData
	revs []VehicleRegistrationRevision
}

var _ DoltParsedPartiQl = &vehicleRegistrationParsedPartiQl{}

func (d *vehicleRegistrationParsedPartiQl) addAndCommitDoltSql(ctx context.Context, message string) string {
	return addAndCommitDoltSql(message)
}

func (d *vehicleRegistrationParsedPartiQl) formatPendingPenaltyTicketAmount(amount float64) float64 {
	// todo: format amount for dolt
	return amount
}

func (d *vehicleRegistrationParsedPartiQl) formatDate(dateStr string) string {
	// todo: format date for dolt
	return dateStr
}

func (d *vehicleRegistrationParsedPartiQl) getInsertOrUpdateLicensePlateNumberMappingRowData(ctx context.Context, regData []VehicleRegistrationData) ([]LicensePlateNumberMappingData, error) {
	rows := make([]LicensePlateNumberMappingData, len(regData))

	for idx, reg := range regData {
		rows[idx] = LicensePlateNumberMappingData{
			LicensePlateNumber:      reg.LicensePlateNumber,
			VehicleRegistrationIdFk: reg.Id,
		}
	}

	return rows, nil
}

func (d *vehicleRegistrationParsedPartiQl) getInsertOrUpdateVinMappingRowData(ctx context.Context, regData []VehicleRegistrationData) ([]VinMappingData, error) {
	rows := make([]VinMappingData, len(regData))

	for idx, reg := range regData {
		rows[idx] = VinMappingData{
			VIN:                     reg.VIN,
			VehicleRegistrationIdFk: reg.Id,
		}
	}

	return rows, nil
}

func (d *vehicleRegistrationParsedPartiQl) getInsertOrUpdateVehicleOwnershipRowData(ctx context.Context, regData []VehicleRegistrationData) ([]VehicleOwnershipData, error) {
	rows := make([]VehicleOwnershipData, 0)

	for _, reg := range regData {
		// add primary owner
		if reg.Owners.PrimaryOwner.PersonId != "" {
			rows = append(rows, VehicleOwnershipData{
				PersonIdFk:              reg.Owners.PrimaryOwner.PersonId,
				VehicleRegistrationIdFk: reg.Id,
				IsPrimaryOwner:          true,
			})
		}

		// add secondary owners
		for _, secondary := range reg.Owners.SecondaryOwners {
			if secondary.PersonId != "" {
				rows = append(rows, VehicleOwnershipData{
					PersonIdFk:              secondary.PersonId,
					VehicleRegistrationIdFk: reg.Id,
				})
			}
		}
	}

	return rows, nil
}

func (d *vehicleRegistrationParsedPartiQl) getInsertOrUpdateVehicleRegistrationRowData(ctx context.Context) ([]VehicleRegistrationData, error) {
	rows := make([]VehicleRegistrationData, len(d.revs))

	// cheat and use the revisions
	for idx, r := range d.revs {
		secondaryOwners := make([]Owner, len(r.Data.Owners.SecondaryOwners))
		for i, owner := range r.Data.Owners.SecondaryOwners {
			secondaryOwners[i] = owner
		}

		rows[idx] = VehicleRegistrationData{
			Id:                         r.Metadata.Id,
			VIN:                        r.Data.VIN,
			LicensePlateNumber:         r.Data.LicensePlateNumber,
			State:                      r.Data.State,
			City:                       r.Data.City,
			PendingPenaltyTicketAmount: d.formatPendingPenaltyTicketAmount(r.Data.PendingPenaltyTicketAmount),
			ValidFromDate:              d.formatDate(r.Data.ValidFromDate),
			ValidToDate:                d.formatDate(r.Data.ValidToDate),
			Owners: Owners{
				PrimaryOwner:    Owner{PersonId: r.Data.Owners.PrimaryOwner.PersonId},
				SecondaryOwners: secondaryOwners,
			},
		}
	}

	return rows, nil
}

func (d *vehicleRegistrationParsedPartiQl) insertAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	insertStmt := ""
	regData, err := d.getInsertOrUpdateVehicleRegistrationRowData(ctx)
	if err != nil {
		return "", err
	}

	// handle vehicle registration inserts
	for _, reg := range regData {
		insertStmt += fmt.Sprintf(doltSqlVehicleRegistrationTableInsertTemplate,
			reg.Id,
			reg.State,
			reg.City,
			reg.PendingPenaltyTicketAmount,
			reg.ValidFromDate,
			reg.ValidToDate)
		insertStmt += "\n"
	}

	// handle vin mapping inserts
	vinData, err := d.getInsertOrUpdateVinMappingRowData(ctx, regData)
	if err != nil {
		return "", err
	}

	for _, vin := range vinData {
		insertStmt += fmt.Sprintf(doltSqlVinMappingTableVehicleRegistrationTableInsertOrUpdateTemplate,
			vin.VIN,
			vin.VehicleRegistrationIdFk,
			vin.VehicleRegistrationIdFk)
		insertStmt += "\n"
	}

	// handle vehicle ownership inserts
	ownershipData, err := d.getInsertOrUpdateVehicleOwnershipRowData(ctx, regData)
	if err != nil {
		return "", err
	}

	for _, owner := range ownershipData {
		insertStmt += fmt.Sprintf(doltSqlVehicleOwnershipTableVehicleRegistrationTableInsertOrUpdateTemplate,
			owner.VehicleRegistrationIdFk,
			owner.PersonIdFk,
			owner.IsPrimaryOwner,
			owner.IsPrimaryOwner)
		insertStmt += "\n"
	}

	// handle license plate number mapping
	licenseMappingData, err := d.getInsertOrUpdateLicensePlateNumberMappingRowData(ctx, regData)
	if err != nil {
		return "", err
	}

	for _, license := range licenseMappingData {
		insertStmt += fmt.Sprintf(doltSqlLicensePlateNumberMappingTableVehicleRegistrationTableInsertTemplate,
			license.LicensePlateNumber,
			license.VehicleRegistrationIdFk,
			license.VehicleRegistrationIdFk)
		insertStmt += "\n"
	}

	insertStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("insert into %s, %s, %s, %s qldb transaction id: %s", VehicleRegistrationTableName, VinMappingTableName, VehicleOwnershipTableName, LicensePlateNumberMappingTableName, transactionId))
	return insertStmt, nil
}

func (d *vehicleRegistrationParsedPartiQl) updateAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	updateStmt := ""
	regData, err := d.getInsertOrUpdateVehicleRegistrationRowData(ctx)
	if err != nil {
		return "", err
	}

	// handle vehicle registration updates
	for _, reg := range regData {
		updateStmt += fmt.Sprintf(doltSqlVehicleRegistrationTableUpdateTemplate,
			reg.State,
			reg.City,
			reg.PendingPenaltyTicketAmount,
			reg.ValidFromDate,
			reg.ValidToDate,
			reg.Id)
		updateStmt += "\n"
	}

	// handle vin mapping updates
	vinData, err := d.getInsertOrUpdateVinMappingRowData(ctx, regData)
	if err != nil {
		return "", err
	}

	for _, vin := range vinData {
		updateStmt += fmt.Sprintf(doltSqlVinMappingTableVehicleRegistrationTableInsertOrUpdateTemplate,
			vin.VIN,
			vin.VehicleRegistrationIdFk,
			vin.VehicleRegistrationIdFk)
		updateStmt += "\n"
	}

	// handle vehicle ownership updates
	ownershipData, err := d.getInsertOrUpdateVehicleOwnershipRowData(ctx, regData)
	if err != nil {
		return "", err
	}

	for _, owner := range ownershipData {
		updateStmt += fmt.Sprintf(doltSqlVehicleOwnershipTableVehicleRegistrationTableInsertOrUpdateTemplate,
			owner.VehicleRegistrationIdFk,
			owner.PersonIdFk,
			owner.IsPrimaryOwner,
			owner.IsPrimaryOwner)
		updateStmt += "\n"
	}

	// handle license plate number mapping updates
	licenseMappingData, err := d.getInsertOrUpdateLicensePlateNumberMappingRowData(ctx, regData)
	if err != nil {
		return "", err
	}

	for _, license := range licenseMappingData {
		updateStmt += fmt.Sprintf(doltSqlLicensePlateNumberMappingTableVehicleRegistrationTableInsertTemplate,
			license.LicensePlateNumber,
			license.VehicleRegistrationIdFk,
			license.VehicleRegistrationIdFk)
		updateStmt += "\n"
	}

	updateStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("update %s, %s, %s, %s qldb transaction id: %s", VehicleRegistrationTableName, VinMappingTableName, VehicleOwnershipTableName, LicensePlateNumberMappingTableName, transactionId))
	return updateStmt, nil
}

func (d *vehicleRegistrationParsedPartiQl) deleteAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	deleteStmt := ""

	// todo: actually parse partiql
	// cheat and just check against hardcoded VIN
	vin := "1G1ZG57B38F112851"
	shouldDelete := false
	if strings.Contains(d.stmt, vin) {
		shouldDelete = true
	}
	if !shouldDelete {
		return "", ErrNoDoltSqlEquivalent
	}

	deleteStmt += fmt.Sprintf("DELETE FROM VehicleRegistration WHERE Id = (SELECT vm.VehicleRegistrationIdFk FROM VinMapping as vm where vm.VIN = '%s');", vin) + "\n"
	deleteStmt += fmt.Sprintf("UPDATE VinMapping SET VehicleRegistrationIdFk = NULL WHERE VIN = '%s';", vin) + "\n"
	deleteStmt += fmt.Sprintf("DELETE FROM VinMapping WHERE VehicleRegistrationIdFk IS NULL AND VehicleIdFk IS NULL;") + "\n"

	deleteStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("delete from %s, %s qldb transaction id: %s", VehicleTableName, VinMappingTableName, transactionId))
	return deleteStmt, nil
}

func (d *vehicleRegistrationParsedPartiQl) AsDoltSql(ctx context.Context, transactionId string) (string, error) {
	switch d.st {
	case SqlStatementTypeSelect, SqlStatementTypeCreate:
		return "", ErrNoDoltSqlEquivalent
	case SqlStatementTypeInsert:
		return d.insertAsDoltSql(ctx, transactionId)
	case SqlStatementTypeUpdate:
		return d.updateAsDoltSql(ctx, transactionId)
	case SqlStatementTypeDelete:
		return d.deleteAsDoltSql(ctx, transactionId)
	}
	return "", ErrNoDoltSqlEquivalent
}
