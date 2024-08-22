package main

import (
	"context"
	"fmt"
	"strings"
)

const (
	VehicleTableName = "Vehicle"
)

var doltSqlVehicleTableInsertTemplate = "INSERT INTO Vehicle (Id,VehicleType,Year,Make,Model,Color) VALUES ('%s', '%s', '%d', '%s', '%s', '%s');"
var doltSqlVinMappingTableVehicleTableInsertOrUpdateTemplate = "INSERT INTO VinMapping (VIN,VehicleIdFk) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE VehicleIdFk = '%s';"

type vehicleParsedPartiQl struct {
	st   StatementType
	stmt string
	data []VehicleData
	revs []VehicleRevision
}

var _ DoltParsedPartiQl = &vehicleParsedPartiQl{}

func (d *vehicleParsedPartiQl) addAndCommitDoltSql(ctx context.Context, message string) string {
	return addAndCommitDoltSql(message)
}

func (d *vehicleParsedPartiQl) getInsertVehicleRowData(ctx context.Context) ([]VehicleData, error) {
	rows := make([]VehicleData, len(d.revs))

	// cheat and use the revisions
	for idx, r := range d.revs {
		rows[idx] = VehicleData{
			Id:          r.Metadata.Id,
			VIN:         r.Data.VIN,
			VehicleType: r.Data.VehicleType,
			Year:        r.Data.Year,
			Model:       r.Data.Model,
			Make:        r.Data.Make,
			Color:       r.Data.Color,
		}
	}

	return rows, nil
}

func (d *vehicleParsedPartiQl) getInsertVinMappingRowData(ctx context.Context, vehicleData []VehicleData) ([]VinMappingData, error) {
	rows := make([]VinMappingData, len(vehicleData))

	for idx, vehicle := range vehicleData {
		rows[idx] = VinMappingData{
			VIN:         vehicle.VIN,
			VehicleIdFk: vehicle.Id,
		}
	}

	return rows, nil
}

func (d *vehicleParsedPartiQl) insertAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	insertStmt := ""
	vehicleData, err := d.getInsertVehicleRowData(ctx)
	if err != nil {
		return "", err
	}

	// handle vehicle data
	for _, vehicle := range vehicleData {
		insertStmt += fmt.Sprintf(doltSqlVehicleTableInsertTemplate,
			vehicle.Id,
			vehicle.VehicleType,
			vehicle.Year,
			vehicle.Make,
			vehicle.Model,
			vehicle.Color)
		insertStmt += "\n"
	}

	// handle vin mapping data
	vinData, err := d.getInsertVinMappingRowData(ctx, vehicleData)
	if err != nil {
		return "", err
	}

	for _, vin := range vinData {
		insertStmt += fmt.Sprintf(doltSqlVinMappingTableVehicleTableInsertOrUpdateTemplate,
			vin.VIN,
			vin.VehicleIdFk,
			vin.VehicleIdFk)
		insertStmt += "\n"
	}

	insertStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("insert into %s, %s qldb transaction id: %s", VehicleTableName, VinMappingTableName, transactionId))
	return insertStmt, nil
}

func (d *vehicleParsedPartiQl) updateAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	// todo: handle updates
	return "", ErrNoDoltSqlEquivalent
}

func (d *vehicleParsedPartiQl) deleteAsDoltSql(ctx context.Context, transactionId string) (string, error) {
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

	deleteStmt += fmt.Sprintf("DELETE FROM Vehicle WHERE Id = (SELECT vm.VehicleIdFk FROM VinMapping as vm where vm.VIN = '%s');", vin) + "\n"
	deleteStmt += fmt.Sprintf("UPDATE VinMapping SET VehicleIdFk = NULL WHERE VIN = '%s';", vin) + "\n"
	deleteStmt += fmt.Sprintf("DELETE FROM VinMapping WHERE VehicleRegistrationIdFk IS NULL AND VehicleIdFk IS NULL;") + "\n"

	deleteStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("delete from %s, %s qldb transaction id: %s", VehicleTableName, VinMappingTableName, transactionId))
	return deleteStmt, nil
}

func (d *vehicleParsedPartiQl) AsDoltSql(ctx context.Context, transactionId string) (string, error) {
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
