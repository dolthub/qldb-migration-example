package main

import (
	"context"
	"fmt"
)

const (
	DriversLicenseTableName            = "DriversLicense"
	LicensePlateNumberMappingTableName = "LicensePlateNumberMapping"
)

type driversLicenseParsedPartiQl struct {
	st   StatementType
	stmt string
	data []DriversLicenseData
	revs []DriversLicenseRevision
}

var _ DoltParsedPartiQl = &driversLicenseParsedPartiQl{}

var doltSqlDriversLicenseTableInsertTemplate = "INSERT INTO DriversLicense (Id,LicenseType,ValidFromDate,ValidToDate,PersonIdFk) VALUES ('%s', '%s', '%s', '%s', '%s');"
var doltSqlDriversLicenseTableUpdateTemplate = "UPDATE DriversLicense SET LicenseType = '%s', ValidFromDate = '%s', ValidToDate = '%s', PersonIdFk = '%s' WHERE Id = '%s';"

var doltSqlLicensePlateNumberMappingTableDriversLicenseTableInsertTemplate = "INSERT INTO LicensePlateNumberMapping (LicensePlateNumber,DriversLicenseIdFk) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE DriversLicenseIdFk = '%s';"

func (d *driversLicenseParsedPartiQl) addAndCommitDoltSql(ctx context.Context, message string) string {
	return addAndCommitDoltSql(message)
}

func (d *driversLicenseParsedPartiQl) formatDate(dateStr string) string {
	// todo: format date for dolt
	return dateStr
}

func (d *driversLicenseParsedPartiQl) getInsertDriversLicenseRowData(ctx context.Context) ([]DriversLicenseData, error) {
	rows := make([]DriversLicenseData, len(d.revs))

	// cheat and use the revisions
	for idx, r := range d.revs {
		rows[idx] = DriversLicenseData{
			Id:                 r.Metadata.Id,
			LicensePlateNumber: r.Data.LicensePlateNumber,
			LicenseType:        r.Data.LicenseType,
			ValidFromDate:      r.Data.ValidFromDate,
			ValidToDate:        r.Data.ValidToDate,
			PersonIdFk:         r.Data.PersonIdFk,
		}
	}

	return rows, nil
}

func (d *driversLicenseParsedPartiQl) getInsertLicensePlateNumberMappingRowData(ctx context.Context, licenseData []DriversLicenseData) ([]LicensePlateNumberMappingData, error) {
	rows := make([]LicensePlateNumberMappingData, len(licenseData))

	for idx, license := range licenseData {
		rows[idx] = LicensePlateNumberMappingData{
			LicensePlateNumber: license.LicensePlateNumber,
			DriversLicenseIdFk: license.Id,
		}
	}

	return rows, nil
}

func (d *driversLicenseParsedPartiQl) insertAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	insertStmt := ""
	driversLicenseData, err := d.getInsertDriversLicenseRowData(ctx)
	if err != nil {
		return "", err
	}

	// handle drivers license inserts
	for _, license := range driversLicenseData {
		insertStmt += fmt.Sprintf(doltSqlDriversLicenseTableInsertTemplate,
			license.Id,
			license.LicenseType,
			d.formatDate(license.ValidFromDate),
			d.formatDate(license.ValidToDate),
			license.PersonIdFk,
		)
		insertStmt += "\n"
	}

	// handle license plate mapping inserts
	licensePlateMappingData, err := d.getInsertLicensePlateNumberMappingRowData(ctx, driversLicenseData)
	if err != nil {
		return "", err
	}

	for _, license := range licensePlateMappingData {
		insertStmt += fmt.Sprintf(doltSqlLicensePlateNumberMappingTableDriversLicenseTableInsertTemplate,
			license.LicensePlateNumber,
			license.DriversLicenseIdFk,
			license.DriversLicenseIdFk)
		insertStmt += "\n"
	}

	insertStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("insert into %s, %s qldb transaction id: %s", DriversLicenseTableName, LicensePlateNumberMappingTableName, transactionId))
	return insertStmt, nil
}

func (d *driversLicenseParsedPartiQl) updateAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	// todo: handle updates
	return "", ErrNoDoltSqlEquivalent
}

func (d *driversLicenseParsedPartiQl) deleteAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	// todo: handle deletes
	return "", ErrNoDoltSqlEquivalent
}

func (d *driversLicenseParsedPartiQl) AsDoltSql(ctx context.Context, transactionId string) (string, error) {
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
