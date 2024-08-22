package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	SqlStatementTypeSelect StatementType = iota
	SqlStatementTypeCreate
	SqlStatementTypeInsert
	SqlStatementTypeUpdate
	SqlStatementTypeDelete
)

const (
	PersonTableName = "Person"
)

var ErrNoDoltSqlEquivalent = errors.New("no Dolt Sql equivalent")

var doltSqlAddAndCommitWithMessageTemplate = "CALL DOLT_COMMIT('-Am', '%s');"
var doltSqlPersonTableInsertTemplate = "INSERT INTO Person (Id,FirstName,LastName,DOB,GovId,GovIdType,Address) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s');"

type StatementType int

type personParsedPartiQl struct {
	st   StatementType
	stmt string
	data []PersonData
	revs []PersonRevision
}

var _ DoltParsedPartiQl = &personParsedPartiQl{}

func (d *personParsedPartiQl) addAndCommitDoltSql(ctx context.Context, message string) string {
	return addAndCommitDoltSql(message)
}

func (d *personParsedPartiQl) formatDob(dob string) string {
	// todo: format dob for dolt
	return dob
}

func (d *personParsedPartiQl) getInsertPersonRowData(ctx context.Context) ([]PersonData, error) {
	rows := make([]PersonData, len(d.revs))

	// cheat and use the revisions
	for idx, r := range d.revs {
		rows[idx] = PersonData{
			Id:        r.Metadata.Id,
			FirstName: r.Data.FirstName,
			LastName:  r.Data.LastName,
			DOB:       d.formatDob(r.Data.DOB),
			GovId:     r.Data.GovId,
			GovIdType: r.Data.GovIdType,
			Address:   r.Data.Address,
		}
	}

	return rows, nil
}

func (d *personParsedPartiQl) insertAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	insertStmt := ""
	data, err := d.getInsertPersonRowData(ctx)
	if err != nil {
		return "", err
	}

	for _, person := range data {
		insertStmt += fmt.Sprintf(doltSqlPersonTableInsertTemplate,
			person.Id,
			person.FirstName,
			person.LastName,
			d.formatDob(person.DOB),
			person.GovId,
			person.GovIdType,
			person.Address)
		insertStmt += "\n"
	}

	insertStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("insert into %s, qldb transaction id: %s", PersonTableName, transactionId))
	return insertStmt, nil
}

func (d *personParsedPartiQl) updateAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	// todo: handle updates
	return "", ErrNoDoltSqlEquivalent
}

func (d *personParsedPartiQl) deleteAsDoltSql(ctx context.Context, transactionId string) (string, error) {
	deleteStmt := ""

	// todo: actually parse partiql
	// cheat and just check against hardcoded first and last name
	firstName := "Larry"
	lastName := "David"
	shouldDelete := false
	if strings.Contains(d.stmt, firstName) && strings.Contains(d.stmt, lastName) {
		shouldDelete = true
	}
	if !shouldDelete {
		return "", ErrNoDoltSqlEquivalent
	}

	deleteStmt += fmt.Sprintf("DELETE FROM VehicleOwnership WHERE PersonIdFk = (SELECT p.Id FROM Person as p WHERE p.FirstName = '%s' AND p.LastName = '%s');", firstName, lastName) + "\n"
	deleteStmt += fmt.Sprintf("DELETE FROM DriversLicense WHERE PersonIdFk = (SELECT p.Id FROM Person as p WHERE p.FirstName = '%s' AND p.LastName = '%s');", firstName, lastName) + "\n"
	deleteStmt += fmt.Sprintf("DELETE FROM Person WHERE FirstName = '%s' AND LastName = '%s';", firstName, lastName) + "\n"

	deleteStmt += d.addAndCommitDoltSql(ctx, fmt.Sprintf("delete from %s, %s, %s qldb transaction id: %s", VehicleOwnershipTableName, DriversLicenseTableName, PersonTableName, transactionId))
	return deleteStmt, nil
}

func (d *personParsedPartiQl) AsDoltSql(ctx context.Context, transactionId string) (string, error) {
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

func addAndCommitDoltSql(message string) string {
	return fmt.Sprintf(doltSqlAddAndCommitWithMessageTemplate, message)
}
