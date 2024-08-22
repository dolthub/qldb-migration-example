package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	selectStatementPrefix      = "select"
	createStatementPrefix      = "create"
	createTableStatementPrefix = createStatementPrefix + " table"
	createIndexStatementPrefix = createStatementPrefix + " index"
	deleteStatementPrefix      = "delete from"
	insertStatementPrefix      = "insert into"
	updateStatementPrefix      = "update"
)

var ErrEncounteredUnknownBlockType = errors.New("encountered unknown block type")

type ReplayWriter interface {
	WriteReplay(ctx context.Context) error
}

type qldbToDoltReplayWriter struct {
	localDataKeys []string
	outFile       string
	parser        SqlParser
}

var _ ReplayWriter = &qldbToDoltReplayWriter{}

func NewQldbToDoltReplayWriter(localDataKeys []string, outFile string) *qldbToDoltReplayWriter {
	return &qldbToDoltReplayWriter{
		localDataKeys: localDataKeys,
		outFile:       outFile,
		parser:        NewPartiQlSqlParser(),
	}
}

func (m *qldbToDoltReplayWriter) appendToOutfile(ctx context.Context, statement string) error {
	f, err := os.OpenFile(m.outFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(statement)
	return err
}

func (m *qldbToDoltReplayWriter) translateVehicleBlockForDoltSqlReplay(ctx context.Context, block *VehicleBlock) error {
	stmt, err := m.doltSqlVehicleTableStatementBuilder(ctx, block.TransactionId, block)
	if err != nil {
		return err
	}
	return m.appendToOutfile(ctx, stmt)
}

func (m *qldbToDoltReplayWriter) translateDriversLicenseBlockForDoltSqlReplay(ctx context.Context, block *DriversLicenseBlock) error {
	stmt, err := m.doltSqlDriversLicenseTableStatementBuilder(ctx, block.TransactionId, block)
	if err != nil {
		return err
	}
	return m.appendToOutfile(ctx, stmt)
}

func (m *qldbToDoltReplayWriter) translatePersonBlockForDoltSqlReplay(ctx context.Context, block *PersonBlock) error {
	stmt, err := m.doltSqlPersonTableStatementBuilder(ctx, block.TransactionId, block)
	if err != nil {
		return err
	}
	return m.appendToOutfile(ctx, stmt)
}

func (m *qldbToDoltReplayWriter) translateVehicleRegistrationBlockForDoltSqlReplay(ctx context.Context, block *VehicleRegistrationBlock) error {
	stmt, err := m.doltSqlVehicleRegistrationTableStatementBuilder(ctx, block.TransactionId, block)
	if err != nil {
		return err
	}
	return m.appendToOutfile(ctx, stmt)
}

func (m *qldbToDoltReplayWriter) doltSqlVehicleTableStatementBuilder(ctx context.Context, transactionId string, block *VehicleBlock) (string, error) {
	doltStmt := ""

	for _, stmt := range block.TransactionInfo.Statements {
		ps, err := m.parser.ParseVehicleTableStatement(ctx, stmt.Statement, block.Revisions)
		if err != nil {
			return "", err
		}
		doltSql, err := ps.AsDoltSql(ctx, transactionId)
		if err == nil {
			doltStmt += doltSql
			doltStmt += "\n"
			continue
		}
		if errors.Is(err, ErrNoDoltSqlEquivalent) {
			continue
		} else {
			return "", err
		}
	}

	return doltStmt, nil
}

func (m *qldbToDoltReplayWriter) doltSqlPersonTableStatementBuilder(ctx context.Context, transactionId string, block *PersonBlock) (string, error) {
	doltStmt := ""

	for _, stmt := range block.TransactionInfo.Statements {
		ps, err := m.parser.ParsePersonTableStatement(ctx, stmt.Statement, block.Revisions)
		if err != nil {
			return "", err
		}
		doltSql, err := ps.AsDoltSql(ctx, transactionId)
		if err == nil {
			doltStmt += doltSql
			doltStmt += "\n"
			continue
		}
		if errors.Is(err, ErrNoDoltSqlEquivalent) {
			continue
		} else {
			return "", err
		}
	}

	return doltStmt, nil
}

func (m *qldbToDoltReplayWriter) doltSqlVehicleRegistrationTableStatementBuilder(ctx context.Context, transactionId string, block *VehicleRegistrationBlock) (string, error) {
	doltStmt := ""

	for _, stmt := range block.TransactionInfo.Statements {
		ps, err := m.parser.ParseVehicleRegistrationTableStatement(ctx, stmt.Statement, block.Revisions)
		if err != nil {
			return "", err
		}
		doltSql, err := ps.AsDoltSql(ctx, transactionId)
		if err == nil {
			doltStmt += doltSql
			doltStmt += "\n"
			continue
		}
		if errors.Is(err, ErrNoDoltSqlEquivalent) {
			continue
		} else {
			return "", err
		}
	}

	return doltStmt, nil
}

func (m *qldbToDoltReplayWriter) doltSqlDriversLicenseTableStatementBuilder(ctx context.Context, transactionId string, block *DriversLicenseBlock) (string, error) {
	doltStmt := ""

	for _, stmt := range block.TransactionInfo.Statements {
		ps, err := m.parser.ParseDriversLicenseTableStatement(ctx, stmt.Statement, block.Revisions)
		if err != nil {
			return "", err
		}
		doltSql, err := ps.AsDoltSql(ctx, transactionId)
		if err == nil {
			doltStmt += doltSql
			doltStmt += "\n"
			continue
		}
		if errors.Is(err, ErrNoDoltSqlEquivalent) {
			continue
		} else {
			return "", err
		}
	}

	return doltStmt, nil
}

func (m *qldbToDoltReplayWriter) getTableNameFromDeleteStatement(ctx context.Context, s string) string {
	table := strings.TrimSpace(s)
	table = table[len(deleteStatementPrefix)+1:]
	end := strings.Index(table, " ")
	return strings.TrimSpace(table[0:end])
}

func (m *qldbToDoltReplayWriter) translateBlockToDoltSqlReplay(ctx context.Context, b []byte) error {
	unknown, err := ReadBlock(b)
	if err != nil {
		return err
	}

	switch t := unknown.(type) {
	case *VehicleRegistrationBlock:
		return m.translateVehicleRegistrationBlockForDoltSqlReplay(ctx, t)
	case *DriversLicenseBlock:
		return m.translateDriversLicenseBlockForDoltSqlReplay(ctx, t)
	case *PersonBlock:
		return m.translatePersonBlockForDoltSqlReplay(ctx, t)
	case *VehicleBlock:
		return m.translateVehicleBlockForDoltSqlReplay(ctx, t)
	case *Block:
		for _, stmt := range t.TransactionInfo.Statements {
			lowered := strings.TrimSpace(strings.ToLower(stmt.Statement))

			if strings.HasPrefix(lowered, deleteStatementPrefix) {
				table := m.getTableNameFromDeleteStatement(ctx, stmt.Statement)

				// handle tables that support deletes
				switch table {
				case VehicleTableName:
					return m.translateVehicleBlockForDoltSqlReplay(ctx, &VehicleBlock{
						TransactionId:   t.TransactionId,
						TransactionInfo: t.TransactionInfo,
					})
				case VehicleRegistrationTableName:
					return m.translateVehicleRegistrationBlockForDoltSqlReplay(ctx, &VehicleRegistrationBlock{
						TransactionId:   t.TransactionId,
						TransactionInfo: t.TransactionInfo,
					})
				case PersonTableName:
					return m.translatePersonBlockForDoltSqlReplay(ctx, &PersonBlock{
						TransactionId:   t.TransactionId,
						TransactionInfo: t.TransactionInfo,
					})
				}
			}

			// skip blocks we dont want to replay
			if !strings.HasPrefix(lowered, selectStatementPrefix) &&
				!strings.HasPrefix(lowered, createTableStatementPrefix) &&
				!strings.HasPrefix(lowered, createIndexStatementPrefix) {
				return fmt.Errorf("%w: with statement: %s", ErrEncounteredUnknownBlockType, stmt.Statement)
			}
		}
		return nil
	default:
		return ErrEncounteredUnknownBlockType
	}
}

func (m *qldbToDoltReplayWriter) translateQldbDataFile(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		err = m.translateBlockToDoltSqlReplay(ctx, s.Bytes())
		if err != nil {
			return err
		}
	}

	return s.Err()
}

func (m *qldbToDoltReplayWriter) prepareOutfile(ctx context.Context) error {
	// remove old outfile
	err := os.RemoveAll(m.outFile)
	if err != nil {
		return err
	}

	// create new outfile
	f, err := os.Create(m.outFile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString("SET FOREIGN_KEY_CHECKS = 0;")
	if err != nil {
		return err
	}
	_, err = f.WriteString("\n")
	return err
}

func (m *qldbToDoltReplayWriter) WriteReplay(ctx context.Context) error {
	err := m.prepareOutfile(ctx)
	if err != nil {
		return err
	}
	for _, localDataKey := range m.localDataKeys {
		if err := m.translateQldbDataFile(ctx, localDataKey); err != nil {
			return err
		}
	}
	return nil
}
