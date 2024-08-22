package main

import (
	"context"
	"strings"
)

type SqlParser interface {
	ParsePersonTableStatement(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error)
	ParseDriversLicenseTableStatement(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error)
	ParseVehicleRegistrationTableStatement(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error)
	ParseVehicleTableStatement(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error)
}

type partiqlParser struct{}

var _ SqlParser = &partiqlParser{}

func NewPartiQlSqlParser() *partiqlParser {
	return &partiqlParser{}
}

func (p *partiqlParser) parsePersonDeleteStmt(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error) {
	return &personParsedPartiQl{
		st:   SqlStatementTypeDelete,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parsePersonUpdateStmt(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error) {
	return &personParsedPartiQl{
		st:   SqlStatementTypeUpdate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parsePersonInsertStmt(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error) {
	return &personParsedPartiQl{
		st:   SqlStatementTypeInsert,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parsePersonCreateStmt(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error) {
	return &personParsedPartiQl{
		st:   SqlStatementTypeCreate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parsePersonSelectStmt(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error) {
	return &personParsedPartiQl{
		st:   SqlStatementTypeSelect,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseDriversLicenseDeleteStmt(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error) {
	return &driversLicenseParsedPartiQl{
		st:   SqlStatementTypeDelete,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseDriversLicenseUpdateStmt(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error) {
	return &driversLicenseParsedPartiQl{
		st:   SqlStatementTypeUpdate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseDriversLicenseInsertStmt(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error) {
	return &driversLicenseParsedPartiQl{
		st:   SqlStatementTypeInsert,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseDriversLicenseCreateStmt(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error) {
	return &driversLicenseParsedPartiQl{
		st:   SqlStatementTypeCreate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseDriversLicenseSelectStmt(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error) {
	return &driversLicenseParsedPartiQl{
		st:   SqlStatementTypeSelect,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleRegistrationDeleteStmt(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error) {
	return &vehicleRegistrationParsedPartiQl{
		st:   SqlStatementTypeDelete,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleRegistrationUpdateStmt(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error) {
	return &vehicleRegistrationParsedPartiQl{
		st:   SqlStatementTypeUpdate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleRegistrationInsertStmt(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error) {
	return &vehicleRegistrationParsedPartiQl{
		st:   SqlStatementTypeInsert,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleRegistrationCreateStmt(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error) {
	return &vehicleRegistrationParsedPartiQl{
		st:   SqlStatementTypeCreate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleRegistrationSelectStmt(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error) {
	return &vehicleRegistrationParsedPartiQl{
		st:   SqlStatementTypeSelect,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleDeleteStmt(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error) {
	return &vehicleParsedPartiQl{
		st:   SqlStatementTypeDelete,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleUpdateStmt(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error) {
	return &vehicleParsedPartiQl{
		st:   SqlStatementTypeUpdate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleInsertStmt(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error) {
	return &vehicleParsedPartiQl{
		st:   SqlStatementTypeInsert,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleCreateStmt(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error) {
	return &vehicleParsedPartiQl{
		st:   SqlStatementTypeCreate,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) parseVehicleSelectStmt(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error) {
	return &vehicleParsedPartiQl{
		st:   SqlStatementTypeSelect,
		stmt: stmt,
		revs: revs,
	}, nil
}

func (p *partiqlParser) ParsePersonTableStatement(ctx context.Context, stmt string, revs []PersonRevision) (DoltParsedPartiQl, error) {
	lowered := strings.TrimSpace(strings.ToLower(stmt))
	if strings.HasPrefix(lowered, selectStatementPrefix) {
		return p.parsePersonSelectStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, createStatementPrefix) {
		return p.parsePersonCreateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, updateStatementPrefix) {
		return p.parsePersonUpdateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, deleteStatementPrefix) {
		return p.parsePersonDeleteStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, insertStatementPrefix) {
		return p.parsePersonInsertStmt(ctx, stmt, revs)
	}
	return &personParsedPartiQl{}, nil
}

func (p *partiqlParser) ParseDriversLicenseTableStatement(ctx context.Context, stmt string, revs []DriversLicenseRevision) (DoltParsedPartiQl, error) {
	lowered := strings.TrimSpace(strings.ToLower(stmt))
	if strings.HasPrefix(lowered, selectStatementPrefix) {
		return p.parseDriversLicenseSelectStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, createStatementPrefix) {
		return p.parseDriversLicenseCreateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, updateStatementPrefix) {
		return p.parseDriversLicenseUpdateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, deleteStatementPrefix) {
		return p.parseDriversLicenseDeleteStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, insertStatementPrefix) {
		return p.parseDriversLicenseInsertStmt(ctx, stmt, revs)
	}
	return &personParsedPartiQl{}, nil
}

func (p *partiqlParser) ParseVehicleRegistrationTableStatement(ctx context.Context, stmt string, revs []VehicleRegistrationRevision) (DoltParsedPartiQl, error) {
	lowered := strings.TrimSpace(strings.ToLower(stmt))
	if strings.HasPrefix(lowered, selectStatementPrefix) {
		return p.parseVehicleRegistrationSelectStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, createStatementPrefix) {
		return p.parseVehicleRegistrationCreateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, updateStatementPrefix) {
		return p.parseVehicleRegistrationUpdateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, deleteStatementPrefix) {
		return p.parseVehicleRegistrationDeleteStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, insertStatementPrefix) {
		return p.parseVehicleRegistrationInsertStmt(ctx, stmt, revs)
	}
	return &personParsedPartiQl{}, nil
}

func (p *partiqlParser) ParseVehicleTableStatement(ctx context.Context, stmt string, revs []VehicleRevision) (DoltParsedPartiQl, error) {
	lowered := strings.TrimSpace(strings.ToLower(stmt))
	if strings.HasPrefix(lowered, selectStatementPrefix) {
		return p.parseVehicleSelectStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, createStatementPrefix) {
		return p.parseVehicleCreateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, updateStatementPrefix) {
		return p.parseVehicleUpdateStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, deleteStatementPrefix) {
		return p.parseVehicleDeleteStmt(ctx, stmt, revs)
	}
	if strings.HasPrefix(lowered, insertStatementPrefix) {
		return p.parseVehicleInsertStmt(ctx, stmt, revs)
	}
	return &personParsedPartiQl{}, nil
}
