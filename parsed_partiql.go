package main

import "context"

type DoltParsedPartiQl interface {
	AsDoltSql(ctx context.Context, transactionId string) (string, error)
}
