package persistence

import (
	"context"
	"errors"

	"github.com/uptrace/bun"
)

var (
	// ErrTxDBNil indicates RunInTx was called with a nil database handle.
	ErrTxDBNil = errors.New("persistence: transaction db handle is nil")
	// ErrTxFuncNil indicates RunInTx was called with a nil callback.
	ErrTxFuncNil = errors.New("persistence: transaction callback is nil")
)

// RunInTx executes fn in a transaction.
//
// When db is an existing bun.Tx (or *bun.Tx), fn is executed directly without
// starting a nested transaction/savepoint.
// Otherwise, a new transaction is started and committed on success, and rolled
// back on error or panic.
func RunInTx(ctx context.Context, db bun.IDB, fn func(ctx context.Context, tx bun.Tx) error) (err error) {
	if db == nil {
		return ErrTxDBNil
	}
	if fn == nil {
		return ErrTxFuncNil
	}

	switch typed := db.(type) {
	case bun.Tx:
		return fn(ctx, typed)
	case *bun.Tx:
		if typed == nil {
			return ErrTxDBNil
		}
		return fn(ctx, *typed)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			panic(recovered)
		}
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	committed = true
	return nil
}
