package persistence

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func TestRunInTx_CommitsOnSuccess(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	db := bun.NewDB(sqlDB, pgdialect.New())

	err = RunInTx(context.Background(), db, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewRaw("SELECT 1").Exec(ctx)
		return err
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRunInTx_RollsBackOnError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	db := bun.NewDB(sqlDB, pgdialect.New())
	expectedErr := errors.New("write failed")

	err = RunInTx(context.Background(), db, func(ctx context.Context, tx bun.Tx) error {
		if _, execErr := tx.NewRaw("SELECT 1").Exec(ctx); execErr != nil {
			return execErr
		}
		return expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRunInTx_RollsBackOnPanic(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	db := bun.NewDB(sqlDB, pgdialect.New())

	assert.PanicsWithValue(t, "boom", func() {
		_ = RunInTx(context.Background(), db, func(ctx context.Context, tx bun.Tx) error {
			_, _ = tx.NewRaw("SELECT 1").Exec(ctx)
			panic("boom")
		})
	})

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRunInTx_ExistingTxAvoidsNestedBegin(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("SELECT 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	db := bun.NewDB(sqlDB, pgdialect.New())

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = RunInTx(context.Background(), tx, func(ctx context.Context, activeTx bun.Tx) error {
		_, err := activeTx.NewRaw("SELECT 1").Exec(ctx)
		return err
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRunInTx_ValidatesInput(t *testing.T) {
	err := RunInTx(context.Background(), nil, func(ctx context.Context, tx bun.Tx) error {
		return nil
	})
	require.ErrorIs(t, err, ErrTxDBNil)

	sqlDB, _, openErr := sqlmock.New()
	require.NoError(t, openErr)
	defer sqlDB.Close()
	db := bun.NewDB(sqlDB, pgdialect.New())

	err = RunInTx(context.Background(), db, nil)
	require.ErrorIs(t, err, ErrTxFuncNil)
}
