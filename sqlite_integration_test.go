package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

type integrationJSONRecord struct {
	bun.BaseModel `bun:"table:integration_json_records"`

	ID   int64           `bun:"id,pk,autoincrement"`
	Obj  JSONMap         `bun:"obj"`
	Tags JSONStringSlice `bun:"tags"`
}

type integrationValidationRun struct {
	bun.BaseModel `bun:"table:validation_runs"`

	ID         int64   `bun:"id,pk,autoincrement"`
	MerchantID string  `bun:"merchant_id,notnull"`
	Channel    string  `bun:"channel,notnull"`
	Status     string  `bun:"status,notnull"`
	Counts     JSONMap `bun:"counts"`
}

type integrationValidationIssue struct {
	bun.BaseModel `bun:"table:validation_issues"`

	ID        int64  `bun:"id,pk,autoincrement"`
	RunID     int64  `bun:"run_id,notnull"`
	Severity  string `bun:"severity,notnull"`
	IssueCode string `bun:"issue_code,notnull"`
	Message   string `bun:"message"`
	Status    string `bun:"status,notnull"`
}

type concurrencyRecord struct {
	bun.BaseModel `bun:"table:integration_concurrency_records"`

	ID     int64  `bun:"id,pk,autoincrement"`
	Worker string `bun:"worker,notnull"`
}

func TestJSONWrappers_SQLiteRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE integration_json_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			obj TEXT,
			tags TEXT
		)
	`)
	require.NoError(t, err)

	in := &integrationJSONRecord{
		Obj: JSONMap{
			"merchant_id": "merchant-1",
			"counts": map[string]any{
				"blocker": 2,
				"warning": 1,
			},
		},
		Tags: JSONStringSlice{"blocker", "warning", "pass"},
	}

	_, err = db.NewInsert().Model(in).Exec(ctx)
	require.NoError(t, err)

	var out integrationJSONRecord
	err = db.NewSelect().
		Model(&out).
		Where("id = ?", in.ID).
		Scan(ctx)
	require.NoError(t, err)
	inObjJSON, err := json.Marshal(in.Obj)
	require.NoError(t, err)
	outObjJSON, err := json.Marshal(out.Obj)
	require.NoError(t, err)
	assert.JSONEq(t, string(inObjJSON), string(outObjJSON))
	assert.Equal(t, in.Tags, out.Tags)
}

func TestRunInTx_SQLiteParentChildrenAndRollback(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()
	createValidationTables(t, db, ctx)

	var successfulRunID int64
	err := RunInTx(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		run := &integrationValidationRun{
			MerchantID: "merchant-1",
			Channel:    "shopify",
			Status:     "running",
			Counts: JSONMap{
				"blocker": 1,
				"warning": 1,
				"pass":    0,
			},
		}

		if _, err := tx.NewInsert().Model(run).Exec(ctx); err != nil {
			return err
		}
		successfulRunID = run.ID
		if successfulRunID == 0 {
			return errors.New("missing run id after insert")
		}

		issues := []*integrationValidationIssue{
			{RunID: successfulRunID, Severity: "blocker", IssueCode: "missing_tax_id", Message: "Tax ID missing", Status: "open"},
			{RunID: successfulRunID, Severity: "warning", IssueCode: "missing_phone", Message: "Phone missing", Status: "open"},
			{RunID: successfulRunID, Severity: "warning", IssueCode: "missing_logo", Message: "Logo missing", Status: "open"},
		}

		for _, issue := range issues {
			if _, err := tx.NewInsert().Model(issue).Exec(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	var runCount int
	err = db.NewSelect().
		TableExpr("validation_runs").
		ColumnExpr("COUNT(*)").
		Scan(ctx, &runCount)
	require.NoError(t, err)
	assert.Equal(t, 1, runCount)

	var issueCount int
	err = db.NewSelect().
		TableExpr("validation_issues").
		ColumnExpr("COUNT(*)").
		Scan(ctx, &issueCount)
	require.NoError(t, err)
	assert.Equal(t, 3, issueCount)

	var grouped []GroupCount
	err = NewGroupedCountQuery(db, (*integrationValidationIssue)(nil), "severity").
		Where("run_id = ?", successfulRunID).
		Scan(ctx, &grouped)
	require.NoError(t, err)
	assert.Equal(t, []GroupCount{
		{Key: "blocker", Count: 1},
		{Key: "warning", Count: 2},
	}, grouped)

	expectedErr := errors.New("force rollback")
	err = RunInTx(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		run := &integrationValidationRun{
			MerchantID: "merchant-rollback",
			Channel:    "shopify",
			Status:     "running",
		}

		if _, err := tx.NewInsert().Model(run).Exec(ctx); err != nil {
			return err
		}
		runID := run.ID
		if runID == 0 {
			return errors.New("missing run id after insert")
		}

		issue := &integrationValidationIssue{
			RunID:     runID,
			Severity:  "blocker",
			IssueCode: "fatal_error",
			Message:   "simulated failure",
			Status:    "open",
		}
		if _, err := tx.NewInsert().Model(issue).Exec(ctx); err != nil {
			return err
		}

		return expectedErr
	})
	require.ErrorIs(t, err, expectedErr)

	var rollbackRunCount int
	err = db.NewSelect().
		TableExpr("validation_runs").
		Where("merchant_id = ?", "merchant-rollback").
		ColumnExpr("COUNT(*)").
		Scan(ctx, &rollbackRunCount)
	require.NoError(t, err)
	assert.Equal(t, 0, rollbackRunCount)
}

func TestRunInTx_SQLiteConcurrencySanity(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE integration_concurrency_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			worker TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	const workers = 24
	rollbackErr := errors.New("intentional rollback")
	var committed atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := RunInTx(ctx, db, func(ctx context.Context, tx bun.Tx) error {
				record := &concurrencyRecord{
					Worker: "worker",
				}
				if _, err := tx.NewInsert().Model(record).Exec(ctx); err != nil {
					return err
				}

				if workerID%5 == 0 {
					return rollbackErr
				}
				return nil
			})

			if err != nil {
				if errors.Is(err, rollbackErr) {
					return
				}
				errCh <- err
				return
			}
			committed.Add(1)
		}()
	}

	wg.Wait()
	close(errCh)

	for goroutineErr := range errCh {
		require.NoError(t, goroutineErr)
	}

	var count int64
	err = db.NewSelect().
		TableExpr("integration_concurrency_records").
		ColumnExpr("COUNT(*)").
		Scan(ctx, &count)
	require.NoError(t, err)
	assert.Equal(t, committed.Load(), count)
}

func newSQLiteTestDB(t *testing.T) (*bun.DB, func()) {
	t.Helper()

	sqlDB, err := sql.Open(sqliteshim.ShimName, "file::memory:?cache=shared")
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)

	db := bun.NewDB(sqlDB, sqlitedialect.New())

	cleanup := func() {
		_ = db.Close()
		_ = sqlDB.Close()
	}

	return db, cleanup
}

func createValidationTables(t *testing.T, db *bun.DB, ctx context.Context) {
	t.Helper()

	_, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON")
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE validation_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			merchant_id TEXT NOT NULL,
			channel TEXT NOT NULL,
			status TEXT NOT NULL,
			counts TEXT
		)
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE validation_issues (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id INTEGER NOT NULL,
			severity TEXT NOT NULL,
			issue_code TEXT NOT NULL,
			message TEXT,
			status TEXT NOT NULL,
			FOREIGN KEY(run_id) REFERENCES validation_runs(id) ON DELETE CASCADE
		)
	`)
	require.NoError(t, err)
}
