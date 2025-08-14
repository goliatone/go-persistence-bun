package persistence

import (
	"context"
	"database/sql"

	// "fmt" is no longer needed
	"io/fs"
	"strings"
	"sync"

	apierrors "github.com/goliatone/go-errors"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

// DriverConfig remains the same
type DriverConfig interface {
	Connect(options ...bun.DBOption) (*bun.DB, *sql.DB, error)
}

// Migrations holds configuration options
// for migrations
// See https://bun.uptrace.dev/guide/migrations.html
type Migrations struct {
	mx         sync.Mutex
	Files      []fs.FS // For SQL files
	migrations *migrate.MigrationGroup
	lgr        Logger
}

func NewMigrations() *Migrations {
	m := &Migrations{
		Files: make([]fs.FS, 0),
		lgr:   &defaultLogger{},
	}
	return m
}

func (m *Migrations) SetLogger(logger Logger) {
	if logger != nil {
		m.lgr = logger
	}
}

func (m *Migrations) logger() Logger {
	if m.lgr == nil {
		return &defaultLogger{}
	}
	return m.lgr
}

// TODO: We need to make sure we run down migrations in the reverse order that
// were up.run

// TODO: We should support ordering of migrations outside of the naming convention
// for the scneario of importing migrations from a different project that might need
// to be run before others but have a naming that would put them after
func (m *Migrations) initSQLMigrations() (*migrate.Migrations, error) {
	if len(m.Files) == 0 {
		return nil, nil // Nothing to do
	}

	migrations := migrate.NewMigrations()
	for i, migrationFS := range m.Files {
		if err := migrations.Discover(migrationFS); err != nil {
			return nil, apierrors.Wrap(err,
				apierrors.CategoryInternal,
				"failed to discover filesystem migrations",
			).WithMetadata(map[string]any{"index": i})
		}
	}
	return migrations, nil
}

// RegisterSQLMigrations adds SQL based migrations
func (m *Migrations) RegisterSQLMigrations(migrations ...fs.FS) *Migrations {
	m.mx.Lock()
	m.Files = append(m.Files, migrations...)
	m.mx.Unlock()
	return m
}

// run is a helper to execute migrations for a given collection
func (m *Migrations) run(ctx context.Context, db *bun.DB, migrations *migrate.Migrations) (*migrate.MigrationGroup, error) {
	migrator := migrate.NewMigrator(db, migrations)
	if err := migrator.Init(ctx); err != nil {
		return nil, apierrors.Wrap(err, apierrors.CategoryOperation, "failed to initialize migrator")
	}

	group, err := migrator.Migrate(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no new migrations") {
			return nil, nil // not an error, just nothing to do
		}
		return nil, apierrors.Wrap(err, apierrors.CategoryOperation, "failed to run migrations")
	}

	if group.IsZero() {
		m.logger().Debug("migrations: no new migrations were applied in this group")
	} else {
		m.logger().Debug("migrations: successfully applied migration group", "group", group.String())
	}

	return group, nil
}

// Migrate runs SQL file-based migrations discovered from registered filesystems.
func (m *Migrations) Migrate(ctx context.Context, db *bun.DB) error {
	// Only run SQL migrations if that's all you have
	m.logger().Debug("migrations: running SQL file-based migrations...")

	sqlMigrations, err := m.initSQLMigrations()
	if err != nil {
		return err
	}

	if sqlMigrations != nil && len(sqlMigrations.Sorted()) > 0 {
		sqlMigrationsGroup, err := m.run(ctx, db, sqlMigrations)
		if err != nil {
			return apierrors.Wrap(err, apierrors.CategoryOperation, "failed to run SQL migrations")
		}
		m.migrations = sqlMigrationsGroup
	} else {
		m.logger().Debug("migrations: no SQL migrations found")
	}

	m.logger().Debug("migrations: all migration groups completed")
	return nil
}

// Rollback will only roll back the most recent migration,
// which will be from the SQL set if it exists, otherwise from the Go set.
// TODO: more robust implementation which requires more complex logic
func (m *Migrations) Rollback(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	sqlMigrations, err := m.initSQLMigrations()
	if err != nil {
		return err
	}

	if sqlMigrations == nil {
		//no migrations registered so nothing to rollback
		m.logger().Debug("migrations: no migrations registered to roll back")
		return nil
	}

	migrator := migrate.NewMigrator(db, sqlMigrations)
	if err := migrator.Init(ctx); err != nil {
		return apierrors.Wrap(err, apierrors.CategoryOperation, "failed to initialize migrator for rollback")
	}

	group, err := migrator.Rollback(ctx, opts...)
	if err != nil {
		if strings.Contains(err.Error(), "no migrations to roll back") {
			m.logger().Debug("migrations: no migrations to roll back")
			return nil
		}
		return apierrors.Wrap(err, apierrors.CategoryOperation, "failed to rollback migrations")
	}

	m.migrations = group
	if group != nil && !group.IsZero() {
		m.logger().Debug("migrations: successfully rolled back migration group", "group", group.String())
	}

	return nil
}

// RollbackAll rollbacks every registered migration group.
func (m *Migrations) RollbackAll(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	sqlMigrations, err := m.initSQLMigrations()
	if err != nil {
		return err
	}

	if sqlMigrations == nil {
		//no migrations registered so nothing to rollback
		m.logger().Debug("migrations: no migrations registered to roll back")
		return nil
	}

	migrator := migrate.NewMigrator(db, sqlMigrations)
	if err := migrator.Init(ctx); err != nil {
		return apierrors.Wrap(err, apierrors.CategoryOperation, "failed to initialize migrator for rollback")
	}

	var lastGroup *migrate.MigrationGroup
	for {
		group, err := migrator.Rollback(ctx, opts...)
		if err != nil {
			if strings.Contains(err.Error(), "no migrations to roll back") {
				break
			}
			return apierrors.Wrap(err, apierrors.CategoryOperation, "failed to rollback all migrations")
		}
		if len(group.Migrations) == 0 {
			break
		}
		lastGroup = group
		m.logger().Debug("migrations: rolled back group", "group", group.String())
	}

	m.migrations = lastGroup
	return nil
}

// Report returns the status of the last migration group.
// It returns nil if Execute has not been called or has
// failed.
func (m *Migrations) Report() *migrate.MigrationGroup {
	return m.migrations
}
