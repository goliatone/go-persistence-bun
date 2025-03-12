package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sync"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

// DriverConfig provides a way to abstract
// the connection process
type DriverConfig interface {
	Connect(options ...bun.DBOption) (*bun.DB, *sql.DB, error)
}

// MigratorFunc holds a migration
type MigratorFunc struct {
	Up   migrate.MigrationFunc
	Down migrate.MigrationFunc
}

// Migrations holds configuration options
// for migrations
// See https://bun.uptrace.dev/guide/migrations.html
type Migrations struct {
	mx         sync.Mutex
	Files      []fs.FS
	Func       []MigratorFunc
	migrations *migrate.MigrationGroup
	logf       func(format string, a ...any)
}

// TODO: We need to make sure we run migrations in the reverse order that
// were up.run
func (m *Migrations) initMigrations() (*migrate.Migrations, error) {
	migrations := migrate.NewMigrations()

	for i, migration := range m.Files {
		if err := migrations.Discover(migration); err != nil {
			return nil, fmt.Errorf("error filesystem migration FS %v: %w", i, err)
		}
	}

	for i, migration := range m.Func {
		if err := migrations.Register(migration.Up, migration.Down); err != nil {
			return nil, fmt.Errorf("error migrator migration FS %v: %w", i, err)
		}
	}
	return migrations, nil
}

func (m *Migrations) log(format string, a ...any) {
	if m.logf != nil {
		m.logf(format, a...)
	}
}

// RegisterSQLMigrations adds SQL based migrations
func (m *Migrations) RegisterSQLMigrations(migrations ...fs.FS) *Migrations {
	m.mx.Lock()
	m.Files = append(m.Files, migrations...)
	m.mx.Unlock()
	return m
}

// RegisterFuncMigrations adds SQL based migrations
func (m *Migrations) RegisterFuncMigrations(migrations ...MigratorFunc) *Migrations {
	m.mx.Lock()
	m.Func = append(m.Func, migrations...)
	m.mx.Unlock()
	return m
}

// Migrate will execute every registered migration.
// Method can be called multiple times, the ORM knows
// how to manage executed migrations.
func (m *Migrations) Migrate(ctx context.Context, db *bun.DB) error {
	migrations, err := m.initMigrations()
	if err != nil {
		return fmt.Errorf("error init migrations: %w", err)
	}

	m.log("migrations: found files: %s\n", migrations.Sorted().String())

	migrator := migrate.NewMigrator(db, migrations)
	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("error create migrator: %w", err)
	}

	if len(m.Files) == len(m.Func) && len(m.Func) == 0 {
		m.log("migrations: we did not find any migrations")
		return nil
	}

	groups, err := migrator.Migrate(ctx)
	if err != nil {
		if err.Error() == "migrate: there are no migrations" {
			return nil
		}
		return fmt.Errorf("error running migrator: %w", err)
	}

	m.migrations = groups

	return nil
}

// Rollback previously executed migrations.
// It will rollback a group at a time.
// See https://bun.uptrace.dev/guide/migrations.html#migration-groups-and-rollbacks.
func (m *Migrations) Rollback(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	return m.rollback(ctx, db, false, opts...)
}

// RollbackAll rollbacks every registered migration group.
func (m *Migrations) RollbackAll(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	return m.rollback(ctx, db, true, opts...)
}

func (m *Migrations) rollback(ctx context.Context, db *bun.DB, all bool, opts ...migrate.MigrationOption) error {
	migrations, err := m.initMigrations()
	if err != nil {
		return fmt.Errorf("error init migrations: %w", err)
	}

	migrator := migrate.NewMigrator(db, migrations)
	if err := migrator.Init(ctx); err != nil {
		return fmt.Errorf("error create migrator: %w", err)
	}

	group, err := migrator.Rollback(ctx, opts...)
	if all {
		for len(group.Migrations) > 0 && err == nil {
			group, err = migrator.Rollback(ctx, opts...)
		}
	}
	if err != nil {
		return fmt.Errorf("error rolling back migrator: %w", err)
	}

	m.migrations = group

	return nil
}

// Report returns the status of migrations.
// It returns nil if Execute has not been called
// or has failed.
func (m *Migrations) Report() *migrate.MigrationGroup {
	return m.migrations
}
