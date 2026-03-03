package persistence

import (
	"context"
	"database/sql"
	"fmt"
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
	mx                   sync.Mutex
	Files                []fs.FS // For SQL files
	dialectRegistrations []dialectRegistration
	orderedRegistrations []orderedSourceRegistration
	orderedMetadata      map[string]OrderedMigrationMetadata
	migrations           *migrate.MigrationGroup
	lgr                  Logger
}

func NewMigrations() *Migrations {
	m := &Migrations{
		Files:                make([]fs.FS, 0),
		dialectRegistrations: make([]dialectRegistration, 0),
		orderedRegistrations: make([]orderedSourceRegistration, 0),
		orderedMetadata:      make(map[string]OrderedMigrationMetadata),
		lgr:                  &defaultLogger{},
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
func (m *Migrations) initSQLMigrations(ctx context.Context, db *bun.DB) (*migrate.Migrations, error) {
	m.mx.Lock()
	files := append([]fs.FS(nil), m.Files...)
	dialectRegistrations := append([]dialectRegistration(nil), m.dialectRegistrations...)
	orderedRegistrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()

	if len(files) == 0 && len(dialectRegistrations) == 0 && len(orderedRegistrations) == 0 {
		return nil, nil // Nothing to do
	}

	migrations := migrate.NewMigrations()
	for i, migrationFS := range files {
		if err := migrations.Discover(migrationFS); err != nil {
			return nil, apierrors.Wrap(err,
				apierrors.CategoryInternal,
				"failed to discover filesystem migrations",
			).WithMetadata(map[string]any{"index": i})
		}
	}

	for i, registration := range dialectRegistrations {
		buildResult, err := registration.buildFileSystems(ctx, db)
		if err != nil {
			return nil, apierrors.Wrap(err,
				apierrors.CategoryInternal,
				"failed to prepare dialect-specific migrations",
			).WithMetadata(map[string]any{"index": i})
		}
		for j, migrationFS := range buildResult.fileSystems {
			if err := migrations.Discover(migrationFS); err != nil {
				return nil, apierrors.Wrap(err,
					apierrors.CategoryInternal,
					"failed to discover dialect filesystem migrations",
				).WithMetadata(map[string]any{"index": j, "dialect_registration": i})
			}
		}
	}

	orderedMigrations, orderedMetadata, err := buildOrderedMigrations(ctx, db, orderedRegistrations)
	if err != nil {
		return nil, err
	}
	for _, migration := range orderedMigrations {
		migrations.Add(migration)
	}

	m.mx.Lock()
	m.orderedMetadata = orderedMetadata
	m.mx.Unlock()

	if len(migrations.Sorted()) == 0 {
		return nil, nil
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

// RegisterDialectMigrations registers migrations that may differ per dialect.
func (m *Migrations) RegisterDialectMigrations(root fs.FS, opts ...DialectMigrationOption) *Migrations {
	if root == nil {
		return m
	}

	config := defaultDialectOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&config)
	}

	m.mx.Lock()
	m.dialectRegistrations = append(m.dialectRegistrations, dialectRegistration{
		root: root,
		opts: config,
	})
	m.mx.Unlock()

	return m
}

// RegisterOrderedMigrationSources registers ordered SQL migration sources.
func (m *Migrations) RegisterOrderedMigrationSources(sources ...OrderedMigrationSource) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	seen := make(map[string]struct{}, len(m.orderedRegistrations)+len(sources))
	for _, existing := range m.orderedRegistrations {
		seen[existing.name] = struct{}{}
	}

	for idx, source := range sources {
		name := strings.TrimSpace(source.Name)
		if name == "" {
			return fmt.Errorf("ordered migration source at index %d has empty name", idx)
		}
		if source.Root == nil {
			return fmt.Errorf("ordered migration source %q has nil root filesystem", name)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate ordered migration source name %q", name)
		}
		seen[name] = struct{}{}

		opts := defaultDialectOptions()
		for _, opt := range source.Options {
			if opt == nil {
				continue
			}
			opt(&opts)
		}
		if opts.sourceLabel == defaultDialectSourceLabel {
			opts.sourceLabel = name
		}

		m.orderedRegistrations = append(m.orderedRegistrations, orderedSourceRegistration{
			name: name,
			registration: dialectRegistration{
				root: source.Root,
				opts: opts,
			},
		})
	}

	return nil
}

// ValidateDialects executes configured dialect validation callbacks.
func (m *Migrations) ValidateDialects(ctx context.Context, db *bun.DB) error {
	m.mx.Lock()
	registrations := make([]dialectRegistration, len(m.dialectRegistrations))
	copy(registrations, m.dialectRegistrations)
	orderedRegistrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()

	for idx, registration := range registrations {
		if err := registration.validate(ctx, db, idx); err != nil {
			return err
		}
	}
	for idx, registration := range orderedRegistrations {
		if err := registration.registration.validate(ctx, db, idx); err != nil {
			return err
		}
	}
	return nil
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
		m.logOrderedGroup(group.Migrations)
	}

	return group, nil
}

// Migrate runs SQL file-based migrations discovered from registered filesystems.
func (m *Migrations) Migrate(ctx context.Context, db *bun.DB) error {
	// Only run SQL migrations if that's all you have
	m.logger().Debug("migrations: running SQL file-based migrations...")

	if m.shouldValidateDialectsOnMigrate() {
		if err := m.ValidateDialects(ctx, db); err != nil {
			return err
		}
	}

	sqlMigrations, err := m.initSQLMigrations(ctx, db)
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
	sqlMigrations, err := m.initSQLMigrations(ctx, db)
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
		m.logOrderedGroup(group.Migrations)
	}

	return nil
}

// RollbackAll rollbacks every registered migration group.
func (m *Migrations) RollbackAll(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	sqlMigrations, err := m.initSQLMigrations(ctx, db)
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
		m.logOrderedGroup(group.Migrations)
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

func (m *Migrations) logOrderedGroup(migrations migrate.MigrationSlice) {
	if len(migrations) == 0 {
		return
	}
	m.mx.Lock()
	metadata := make(map[string]OrderedMigrationMetadata, len(m.orderedMetadata))
	for k, v := range m.orderedMetadata {
		metadata[k] = v
	}
	m.mx.Unlock()
	for _, migration := range migrations {
		meta, ok := metadata[migration.Name]
		if !ok {
			continue
		}
		m.logger().Debug(
			"migrations: ordered source migration",
			"synthetic", migration.Name,
			"source", meta.SourceName,
			"version", meta.OriginalVersion,
			"up", meta.UpPath,
			"down", meta.DownPath,
		)
	}
}

func (m *Migrations) shouldValidateDialectsOnMigrate() bool {
	m.mx.Lock()
	dialectRegistrations := append([]dialectRegistration(nil), m.dialectRegistrations...)
	orderedRegistrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()

	for _, registration := range dialectRegistrations {
		if registration.opts.validateOnMigrate {
			return true
		}
	}
	for _, registration := range orderedRegistrations {
		if registration.registration.opts.validateOnMigrate {
			return true
		}
	}
	return false
}
