package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"maps"
	"sort"
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
	sqlSourceNames       []string
	dialectRegistrations []dialectRegistration
	orderedRegistrations []orderedSourceRegistration
	orderedMetadata      map[string]OrderedMigrationMetadata
	lastPlan             *MigrationPlan
	planEntries          map[string]MigrationPlanEntry
	migrations           *migrate.MigrationGroup
	lgr                  Logger
}

func NewMigrations() *Migrations {
	m := &Migrations{
		Files:                make([]fs.FS, 0),
		sqlSourceNames:       make([]string, 0),
		dialectRegistrations: make([]dialectRegistration, 0),
		orderedRegistrations: make([]orderedSourceRegistration, 0),
		orderedMetadata:      make(map[string]OrderedMigrationMetadata),
		planEntries:          make(map[string]MigrationPlanEntry),
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
	resolved, err := m.resolvePlan(ctx, db, resolvePlanOptions{
		rejectSubsetConflicts: true,
	})
	if err != nil {
		return nil, err
	}

	m.cacheResolvedPlan(resolved)
	if resolved.migrations == nil || len(resolved.migrations.Sorted()) == 0 {
		return nil, nil
	}
	return resolved.migrations, nil
}

// RegisterSQLMigrations adds SQL based migrations
func (m *Migrations) RegisterSQLMigrations(migrations ...fs.FS) *Migrations {
	m.mx.Lock()
	for _, migrationFS := range migrations {
		m.Files = append(m.Files, migrationFS)
		m.sqlSourceNames = append(m.sqlSourceNames, m.nextAvailableAutoSourceNameLocked(defaultSQLSourceName))
	}
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
		root:       root,
		opts:       config,
		sourceName: m.nextAvailableAutoSourceNameLocked(defaultDialectSourceName),
	})
	m.mx.Unlock()

	return m
}

// RegisterOrderedMigrationSources registers ordered SQL migration sources.
func (m *Migrations) RegisterOrderedMigrationSources(sources ...OrderedMigrationSource) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	seen := m.sourceNameSetLocked()

	registrations := make([]orderedSourceRegistration, 0, len(sources))
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

		registration, err := normalizeOrderedSourceRegistration(source, name, len(m.orderedRegistrations)+len(registrations))
		if err != nil {
			return err
		}

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

		registration.registration = dialectRegistration{
			root: source.Root,
			opts: opts,
		}
		registrations = append(registrations, registration)
	}

	if err := validateOrderedIdentityModeSet(append(append([]orderedSourceRegistration(nil), m.orderedRegistrations...), registrations...)); err != nil {
		return err
	}
	m.orderedRegistrations = append(m.orderedRegistrations, registrations...)

	return nil
}

func normalizeOrderedSourceRegistration(source OrderedMigrationSource, name string, sequence int) (orderedSourceRegistration, error) {
	registration := orderedSourceRegistration{
		name:         name,
		sequence:     sequence,
		identityMode: source.IdentityMode,
	}

	if source.IdentityMode == OrderedMigrationIdentityPositional {
		if strings.TrimSpace(source.SourceKey) != "" || source.Order != 0 || len(source.DependsOn) > 0 {
			return registration, &OrderedSourceGraphError{
				Kind:       ErrOrderedSourceInvalidConfig,
				SourceName: name,
				Message:    fmt.Sprintf("ordered migration source %q supplies source-stable fields but does not opt into source-stable identity mode", name),
			}
		}
		return registration, nil
	}

	if source.IdentityMode != OrderedMigrationIdentitySourceStable {
		return registration, &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: name,
			Message:    fmt.Sprintf("ordered migration source %q has unsupported identity mode %s", name, source.IdentityMode.String()),
		}
	}

	key := strings.TrimSpace(source.SourceKey)
	if key == "" {
		key = name
	}
	normalizedKey, err := normalizeOrderedSourceKey(key)
	if err != nil {
		return registration, &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: name,
			Message:    fmt.Sprintf("ordered migration source %q has invalid source key %q: %v", name, key, err),
		}
	}
	if source.Order <= 0 {
		return registration, &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: name,
			SourceKey:  normalizedKey,
			Message:    fmt.Sprintf("ordered migration source %q must have a positive order in source-stable mode", name),
		}
	}
	if source.Order > MaxOrderedMigrationSourceOrder {
		return registration, &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: name,
			SourceKey:  normalizedKey,
			Message: fmt.Sprintf(
				"ordered migration source %q order %d exceeds the source-stable maximum %d",
				name,
				source.Order,
				MaxOrderedMigrationSourceOrder,
			),
		}
	}

	deps := make([]string, 0, len(source.DependsOn))
	seenDeps := make(map[string]struct{}, len(source.DependsOn))
	for _, rawDependency := range source.DependsOn {
		dependencyKey, depErr := normalizeOrderedSourceKey(rawDependency)
		if depErr != nil {
			return registration, &OrderedSourceGraphError{
				Kind:       ErrOrderedSourceInvalidConfig,
				SourceName: name,
				SourceKey:  normalizedKey,
				Dependency: rawDependency,
				Message:    fmt.Sprintf("ordered migration source %q has invalid dependency key %q: %v", name, rawDependency, depErr),
			}
		}
		if dependencyKey == normalizedKey {
			return registration, &OrderedSourceGraphError{
				Kind:       ErrOrderedSourceCycle,
				SourceName: name,
				SourceKey:  normalizedKey,
				Dependency: dependencyKey,
				Message:    fmt.Sprintf("ordered migration source %q cannot depend on itself", name),
			}
		}
		if _, exists := seenDeps[dependencyKey]; exists {
			continue
		}
		seenDeps[dependencyKey] = struct{}{}
		deps = append(deps, dependencyKey)
	}
	sort.Strings(deps)

	registration.sourceKey = normalizedKey
	registration.sourceOrder = source.Order
	registration.dependsOn = deps
	return registration, nil
}

func validateOrderedIdentityModeSet(registrations []orderedSourceRegistration) error {
	_, err := resolveOrderedSourceGraph(registrations)
	return err
}

// ValidateDialects executes configured dialect validation callbacks.
func (m *Migrations) ValidateDialects(ctx context.Context, db *bun.DB) error {
	return m.validateDialects(ctx, db, nil)
}

func (m *Migrations) validateDialects(
	ctx context.Context,
	db *bun.DB,
	selected map[string]struct{},
) error {
	m.mx.Lock()
	registrations := make([]dialectRegistration, len(m.dialectRegistrations))
	copy(registrations, m.dialectRegistrations)
	orderedRegistrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()

	for idx, registration := range registrations {
		if len(selected) > 0 {
			if _, ok := selected[registration.sourceName]; !ok {
				continue
			}
		}
		if err := registration.validate(ctx, db, idx); err != nil {
			return err
		}
	}
	for idx, registration := range orderedRegistrations {
		if len(selected) > 0 {
			if _, ok := selected[registration.name]; !ok {
				continue
			}
		}
		if err := registration.registration.validate(ctx, db, idx); err != nil {
			return err
		}
	}
	return nil
}

// run is a helper to execute migrations for a given collection
func (m *Migrations) run(
	ctx context.Context,
	db *bun.DB,
	migrations *migrate.Migrations,
	entries map[string]MigrationPlanEntry,
) (*migrate.MigrationGroup, error) {
	migrator := migrate.NewMigrator(db, migrations)
	if err := migrator.Init(ctx); err != nil {
		return nil, apierrors.Wrap(err, apierrors.CategoryOperation, "failed to initialize migrator")
	}

	group, err := migrator.Migrate(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no new migrations") {
			return nil, nil // not an error, just nothing to do
		}
		return nil, apierrors.Wrap(wrapMigrationExecutionError(err, entries), apierrors.CategoryOperation, "failed to run migrations")
	}

	if group.IsZero() {
		m.logger().Debug("migrations: no new migrations were applied in this group")
	} else {
		m.logger().Debug("migrations: successfully applied migration group", "group", group.String())
		m.logMigrationGroup(group.Migrations)
	}

	return group, nil
}

// Migrate runs SQL file-based migrations discovered from registered filesystems.
func (m *Migrations) Migrate(ctx context.Context, db *bun.DB) error {
	return m.migrateWithSourceSelection(ctx, db, nil)
}

// MigrateSources runs migrations for a selected subset of registered sources.
func (m *Migrations) MigrateSources(
	ctx context.Context,
	db *bun.DB,
	sourceNames ...string,
) error {
	if len(sourceNames) == 0 {
		return fmt.Errorf("at least one source name is required")
	}
	return m.migrateWithSourceSelection(ctx, db, sourceNames)
}

func (m *Migrations) migrateWithSourceSelection(
	ctx context.Context,
	db *bun.DB,
	sourceNames []string,
) error {
	// Only run SQL migrations if that's all you have
	m.logger().Debug("migrations: running SQL file-based migrations...")

	selected := make(map[string]struct{}, len(sourceNames))
	for _, name := range sourceNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		selected[name] = struct{}{}
	}

	if m.shouldValidateDialectsOnMigrate(selected) {
		if err := m.validateDialects(ctx, db, selected); err != nil {
			return err
		}
	}

	resolved, err := m.resolvePlan(ctx, db, resolvePlanOptions{
		sourceNames:           sourceNames,
		rejectSubsetConflicts: true,
	})
	if err != nil {
		return err
	}
	m.cacheResolvedPlan(resolved)
	if verifyErr := m.verifyOrderedSourceGraph(ctx, db, resolved.orderedGraph); verifyErr != nil {
		return verifyErr
	}

	if resolved.migrations != nil && len(resolved.migrations.Sorted()) > 0 {
		sqlMigrationsGroup, err := m.run(ctx, db, resolved.migrations, resolved.entryByName)
		if err != nil {
			return apierrors.Wrap(err, apierrors.CategoryOperation, "failed to run SQL migrations")
		}
		m.migrations = sqlMigrationsGroup
	} else {
		m.logger().Debug("migrations: no SQL migrations found")
	}
	if err := m.persistOrderedSourceGraph(ctx, db, resolved.orderedGraph); err != nil {
		return err
	}

	m.logger().Debug("migrations: all migration groups completed")
	return nil
}

// Rollback will only roll back the most recent migration,
// which will be from the SQL set if it exists, otherwise from the Go set.
// TODO: more robust implementation which requires more complex logic
func (m *Migrations) Rollback(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	applied, err := m.appliedMigrations(ctx, db)
	if err != nil {
		return err
	}
	lastGroup := applied.LastGroup()
	if lastGroup == nil || len(lastGroup.Migrations) == 0 {
		m.logger().Debug("migrations: no migrations registered to roll back")
		return nil
	}

	sqlMigrations, resolved, err := m.resolveRollbackMigrations(ctx, db, migrationNameSet(lastGroup.Migrations))
	if err != nil {
		return err
	}
	if sqlMigrations == nil {
		m.logger().Debug("migrations: no migrations registered to roll back")
		return nil
	}
	if verifyErr := m.verifyOrderedSourceGraph(ctx, db, resolved.orderedGraph); verifyErr != nil {
		return verifyErr
	}
	m.cacheResolvedPlan(resolved)

	migrator := migrate.NewMigrator(db, sqlMigrations)
	if initErr := migrator.Init(ctx); initErr != nil {
		return apierrors.Wrap(initErr, apierrors.CategoryOperation, "failed to initialize migrator for rollback")
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
		m.logMigrationGroup(group.Migrations)
	}

	return nil
}

// RollbackAll rollbacks every registered migration group.
func (m *Migrations) RollbackAll(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	applied, err := m.appliedMigrations(ctx, db)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		m.logger().Debug("migrations: no migrations registered to roll back")
		return nil
	}

	sqlMigrations, resolved, err := m.resolveRollbackMigrations(ctx, db, migrationNameSet(applied))
	if err != nil {
		return err
	}
	if sqlMigrations == nil {
		m.logger().Debug("migrations: no migrations registered to roll back")
		return nil
	}
	if err := m.verifyOrderedSourceGraph(ctx, db, resolved.orderedGraph); err != nil {
		return err
	}
	m.cacheResolvedPlan(resolved)

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
		m.logMigrationGroup(group.Migrations)
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

func (m *Migrations) logMigrationGroup(migrations migrate.MigrationSlice) {
	if len(migrations) == 0 {
		return
	}
	m.mx.Lock()
	metadata := make(map[string]MigrationPlanEntry, len(m.planEntries))
	maps.Copy(metadata, m.planEntries)
	m.mx.Unlock()
	for _, migration := range migrations {
		meta, ok := metadata[migration.Name]
		if !ok {
			continue
		}
		m.logger().Debug(
			"migrations: source migration",
			"synthetic", migration.Name,
			"source", meta.SourceName,
			"kind", meta.SourceKind,
			"version", meta.OriginalVersion,
			"comment", meta.OriginalComment,
			"up", meta.UpPath,
			"down", meta.DownPath,
		)
	}
}

func (m *Migrations) shouldValidateDialectsOnMigrate(selected map[string]struct{}) bool {
	m.mx.Lock()
	dialectRegistrations := append([]dialectRegistration(nil), m.dialectRegistrations...)
	orderedRegistrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()

	for _, registration := range dialectRegistrations {
		if len(selected) > 0 {
			if _, ok := selected[registration.sourceName]; !ok {
				continue
			}
		}
		if registration.opts.validateOnMigrate {
			return true
		}
	}
	for _, registration := range orderedRegistrations {
		if len(selected) > 0 {
			if _, ok := selected[registration.name]; !ok {
				continue
			}
		}
		if registration.registration.opts.validateOnMigrate {
			return true
		}
	}
	return false
}

func (m *Migrations) cacheResolvedPlan(resolved *resolvedMigrationPlan) {
	if resolved == nil {
		return
	}

	planEntries := make(map[string]MigrationPlanEntry, len(resolved.entryByName))
	orderedMetadata := make(map[string]OrderedMigrationMetadata)
	for name, entry := range resolved.entryByName {
		planEntries[name] = entry
		if entry.SourceKind != MigrationSourceKindOrdered {
			continue
		}
		orderedMetadata[name] = OrderedMigrationMetadata{
			SyntheticName:    entry.SyntheticName,
			SourceName:       entry.SourceName,
			SourceKey:        entry.SourceKey,
			SourceOrder:      entry.SourceOrder,
			SourceDependsOn:  append([]string(nil), entry.SourceDependsOn...),
			ResolvedPosition: entry.ResolvedPosition,
			IdentityMode:     entry.IdentityMode,
			OriginalVersion:  entry.OriginalVersion,
			OriginalComment:  entry.OriginalComment,
			UpPath:           entry.UpPath,
			DownPath:         entry.DownPath,
		}
	}

	m.mx.Lock()
	m.lastPlan = cloneMigrationPlan(resolved.plan)
	m.planEntries = planEntries
	m.orderedMetadata = orderedMetadata
	m.mx.Unlock()
}

func wrapMigrationExecutionError(err error, entries map[string]MigrationPlanEntry) error {
	if err == nil {
		return nil
	}

	name := migrationNameFromError(err)
	if name == "" {
		return err
	}
	entry, ok := entries[name]
	if !ok {
		return err
	}

	return fmt.Errorf(
		"migration %q from source %q (%s_%s): %w",
		entry.SyntheticName,
		entry.SourceName,
		entry.OriginalVersion,
		entry.OriginalComment,
		err,
	)
}

func migrationNameFromError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	for _, marker := range []string{": up:", ": down:"} {
		if idx := strings.Index(value, marker); idx > 0 {
			return value[:idx]
		}
	}
	return ""
}

func (m *Migrations) nextAvailableAutoSourceNameLocked(generator func(int) string) string {
	used := m.sourceNameSetLocked()
	for idx := 0; ; idx++ {
		name := generator(idx)
		if _, exists := used[name]; exists {
			continue
		}
		return name
	}
}

func (m *Migrations) sourceNameSetLocked() map[string]struct{} {
	used := make(map[string]struct{}, len(m.sqlSourceNames)+len(m.dialectRegistrations)+len(m.orderedRegistrations))
	for _, name := range m.sqlSourceNames {
		used[name] = struct{}{}
	}
	for _, registration := range m.dialectRegistrations {
		if registration.sourceName == "" {
			continue
		}
		used[registration.sourceName] = struct{}{}
	}
	for _, registration := range m.orderedRegistrations {
		used[registration.name] = struct{}{}
	}
	return used
}

func (m *Migrations) appliedMigrations(ctx context.Context, db *bun.DB) (migrate.MigrationSlice, error) {
	migrator := migrate.NewMigrator(db, migrate.NewMigrations())
	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		if isMissingMigrationsTableError(err) {
			return nil, nil
		}
		return nil, apierrors.Wrap(err, apierrors.CategoryOperation, "failed to inspect applied migrations")
	}
	return applied, nil
}

func (m *Migrations) resolveRollbackMigrations(
	ctx context.Context,
	db *bun.DB,
	requiredNames map[string]struct{},
) (*migrate.Migrations, *resolvedMigrationPlan, error) {
	if m.currentOrderedSourcesUseStableIdentity() {
		aliases, err := m.orderedSourceAliases(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		if len(aliases) > 0 {
			for legacyName := range aliases {
				delete(requiredNames, legacyName)
			}
		}
	}
	if len(requiredNames) == 0 {
		return nil, nil, nil
	}
	resolved, err := m.resolvePlan(ctx, db, resolvePlanOptions{
		requiredMigrationNames: requiredNames,
	})
	if err != nil {
		return nil, nil, err
	}
	if resolved.migrations == nil || len(resolved.migrations.Sorted()) == 0 {
		return nil, nil, nil
	}
	return resolved.migrations, resolved, nil
}

func (m *Migrations) currentOrderedSourcesUseStableIdentity() bool {
	m.mx.Lock()
	registrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()
	return orderedSourcesUseStableIdentity(registrations)
}

func migrationNameSet(migrations migrate.MigrationSlice) map[string]struct{} {
	out := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		out[migration.Name] = struct{}{}
	}
	return out
}
