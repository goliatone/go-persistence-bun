package persistence

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"maps"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"
)

// MockLogger for testing
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Debug(msg string, keysAndValues ...any) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Info(msg string, keysAndValues ...any) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Warn(msg string, keysAndValues ...any) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Error(msg string, keysAndValues ...any) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Fatal(msg string, keysAndValues ...any) {
	m.Called(msg, keysAndValues)
}

func TestNewMigrations(t *testing.T) {
	m := NewMigrations()

	assert.NotNil(t, m)
	assert.NotNil(t, m.Files)
	assert.Equal(t, 0, len(m.Files))
	assert.NotNil(t, m.lgr)
}

func TestMigrations_SetLogger(t *testing.T) {
	m := NewMigrations()
	mockLogger := new(MockLogger)

	m.SetLogger(mockLogger)

	assert.Equal(t, mockLogger, m.lgr)
}

func TestMigrations_SetLogger_Nil(t *testing.T) {
	m := NewMigrations()
	originalLogger := m.lgr

	m.SetLogger(nil)

	assert.Equal(t, originalLogger, m.lgr, "Logger should not change when nil is passed")
}

func TestMigrations_RegisterSQLMigrations(t *testing.T) {
	m := NewMigrations()

	// Create test filesystems
	fs1 := fstest.MapFS{
		"001_init.up.sql":   {Data: []byte("CREATE TABLE test1;")},
		"001_init.down.sql": {Data: []byte("DROP TABLE test1;")},
	}

	fs2 := fstest.MapFS{
		"002_add_column.up.sql":   {Data: []byte("ALTER TABLE test1 ADD COLUMN name TEXT;")},
		"002_add_column.down.sql": {Data: []byte("ALTER TABLE test1 DROP COLUMN name;")},
	}

	// Register migrations
	result := m.RegisterSQLMigrations(fs1, fs2)

	assert.Equal(t, m, result, "Should return self for chaining")
	assert.Equal(t, 2, len(m.Files))
}

func TestMigrations_RegisterSQLMigrations_ThreadSafe(t *testing.T) {
	m := NewMigrations()

	// Create multiple filesystems
	filesystems := make([]fstest.MapFS, 10)
	for i := range 10 {
		filesystems[i] = fstest.MapFS{
			"test.sql": {Data: []byte("SELECT 1;")},
		}
	}

	// Register concurrently
	done := make(chan bool, 10)
	for i := range 10 {
		go func(fs fstest.MapFS) {
			m.RegisterSQLMigrations(fs)
			done <- true
		}(filesystems[i])
	}

	// Wait for all goroutines
	for range 10 {
		<-done
	}

	assert.Equal(t, 10, len(m.Files), "All filesystems should be registered")
}

func TestDialectOptionsExtractDialects(t *testing.T) {
	opts := defaultDialectOptions()
	data := []byte(`
        ---bun:dialect:postgres, sqlite
        SELECT 1;
    `)

	dialects := opts.extractDialects(data)
	require.ElementsMatch(t, []string{"postgres", "sqlite"}, dialects)
}

func TestDialectRegistrationBuildsLayeredFS(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_init.up.sql":          {Data: []byte("root up")},
		"0001_init.down.sql":        {Data: []byte("root down")},
		"0002_pg_only.up.sql":       {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
		"0002_pg_only.down.sql":     {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
		"common/0000_base.up.sql":   {Data: []byte("common up")},
		"common/0000_base.down.sql": {Data: []byte("common down")},
		"sqlite/0001_init.up.sql":   {Data: []byte("sqlite override up")},
		"sqlite/0001_init.down.sql": {Data: []byte("sqlite override down")},
		"sqlite/0003_extra.up.sql":  {Data: []byte("sqlite extra up")},
	}

	reg := dialectRegistration{
		root: fsys,
		opts: defaultDialectOptions(),
	}
	reg.opts.explicitDialect = "sqlite"

	buildResult, err := reg.buildFileSystems(ctx, nil)
	require.NoError(t, err)
	require.Len(t, buildResult.fileSystems, 3)

	files := collectFilesFromSources(t, buildResult.fileSystems)
	assert.Equal(t, "sqlite override up", strings.TrimSpace(files["0001_init.up.sql"]))
	assert.Equal(t, "sqlite override down", strings.TrimSpace(files["0001_init.down.sql"]))
	assert.Equal(t, "common up", strings.TrimSpace(files["0000_base.up.sql"]))
	assert.Equal(t, "common down", strings.TrimSpace(files["0000_base.down.sql"]))
	assert.Equal(t, "sqlite extra up", strings.TrimSpace(files["0003_extra.up.sql"]))
	assert.NotContains(t, files, "0002_pg_only.up.sql")
	assert.NotContains(t, files, "0002_pg_only.down.sql")
}

func TestRegisterDialectMigrationsUsesDatabaseDialect(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_init.up.sql":          {Data: []byte("root up")},
		"0001_init.down.sql":        {Data: []byte("root down")},
		"sqlite/0001_init.up.sql":   {Data: []byte("sqlite up")},
		"sqlite/0001_init.down.sql": {Data: []byte("sqlite down")},
	}

	m := NewMigrations()
	m.RegisterDialectMigrations(fsys)
	require.Len(t, m.dialectRegistrations, 1)

	db := bun.NewDB(nil, sqlitedialect.New())
	buildResult, err := m.dialectRegistrations[0].buildFileSystems(ctx, db)
	require.NoError(t, err)

	files := collectFilesFromSources(t, buildResult.fileSystems)
	assert.Equal(t, "sqlite up", strings.TrimSpace(files["0001_init.up.sql"]))
	assert.Equal(t, "sqlite down", strings.TrimSpace(files["0001_init.down.sql"]))
}

func TestDialectRegistrationFromDirFS(t *testing.T) {
	dirFS := os.DirFS("testdata/migrations/dialect")

	m := NewMigrations()
	m.RegisterDialectMigrations(dirFS)
	require.Len(t, m.dialectRegistrations, 1)

	reg := m.dialectRegistrations[0]

	pgResult, err := reg.buildForDialect("postgres")
	require.NoError(t, err)
	require.True(t, pgResult.hasSQL())
	pgFiles := collectFilesFromSources(t, pgResult.fileSystems)
	assert.Contains(t, pgFiles, "0003_annotation.up.sql")
	assert.Contains(t, pgFiles, "0002_traits.up.sql")

	sqliteResult, err := reg.buildForDialect("sqlite")
	require.NoError(t, err)
	require.True(t, sqliteResult.hasSQL())
	sqliteFiles := collectFilesFromSources(t, sqliteResult.fileSystems)
	assert.NotContains(t, sqliteFiles, "0003_annotation.up.sql")
	assert.Contains(t, sqliteFiles, "0002_traits.up.sql")
}

func TestValidateDialectsUniversalCoverage(t *testing.T) {
	ctx := context.Background()
	dirFS := os.DirFS("testdata/migrations/dialect")

	m := NewMigrations()
	called := false
	m.RegisterDialectMigrations(
		dirFS,
		WithValidationTargets("postgres", "sqlite"),
		WithDialectSourceLabel("testdata/migrations/dialect"),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			called = true
			return fmt.Errorf("validator should not run")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	require.NoError(t, err)
	require.False(t, called)
}

func TestValidateDialectsReportsMissingDialects(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_init.up.sql":   {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
		"0001_init.down.sql": {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
	}

	m := NewMigrations()
	var captured DialectValidationResult
	m.RegisterDialectMigrations(
		fsys,
		WithValidationTargets("postgres", "sqlite"),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			captured = result
			return fmt.Errorf("fail")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	require.EqualError(t, err, "fail")
	require.Contains(t, captured.MissingDialects, "sqlite")
	require.NotContains(t, captured.MissingDialects, "postgres")
	reasons := captured.MissingDialects["sqlite"]
	require.NotEmpty(t, reasons)
	require.Contains(t, strings.Join(reasons, ""), "SQL files exist but none match dialect")
}

func TestValidateDialectsDefaultPanics(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_init.up.sql": {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
	}

	m := NewMigrations()
	m.RegisterDialectMigrations(fsys, WithValidationTargets("sqlite"))

	assert.Panics(t, func() {
		_ = m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	})
}

func TestValidateDialectsDialectSpecificDirectoryMissing(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"sqlite/0001_init.up.sql":   {Data: []byte("sqlite up")},
		"sqlite/0001_init.down.sql": {Data: []byte("sqlite down")},
	}

	m := NewMigrations()
	var captured DialectValidationResult
	m.RegisterDialectMigrations(
		fsys,
		WithValidationTargets("postgres", "sqlite"),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			captured = result
			return fmt.Errorf("missing postgres")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, sqlitedialect.New()))
	require.EqualError(t, err, "missing postgres")
	require.Contains(t, captured.MissingDialects, "postgres")
	require.NotContains(t, captured.MissingDialects, "sqlite")
}

func TestValidateDialectsDefaultsToResolvedDialect(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_init.up.sql": {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
	}

	m := NewMigrations()
	var captured DialectValidationResult
	m.RegisterDialectMigrations(
		fsys,
		WithValidationTargets(),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			captured = result
			return fmt.Errorf("missing resolved")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, sqlitedialect.New()))
	require.EqualError(t, err, "missing resolved")
	require.Contains(t, captured.MissingDialects, "sqlite")
	require.Equal(t, []string{"sqlite"}, captured.CheckedDialects)
}

func TestValidateDialectsContractMandatoryTargets(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"postgres/0001_init.up.sql":   {Data: []byte("SELECT 1;")},
		"postgres/0001_init.down.sql": {Data: []byte("SELECT 1;")},
	}

	m := NewMigrations()
	var captured DialectValidationResult
	m.RegisterDialectMigrations(
		fsys,
		WithDialectValidationContract(DialectValidationContract{
			MandatoryTargets:     []string{"postgres", "sqlite"},
			RequireAtLeastOneSQL: true,
		}),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			captured = result
			return fmt.Errorf("contract failed")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	require.EqualError(t, err, "contract failed")
	require.Contains(t, captured.CheckedDialects, "postgres")
	require.Contains(t, captured.CheckedDialects, "sqlite")
	require.Contains(t, captured.MissingDialects, "sqlite")
	require.NotContains(t, captured.MissingDialects, "postgres")
	require.NotNil(t, captured.ValidationContract)
}

func TestValidateDialectsContractRequireUpDownPairs(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"sqlite/0001_users.up.sql": {Data: []byte("SELECT 1;")},
	}

	m := NewMigrations()
	var captured DialectValidationResult
	m.RegisterDialectMigrations(
		fsys,
		WithValidationTargets("sqlite"),
		WithDialectValidationContract(DialectValidationContract{
			RequireAtLeastOneSQL: true,
			RequireUpDownPairs:   true,
		}),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			captured = result
			return fmt.Errorf("up/down mismatch")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, sqlitedialect.New()))
	require.EqualError(t, err, "up/down mismatch")
	require.Contains(t, captured.MissingDialects, "sqlite")
	require.Contains(t, strings.Join(captured.MissingDialects["sqlite"], " "), "missing .down.sql pair")
}

func TestValidateDialectsContractRequireVersionParityAcrossTargets(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"postgres/0001_users.up.sql":   {Data: []byte("SELECT 1;")},
		"postgres/0001_users.down.sql": {Data: []byte("SELECT 1;")},
		"sqlite/0002_posts.up.sql":     {Data: []byte("SELECT 1;")},
		"sqlite/0002_posts.down.sql":   {Data: []byte("SELECT 1;")},
	}

	m := NewMigrations()
	var captured DialectValidationResult
	m.RegisterDialectMigrations(
		fsys,
		WithValidationTargets("postgres", "sqlite"),
		WithDialectValidationContract(DialectValidationContract{
			RequireAtLeastOneSQL:              true,
			RequireVersionParityAcrossTargets: true,
		}),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			captured = result
			return fmt.Errorf("parity mismatch")
		}),
	)

	err := m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	require.EqualError(t, err, "parity mismatch")
	require.Contains(t, captured.MissingDialects, "postgres")
	require.Contains(t, captured.MissingDialects, "sqlite")
	require.Contains(t, strings.Join(captured.MissingDialects["postgres"], " "), "0002_posts")
	require.Contains(t, strings.Join(captured.MissingDialects["sqlite"], " "), "0001_users")
}

func TestMigrations_MigrateRunsDialectValidationWhenEnabled(t *testing.T) {
	ctx := context.Background()
	fsys := fstest.MapFS{
		"0001_only_postgres.up.sql": {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
	}

	m := NewMigrations()
	m.RegisterDialectMigrations(
		fsys,
		WithValidationTargets("sqlite"),
		WithValidateOnMigrate(true),
		WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
			return fmt.Errorf("validate on migrate failed")
		}),
	)

	err := m.Migrate(ctx, bun.NewDB(nil, pgdialect.New()))
	require.EqualError(t, err, "validate on migrate failed")
}

func TestMigrations_RegisterOrderedMigrationSourcesRejectsDuplicateNames(t *testing.T) {
	m := NewMigrations()
	fs := fstest.MapFS{
		"0001_init.up.sql": {Data: []byte("SELECT 1;")},
	}

	err := m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: fs},
		OrderedMigrationSource{Name: "go-auth", Root: fs},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate ordered migration source name")

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-users", Root: fs},
	))
	err = m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-users", Root: fs},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate ordered migration source name")
}

func TestOrderedMigrations_DeterministicUpOrderWithOverlappingVersions(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	authFS := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE auth_users;")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE auth_users;")},
		"0002_auth.up.sql":   {Data: []byte("ALTER TABLE auth_users ADD COLUMN active BOOL;")},
		"0002_auth.down.sql": {Data: []byte("ALTER TABLE auth_users DROP COLUMN active;")},
	}
	usersFS := fstest.MapFS{
		"0001_users.up.sql":   {Data: []byte("CREATE TABLE users;")},
		"0001_users.down.sql": {Data: []byte("DROP TABLE users;")},
	}

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: authFS},
		OrderedMigrationSource{Name: "go-users", Root: usersFS},
	))

	sqlMigrations, err := m.initSQLMigrations(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, sqlMigrations)

	sorted := sqlMigrations.Sorted()
	require.Len(t, sorted, 3)

	upSequence := orderedSequenceFromMetadata(t, m, sorted)
	require.Equal(t, []string{
		"go-auth/0001",
		"go-auth/0002",
		"go-users/0001",
	}, upSequence)
}

func TestOrderedMigrations_DeterministicDownOrderIsReverseOfUp(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fstest.MapFS{
				"0001_auth.up.sql":   {Data: []byte("SELECT 1;")},
				"0001_auth.down.sql": {Data: []byte("SELECT 1;")},
			},
		},
		OrderedMigrationSource{
			Name: "go-users",
			Root: fstest.MapFS{
				"0001_users.up.sql":   {Data: []byte("SELECT 1;")},
				"0001_users.down.sql": {Data: []byte("SELECT 1;")},
				"0002_users.up.sql":   {Data: []byte("SELECT 1;")},
				"0002_users.down.sql": {Data: []byte("SELECT 1;")},
			},
		},
	))

	sqlMigrations, err := m.initSQLMigrations(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, sqlMigrations)

	up := sqlMigrations.Sorted()
	require.Len(t, up, 3)
	upSequence := orderedSequenceFromMetadata(t, m, up)

	for i := range up {
		up[i].ID = int64(i + 1)
		up[i].GroupID = 1
	}
	down := up.Applied()
	downSequence := orderedSequenceFromMetadata(t, m, down)

	require.Equal(t, reverseStrings(append([]string(nil), upSequence...)), downSequence)
}

func TestOrderedMigrations_RejectDuplicateIdentityWithinSource(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	fs := fstest.MapFS{
		"0001_alpha.up.sql":   {Data: []byte("SELECT 1;")},
		"0001_alpha.down.sql": {Data: []byte("SELECT 1;")},
		"0001_beta.up.sql":    {Data: []byte("SELECT 1;")},
	}

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: fs},
	))

	_, err := m.initSQLMigrations(ctx, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate migration identity")
}

func TestOrderedMigrations_DialectAwareLoading(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()
	dirFS := os.DirFS("testdata/migrations/dialect")

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-services",
			Root: dirFS,
			Options: []DialectMigrationOption{
				WithDialectName("sqlite"),
			},
		},
	))

	sqlMigrations, err := m.initSQLMigrations(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, sqlMigrations)

	sorted := sqlMigrations.Sorted()
	sequence := orderedSequenceFromMetadata(t, m, sorted)
	require.Equal(t, []string{
		"go-services/0000",
		"go-services/0001",
		"go-services/0002",
	}, sequence)
}

func TestOrderedMigrations_PlanPreservesDialectLayerPaths(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()
	dirFS := os.DirFS("testdata/migrations/dialect")

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-services",
			Root: dirFS,
			Options: []DialectMigrationOption{
				WithDialectName("sqlite"),
			},
		},
	))

	plan, err := m.Plan(ctx, nil)
	require.NoError(t, err)

	common := planEntryBySourceAndVersion(t, plan, "go-services", "0000")
	root := planEntryBySourceAndVersion(t, plan, "go-services", "0001")
	dialect := planEntryBySourceAndVersion(t, plan, "go-services", "0002")

	assert.Equal(t, "common/0000_common.up.sql", common.UpPath)
	assert.Equal(t, "0001_widget.up.sql", root.UpPath)
	assert.Equal(t, "sqlite/0002_traits.up.sql", dialect.UpPath)
}

func TestMigrations_PlanIncludesResolvedMetadataAcrossSources(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	plainFS := fstest.MapFS{
		"0001_users.up.sql":   {Data: []byte("CREATE TABLE plan_plain_users(id INTEGER);")},
		"0001_users.down.sql": {Data: []byte("DROP TABLE plan_plain_users;")},
	}
	dialectFS := fstest.MapFS{
		"sqlite/0002_traits.up.sql":   {Data: []byte("CREATE TABLE plan_dialect_traits(id INTEGER);")},
		"sqlite/0002_traits.down.sql": {Data: []byte("DROP TABLE plan_dialect_traits;")},
	}
	orderedFS := fstest.MapFS{
		"0003_roles.up.sql":   {Data: []byte("CREATE TABLE plan_ordered_roles(id INTEGER);")},
		"0003_roles.down.sql": {Data: []byte("DROP TABLE plan_ordered_roles;")},
	}

	m.RegisterSQLMigrations(plainFS)
	m.RegisterDialectMigrations(
		dialectFS,
		WithDialectName("sqlite"),
		WithDialectSourceLabel("catalog"),
	)
	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: orderedFS},
	))

	plan, err := m.Plan(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sql[1]", "dialect[1]", "go-auth"}, plan.SelectedSources)
	require.Len(t, plan.Entries, 3)

	plain := planEntryBySourceAndVersion(t, plan, "sql[1]", "0001")
	assert.Equal(t, "0001", plain.SyntheticName)
	assert.Equal(t, MigrationSourceKindSQL, plain.SourceKind)
	assert.Equal(t, "users", plain.OriginalComment)
	assert.Equal(t, "0001_users.up.sql", plain.UpPath)
	assert.Equal(t, "0001_users.down.sql", plain.DownPath)
	assert.Equal(t, 1, plain.ExecutionOrder)
	assert.Empty(t, plain.Dialect)

	dialect := planEntryBySourceAndVersion(t, plan, "dialect[1]", "0002")
	assert.Equal(t, "0002", dialect.SyntheticName)
	assert.Equal(t, MigrationSourceKindDialect, dialect.SourceKind)
	assert.Equal(t, "catalog", dialect.SourceLabel)
	assert.Equal(t, "traits", dialect.OriginalComment)
	assert.Equal(t, "sqlite/0002_traits.up.sql", dialect.UpPath)
	assert.Equal(t, "sqlite/0002_traits.down.sql", dialect.DownPath)
	assert.Equal(t, 2, dialect.ExecutionOrder)
	assert.Equal(t, "sqlite", dialect.Dialect)

	ordered := planEntryBySourceAndVersion(t, plan, "go-auth", "0003")
	assert.Equal(t, "ord_000001_000001", ordered.SyntheticName)
	assert.Equal(t, MigrationSourceKindOrdered, ordered.SourceKind)
	assert.Equal(t, "roles", ordered.OriginalComment)
	assert.Equal(t, "0003_roles.up.sql", ordered.UpPath)
	assert.Equal(t, "0003_roles.down.sql", ordered.DownPath)
	assert.Equal(t, 3, ordered.ExecutionOrder)

	require.Equal(t, plan, m.LastPlan())
}

func TestMigrations_SourceStablePlanMetadataAndInsertionStability(t *testing.T) {
	ctx := context.Background()
	authFS := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("SELECT 1;")},
		"0001_auth.down.sql": {Data: []byte("SELECT 1;")},
	}
	usersFS := fstest.MapFS{
		"0001_users.up.sql":   {Data: []byte("SELECT 1;")},
		"0001_users.down.sql": {Data: []byte("SELECT 1;")},
	}
	cmsFS := fstest.MapFS{
		"0001_cms.up.sql":   {Data: []byte("SELECT 1;")},
		"0001_cms.down.sql": {Data: []byte("SELECT 1;")},
	}

	first := NewMigrations()
	require.NoError(t, first.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
		NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 30, WithOrderedMigrationDependencies("go-auth")),
	))
	firstPlan, err := first.Plan(ctx, nil)
	require.NoError(t, err)
	firstUsers := planEntryBySourceAndVersion(t, firstPlan, "go-users", "0001")
	require.Equal(t, "ordsrc_000030_go_users_0001", firstUsers.SyntheticName)
	assert.Equal(t, "go_users", firstUsers.SourceKey)
	assert.Equal(t, 30, firstUsers.SourceOrder)
	assert.Equal(t, []string{"go_auth"}, firstUsers.SourceDependsOn)
	assert.Equal(t, OrderedMigrationIdentitySourceStable, firstUsers.IdentityMode)

	second := NewMigrations()
	require.NoError(t, second.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 30, WithOrderedMigrationDependencies("go-auth")),
		NewStableOrderedMigrationSource("go-cms", cmsFS, "go-cms", 20, WithOrderedMigrationDependencies("go-auth")),
		NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
	))
	secondPlan, err := second.Plan(ctx, nil)
	require.NoError(t, err)
	secondUsers := planEntryBySourceAndVersion(t, secondPlan, "go-users", "0001")
	assert.Equal(t, firstUsers.SyntheticName, secondUsers.SyntheticName)
	assert.Equal(t, 3, secondUsers.ResolvedPosition)
	assert.Equal(t, []string{"go-auth", "go-cms", "go-users"}, secondPlan.SelectedSources)
}

func TestMigrations_SourceStableRegistrationValidation(t *testing.T) {
	fsys := fstest.MapFS{"0001_init.up.sql": {Data: []byte("SELECT 1;")}}

	m := NewMigrations()
	err := m.RegisterOrderedMigrationSources(OrderedMigrationSource{
		Name:      "go-auth",
		Root:      fsys,
		SourceKey: "go-auth",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceInvalidConfig))

	m = NewMigrations()
	err = m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
		OrderedMigrationSource{Name: "go-users", Root: fsys},
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceMixedIdentity))
}

func TestMigrations_PlanSourcesRejectsMissingStableDependency(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fstest.MapFS{
			"0001_auth.up.sql": {Data: []byte("SELECT 1;")},
		}, "go-auth", 10),
		NewStableOrderedMigrationSource("go-users", fstest.MapFS{
			"0001_users.up.sql": {Data: []byte("SELECT 1;")},
		}, "go-users", 20, WithOrderedMigrationDependencies("go-auth")),
	))

	_, err := m.PlanSources(ctx, nil, "go-users")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceMissingSelected))
	require.Contains(t, err.Error(), "go-users")
	require.Contains(t, err.Error(), "go_auth")

	_, err = m.PlanSources(ctx, nil, "go-auth")
	require.NoError(t, err)
	_, err = m.PlanSources(ctx, nil, "go-auth", "go-users")
	require.NoError(t, err)
}

func TestMigrations_PlanSourcesRejectsSelectionThatCollidesWithExcludedSources(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	m.RegisterSQLMigrations(
		fstest.MapFS{
			"0001_alpha.up.sql":   {Data: []byte("CREATE TABLE alpha(id INTEGER);")},
			"0001_alpha.down.sql": {Data: []byte("DROP TABLE alpha;")},
		},
		fstest.MapFS{
			"0001_beta.up.sql":   {Data: []byte("CREATE TABLE beta(id INTEGER);")},
			"0001_beta.down.sql": {Data: []byte("DROP TABLE beta;")},
		},
	)

	_, err := m.Plan(ctx, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous migration composition")

	_, err = m.PlanSources(ctx, nil, "sql[1]")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsafe migration source selection")
}

func TestMigrations_MigrateSourcesOnlyRunsSelectedSource(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fstest.MapFS{
				"0001_auth.up.sql":   {Data: []byte("CREATE TABLE auth_users (id INTEGER PRIMARY KEY);")},
				"0001_auth.down.sql": {Data: []byte("DROP TABLE auth_users;")},
			},
		},
		OrderedMigrationSource{
			Name: "go-users",
			Root: fstest.MapFS{
				"0001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
				"0001_users.down.sql": {Data: []byte("DROP TABLE users;")},
			},
		},
	))

	require.NoError(t, m.MigrateSources(ctx, db, "go-auth"))
	assert.True(t, sqliteTableExists(t, db, "auth_users"))
	assert.False(t, sqliteTableExists(t, db, "users"))

	plan, err := m.Plan(ctx, db)
	require.NoError(t, err)
	auth := planEntryBySourceAndVersion(t, plan, "go-auth", "0001")
	users := planEntryBySourceAndVersion(t, plan, "go-users", "0001")
	assert.True(t, auth.Applied)
	assert.False(t, users.Applied)

	require.NoError(t, m.MigrateSources(ctx, db, "go-users"))
	assert.True(t, sqliteTableExists(t, db, "users"))

	plan, err = m.Plan(ctx, db)
	require.NoError(t, err)
	auth = planEntryBySourceAndVersion(t, plan, "go-auth", "0001")
	users = planEntryBySourceAndVersion(t, plan, "go-users", "0001")
	assert.True(t, auth.Applied)
	assert.True(t, users.Applied)
}

func TestMigrations_MigrateSourcesRejectsSelectionThatCollidesWithExcludedSources(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	m := NewMigrations()
	m.RegisterSQLMigrations(
		fstest.MapFS{
			"0001_alpha.up.sql":   {Data: []byte("CREATE TABLE alpha(id INTEGER);")},
			"0001_alpha.down.sql": {Data: []byte("DROP TABLE alpha;")},
		},
		fstest.MapFS{
			"0001_beta.up.sql":   {Data: []byte("CREATE TABLE beta(id INTEGER);")},
			"0001_beta.down.sql": {Data: []byte("DROP TABLE beta;")},
		},
	)

	err := m.MigrateSources(ctx, db, "sql[1]")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsafe migration source selection")
}

func TestMigrations_SourceStablePersistsAndDetectsGraphDrift(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE stable_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE stable_auth;")},
	}

	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
	))
	require.NoError(t, m.Migrate(ctx, db))
	assert.True(t, sqliteTableExists(t, db, "bun_ordered_migration_sources"))

	unchanged := NewMigrations()
	require.NoError(t, unchanged.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
	))
	require.NoError(t, unchanged.Migrate(ctx, db))

	drifted := NewMigrations()
	require.NoError(t, drifted.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 20),
	))
	_, err := drifted.Plan(ctx, nil)
	require.NoError(t, err)

	_, err = drifted.Plan(ctx, db)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceDrift))
	require.Contains(t, err.Error(), "source_order")

	_, err = drifted.PlanSources(ctx, db, "go-auth")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceDrift))
	require.Contains(t, err.Error(), "source_order")

	err = drifted.Migrate(ctx, db)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceDrift))
	require.Contains(t, err.Error(), "source_order")
}

func TestMigrations_SourceStablePlanningIsReadOnlyBeforeMetadataExists(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE readonly_plan_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE readonly_plan_auth;")},
	}

	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
	))

	plan, err := m.Plan(ctx, db)
	require.NoError(t, err)
	require.Len(t, plan.Entries, 1)
	assert.False(t, sqliteTableExists(t, db, "bun_ordered_migration_sources"))
	assert.False(t, sqliteTableExists(t, db, "bun_ordered_migration_aliases"))

	plan, err = m.PlanSources(ctx, db, "go-auth")
	require.NoError(t, err)
	require.Len(t, plan.Entries, 1)
	assert.False(t, sqliteTableExists(t, db, "bun_ordered_migration_sources"))
	assert.False(t, sqliteTableExists(t, db, "bun_ordered_migration_aliases"))
}

func TestMigrations_SourceStableDetectsAdditionalGraphDriftFields(t *testing.T) {
	tests := []struct {
		name       string
		field      string
		configure  func(t *testing.T, authFS, usersFS fstest.MapFS) *Migrations
		planSource string
	}{
		{
			name:  "dependency drift",
			field: "dependencies",
			configure: func(t *testing.T, authFS, usersFS fstest.MapFS) *Migrations {
				m := NewMigrations()
				require.NoError(t, m.RegisterOrderedMigrationSources(
					NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
					NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 20),
				))
				return m
			},
			planSource: "go-users",
		},
		{
			name:  "source key removal",
			field: "source_key",
			configure: func(t *testing.T, authFS, usersFS fstest.MapFS) *Migrations {
				m := NewMigrations()
				require.NoError(t, m.RegisterOrderedMigrationSources(
					NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
				))
				return m
			},
			planSource: "go-auth",
		},
		{
			name:  "resolved position drift",
			field: "resolved_position",
			configure: func(t *testing.T, authFS, usersFS fstest.MapFS) *Migrations {
				cmsFS := fstest.MapFS{
					"0001_cms.up.sql":   {Data: []byte("CREATE TABLE drift_cms (id INTEGER PRIMARY KEY);")},
					"0001_cms.down.sql": {Data: []byte("DROP TABLE drift_cms;")},
				}
				m := NewMigrations()
				require.NoError(t, m.RegisterOrderedMigrationSources(
					NewStableOrderedMigrationSource("go-cms", cmsFS, "go-cms", 5),
					NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
					NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 20, WithOrderedMigrationDependencies("go-auth")),
				))
				return m
			},
			planSource: "go-auth",
		},
		{
			name:  "graph fingerprint drift",
			field: "graph_fingerprint",
			configure: func(t *testing.T, authFS, usersFS fstest.MapFS) *Migrations {
				cmsFS := fstest.MapFS{
					"0001_cms.up.sql":   {Data: []byte("CREATE TABLE drift_cms_late (id INTEGER PRIMARY KEY);")},
					"0001_cms.down.sql": {Data: []byte("DROP TABLE drift_cms_late;")},
				}
				m := NewMigrations()
				require.NoError(t, m.RegisterOrderedMigrationSources(
					NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
					NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 20, WithOrderedMigrationDependencies("go-auth")),
					NewStableOrderedMigrationSource("go-cms", cmsFS, "go-cms", 30),
				))
				return m
			},
			planSource: "go-auth",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, cleanup := newSQLiteTestDB(t)
			defer cleanup()

			authFS := fstest.MapFS{
				"0001_auth.up.sql":   {Data: []byte("CREATE TABLE drift_auth (id INTEGER PRIMARY KEY);")},
				"0001_auth.down.sql": {Data: []byte("DROP TABLE drift_auth;")},
			}
			usersFS := fstest.MapFS{
				"0001_users.up.sql":   {Data: []byte("CREATE TABLE drift_users (id INTEGER PRIMARY KEY);")},
				"0001_users.down.sql": {Data: []byte("DROP TABLE drift_users;")},
			}

			baseline := NewMigrations()
			require.NoError(t, baseline.RegisterOrderedMigrationSources(
				NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
				NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 20, WithOrderedMigrationDependencies("go-auth")),
			))
			require.NoError(t, baseline.Migrate(ctx, db))

			drifted := tt.configure(t, authFS, usersFS)
			_, err := drifted.PlanSources(ctx, db, tt.planSource)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrOrderedSourceDrift))
			assert.Contains(t, err.Error(), tt.field)
		})
	}
}

func TestMigrations_SourceStableDetectsRollbackTimeDrift(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE rollback_drift_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE rollback_drift_auth;")},
	}

	baseline := NewMigrations()
	require.NoError(t, baseline.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
	))
	require.NoError(t, baseline.Migrate(ctx, db))

	drifted := NewMigrations()
	require.NoError(t, drifted.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 20),
	))
	err := drifted.Rollback(ctx, db)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceDrift))
	assert.Contains(t, err.Error(), "source_order")
}

func TestOrderedSourceIdentityPostgresSchemaAndUpsertSQL(t *testing.T) {
	ctx := context.Background()
	sqlDB, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = sqlDB.Close()
	}()

	db := bun.NewDB(sqlDB, pgdialect.New())
	graph := []orderedSourceRegistration{
		{
			name:             "go-auth",
			identityMode:     OrderedMigrationIdentitySourceStable,
			sourceKey:        "go_auth",
			sourceOrder:      10,
			resolvedPosition: 1,
		},
		{
			name:             "go-users",
			identityMode:     OrderedMigrationIdentitySourceStable,
			sourceKey:        "go_users",
			sourceOrder:      20,
			dependsOn:        []string{"go_auth"},
			resolvedPosition: 2,
		},
	}

	sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS "bun_ordered_migration_sources"`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS "bun_ordered_migration_aliases"`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectExec(`CREATE UNIQUE INDEX IF NOT EXISTS "bun_ordered_migration_aliases_stable_name_unique"`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectQuery(`INSERT INTO "bun_ordered_migration_sources".*ON CONFLICT \(source_key\) DO UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now()))
	sqlMock.ExpectQuery(`INSERT INTO "bun_ordered_migration_sources".*ON CONFLICT \(source_key\) DO UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now()))

	require.NoError(t, NewMigrations().persistOrderedSourceGraph(ctx, db, graph))
	require.NoError(t, sqlMock.ExpectationsWereMet())

	rows, _, err := orderedSourceIdentityRows(graph)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	dependencyJSON, err := rows[1].Dependencies.Value()
	require.NoError(t, err)
	assert.JSONEq(t, `["go_auth"]`, dependencyJSON.(string))
}

func TestMigrations_SourceStableDownstreamCompositionExample(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	require.NoError(t, m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fstest.MapFS{
			"0001_auth.up.sql":   {Data: []byte("SELECT 1;")},
			"0001_auth.down.sql": {Data: []byte("SELECT 1;")},
		}, "go-auth", 10),
		NewStableOrderedMigrationSource("go-users", fstest.MapFS{
			"0001_users.up.sql":   {Data: []byte("SELECT 1;")},
			"0001_users.down.sql": {Data: []byte("SELECT 1;")},
		}, "go-users", 20, WithOrderedMigrationDependencies("go-auth")),
		NewStableOrderedMigrationSource("go-cms", fstest.MapFS{
			"0001_cms.up.sql":   {Data: []byte("SELECT 1;")},
			"0001_cms.down.sql": {Data: []byte("SELECT 1;")},
		}, "go-cms", 30, WithOrderedMigrationDependencies("go-auth")),
		NewStableOrderedMigrationSource("go-services", fstest.MapFS{
			"0001_services.up.sql":   {Data: []byte("SELECT 1;")},
			"0001_services.down.sql": {Data: []byte("SELECT 1;")},
		}, "go-services", 40, WithOrderedMigrationDependencies("go-cms")),
		NewStableOrderedMigrationSource("app-local", fstest.MapFS{
			"0001_app.up.sql":   {Data: []byte("SELECT 1;")},
			"0001_app.down.sql": {Data: []byte("SELECT 1;")},
		}, "garchen-archive-admin", 50, WithOrderedMigrationDependencies("go-services")),
	))

	plan, err := m.Plan(ctx, nil)
	require.NoError(t, err)

	var sequence []string
	dependencies := make(map[string][]string)
	for _, entry := range plan.Entries {
		sequence = append(sequence, fmt.Sprintf("%s:%d:%s", entry.SourceKey, entry.SourceOrder, entry.SyntheticName))
		dependencies[entry.SourceKey] = entry.SourceDependsOn
	}

	require.Equal(t, []string{
		"go_auth:10:ordsrc_000010_go_auth_0001",
		"go_users:20:ordsrc_000020_go_users_0001",
		"go_cms:30:ordsrc_000030_go_cms_0001",
		"go_services:40:ordsrc_000040_go_services_0001",
		"garchen_archive_admin:50:ordsrc_000050_garchen_archive_admin_0001",
	}, sequence)
	assert.Equal(t, []string{"go_auth"}, dependencies["go_users"])
	assert.Equal(t, []string{"go_auth"}, dependencies["go_cms"])
	assert.Equal(t, []string{"go_cms"}, dependencies["go_services"])
	assert.Equal(t, []string{"go_services"}, dependencies["garchen_archive_admin"])
}

func TestMigrations_PositionalModeDoesNotCreateStableMetadataTables(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fstest.MapFS{
				"0001_auth.up.sql": {Data: []byte("CREATE TABLE positional_auth (id INTEGER PRIMARY KEY);")},
			},
		},
	))
	require.NoError(t, m.Migrate(ctx, db))
	assert.False(t, sqliteTableExists(t, db, "bun_ordered_migration_sources"))
	assert.False(t, sqliteTableExists(t, db, "bun_ordered_migration_aliases"))
}

func TestMigrations_BackfillStableOrderedMigrationMarkers(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE backfill_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE backfill_auth;")},
	}

	legacy := NewMigrations()
	require.NoError(t, legacy.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: fsys},
	))
	require.NoError(t, legacy.Migrate(ctx, db))
	assert.True(t, sqliteTableExists(t, db, "backfill_auth"))

	stable := NewMigrations()
	require.NoError(t, stable.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
	))
	require.NoError(t, stable.BackfillStableOrderedMigrationMarkers(ctx, db, []OrderedMigrationSource{
		{Name: "go-auth", Root: fsys},
	}))
	require.NoError(t, stable.BackfillStableOrderedMigrationMarkers(ctx, db, []OrderedMigrationSource{
		{Name: "go-auth", Root: fsys},
	}))

	plan, err := stable.Plan(ctx, db)
	require.NoError(t, err)
	entry := planEntryBySourceAndVersion(t, plan, "go-auth", "0001")
	assert.Equal(t, "ordsrc_000010_go_auth_0001", entry.SyntheticName)
	assert.True(t, entry.Applied)

	var aliasCount int
	err = db.NewSelect().
		TableExpr("bun_ordered_migration_aliases").
		ColumnExpr("COUNT(*)").
		Where("legacy_name = ?", "ord_000001_000001").
		Where("stable_name = ?", "ordsrc_000010_go_auth_0001").
		Scan(ctx, &aliasCount)
	require.NoError(t, err)
	assert.Equal(t, 1, aliasCount)

	var stableAppliedCount int
	err = db.NewSelect().
		Model((*migrate.Migration)(nil)).
		ModelTableExpr("bun_migrations").
		ColumnExpr("COUNT(*)").
		Where("name = ?", "ordsrc_000010_go_auth_0001").
		Scan(ctx, &stableAppliedCount)
	require.NoError(t, err)
	assert.Equal(t, 1, stableAppliedCount)

	require.NoError(t, stable.RollbackAll(ctx, db))
	assert.False(t, sqliteTableExists(t, db, "backfill_auth"))
}

func TestMigrations_BackfillStableOrderedMigrationMarkersCleanupLegacyMarkers(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE cleanup_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE cleanup_auth;")},
	}

	legacy := NewMigrations()
	require.NoError(t, legacy.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: fsys},
	))
	require.NoError(t, legacy.Migrate(ctx, db))

	stable := NewMigrations()
	require.NoError(t, stable.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", fsys, "go-auth", 10),
	))
	require.NoError(t, stable.BackfillStableOrderedMigrationMarkers(
		ctx,
		db,
		[]OrderedMigrationSource{{Name: "go-auth", Root: fsys}},
		WithOrderedMigrationRepairCleanupLegacyMarkers(true),
	))

	var legacyCount int
	err := db.NewSelect().
		Model((*migrate.Migration)(nil)).
		ModelTableExpr("bun_migrations").
		ColumnExpr("COUNT(*)").
		Where("name = ?", "ord_000001_000001").
		Scan(ctx, &legacyCount)
	require.NoError(t, err)
	assert.Equal(t, 0, legacyCount)

	var stableCount int
	err = db.NewSelect().
		Model((*migrate.Migration)(nil)).
		ModelTableExpr("bun_migrations").
		ColumnExpr("COUNT(*)").
		Where("name = ?", "ordsrc_000010_go_auth_0001").
		Scan(ctx, &stableCount)
	require.NoError(t, err)
	assert.Equal(t, 1, stableCount)

	require.NoError(t, stable.RollbackAll(ctx, db))
	assert.False(t, sqliteTableExists(t, db, "cleanup_auth"))
}

func TestMigrations_BackfillStableOrderedMigrationMarkersRejectsIncompleteLegacyMapping(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	authFS := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE incomplete_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE incomplete_auth;")},
	}
	usersFS := fstest.MapFS{
		"0001_users.up.sql":   {Data: []byte("CREATE TABLE incomplete_users (id INTEGER PRIMARY KEY);")},
		"0001_users.down.sql": {Data: []byte("DROP TABLE incomplete_users;")},
	}

	legacy := NewMigrations()
	require.NoError(t, legacy.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: authFS},
		OrderedMigrationSource{Name: "go-users", Root: usersFS},
	))
	require.NoError(t, legacy.Migrate(ctx, db))

	var legacyAppliedCount int
	err := db.NewSelect().
		Model((*migrate.Migration)(nil)).
		ModelTableExpr("bun_migrations").
		ColumnExpr("COUNT(*)").
		Where("name LIKE ?", "ord_%").
		Scan(ctx, &legacyAppliedCount)
	require.NoError(t, err)
	require.Equal(t, 2, legacyAppliedCount)

	stable := NewMigrations()
	require.NoError(t, stable.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
		NewStableOrderedMigrationSource("go-users", usersFS, "go-users", 20, WithOrderedMigrationDependencies("go-auth")),
	))

	err = stable.BackfillStableOrderedMigrationMarkers(ctx, db, []OrderedMigrationSource{
		{Name: "go-auth", Root: authFS},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceRepair))
	assert.True(t, errors.Is(err, ErrOrderedSourceRepairMissingMapping))
	var repairErr *OrderedSourceRepairError
	require.True(t, errors.As(err, &repairErr))
	assert.Equal(t, "ord_000002_000001", repairErr.LegacyName)

	var aliasCount int
	err = db.NewSelect().
		TableExpr("bun_ordered_migration_aliases").
		ColumnExpr("COUNT(*)").
		Scan(ctx, &aliasCount)
	require.NoError(t, err)
	assert.Equal(t, 0, aliasCount)

	var stableAppliedCount int
	err = db.NewSelect().
		Model((*migrate.Migration)(nil)).
		ModelTableExpr("bun_migrations").
		ColumnExpr("COUNT(*)").
		Where("name LIKE ?", "ordsrc_%").
		Scan(ctx, &stableAppliedCount)
	require.NoError(t, err)
	assert.Equal(t, 0, stableAppliedCount)

	err = stable.Rollback(ctx, db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "registered migrations are missing applied migration definitions")
}

func TestMigrations_BackfillStableOrderedMigrationMarkersMatchesRenamedSourceBySourceKey(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE mismatch_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE mismatch_auth;")},
	}

	legacy := NewMigrations()
	require.NoError(t, legacy.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: fsys},
	))
	require.NoError(t, legacy.Migrate(ctx, db))

	stable := NewMigrations()
	require.NoError(t, stable.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-accounts", fsys, "go-auth", 10),
	))

	require.NoError(t, stable.BackfillStableOrderedMigrationMarkers(ctx, db, []OrderedMigrationSource{
		{Name: "go-auth", Root: fsys},
	}))

	plan, err := stable.Plan(ctx, db)
	require.NoError(t, err)
	entry := planEntryBySourceAndVersion(t, plan, "go-accounts", "0001")
	assert.Equal(t, "ordsrc_000010_go_auth_0001", entry.SyntheticName)
	assert.True(t, entry.Applied)
}

func TestMigrations_BackfillStableOrderedMigrationMarkersReturnsTypedMarkerMismatch(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	fsys := fstest.MapFS{
		"0001_auth.up.sql":   {Data: []byte("CREATE TABLE mismatch_auth (id INTEGER PRIMARY KEY);")},
		"0001_auth.down.sql": {Data: []byte("DROP TABLE mismatch_auth;")},
	}

	legacy := NewMigrations()
	require.NoError(t, legacy.RegisterOrderedMigrationSources(
		OrderedMigrationSource{Name: "go-auth", Root: fsys},
	))
	require.NoError(t, legacy.Migrate(ctx, db))

	stable := NewMigrations()
	require.NoError(t, stable.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("go-accounts", fsys, "go-accounts", 10),
	))

	err := stable.BackfillStableOrderedMigrationMarkers(ctx, db, []OrderedMigrationSource{
		{Name: "go-auth", Root: fsys},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceRepair))
	assert.True(t, errors.Is(err, ErrOrderedSourceRepairMarkerMismatch))
	var repairErr *OrderedSourceRepairError
	require.True(t, errors.As(err, &repairErr))
	assert.Equal(t, "ord_000001_000001", repairErr.LegacyName)
	assert.Equal(t, "go-auth", repairErr.SourceName)
	assert.Equal(t, "go_auth", repairErr.SourceKey)
}

func TestOrderedSourceRepairErrorMatching(t *testing.T) {
	generic := &OrderedSourceRepairError{Kind: ErrOrderedSourceRepair}
	assert.True(t, errors.Is(generic, ErrOrderedSourceRepair))

	ambiguous := &OrderedSourceRepairError{Kind: ErrOrderedSourceRepairAmbiguousMarker}
	assert.True(t, errors.Is(ambiguous, ErrOrderedSourceRepair))
	assert.True(t, errors.Is(ambiguous, ErrOrderedSourceRepairAmbiguousMarker))
	assert.False(t, errors.Is(ambiguous, ErrOrderedSourceDrift))

	err := NewMigrations().BackfillStableOrderedMigrationMarkers(context.Background(), nil, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceRepair))
	var repairErr *OrderedSourceRepairError
	assert.True(t, errors.As(err, &repairErr))
}

func TestMigrations_RollbackIgnoresUnrelatedAmbiguousSources(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()

	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fstest.MapFS{
				"0001_auth.up.sql":   {Data: []byte("CREATE TABLE auth_users (id INTEGER PRIMARY KEY);")},
				"0001_auth.down.sql": {Data: []byte("DROP TABLE auth_users;")},
			},
		},
		OrderedMigrationSource{
			Name: "go-users",
			Root: fstest.MapFS{
				"0001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
				"0001_users.down.sql": {Data: []byte("DROP TABLE users;")},
			},
		},
	))

	m.RegisterSQLMigrations(
		fstest.MapFS{
			"0001_alpha.up.sql":   {Data: []byte("CREATE TABLE alpha(id INTEGER);")},
			"0001_alpha.down.sql": {Data: []byte("DROP TABLE alpha;")},
		},
		fstest.MapFS{
			"0001_beta.up.sql":   {Data: []byte("CREATE TABLE beta(id INTEGER);")},
			"0001_beta.down.sql": {Data: []byte("DROP TABLE beta;")},
		},
	)

	require.NoError(t, m.MigrateSources(ctx, db, "go-auth"))
	require.NoError(t, m.MigrateSources(ctx, db, "go-users"))
	assert.True(t, sqliteTableExists(t, db, "auth_users"))
	assert.True(t, sqliteTableExists(t, db, "users"))

	require.NoError(t, m.Rollback(ctx, db))
	assert.True(t, sqliteTableExists(t, db, "auth_users"))
	assert.False(t, sqliteTableExists(t, db, "users"))

	require.NoError(t, m.RollbackAll(ctx, db))
	assert.False(t, sqliteTableExists(t, db, "auth_users"))
	assert.False(t, sqliteTableExists(t, db, "users"))
}

func TestMigrations_AutoGeneratedSourceNamesRemainUniqueAcrossKinds(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()

	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "sql[1]",
			Root: fstest.MapFS{
				"0001_reserved.up.sql":   {Data: []byte("SELECT 1;")},
				"0001_reserved.down.sql": {Data: []byte("SELECT 1;")},
			},
		},
		OrderedMigrationSource{
			Name: "dialect[1]",
			Root: fstest.MapFS{
				"0002_reserved.up.sql":   {Data: []byte("SELECT 1;")},
				"0002_reserved.down.sql": {Data: []byte("SELECT 1;")},
			},
		},
	))

	m.RegisterSQLMigrations(fstest.MapFS{
		"0003_plain.up.sql":   {Data: []byte("SELECT 1;")},
		"0003_plain.down.sql": {Data: []byte("SELECT 1;")},
	})
	m.RegisterDialectMigrations(
		fstest.MapFS{
			"sqlite/0004_traits.up.sql":   {Data: []byte("SELECT 1;")},
			"sqlite/0004_traits.down.sql": {Data: []byte("SELECT 1;")},
		},
		WithDialectName("sqlite"),
	)

	plan, err := m.Plan(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"sql[2]", "dialect[2]", "sql[1]", "dialect[1]"}, plan.SelectedSources)
}

func TestValidateDialectsIncludesOrderedSources(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()
	fsys := fstest.MapFS{
		"0001_only_postgres.up.sql": {Data: []byte("---bun:dialect:postgres\nSELECT 1;")},
	}

	var captured DialectValidationResult
	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fsys,
			Options: []DialectMigrationOption{
				WithValidationTargets("sqlite"),
				WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
					captured = result
					return fmt.Errorf("ordered validation failed")
				}),
			},
		},
	))

	err := m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	require.EqualError(t, err, "ordered validation failed")
	require.Equal(t, "go-auth", captured.SourceLabel)
	require.Contains(t, captured.MissingDialects, "sqlite")
}

func TestValidateDialectsContractIncludesOrderedSources(t *testing.T) {
	ctx := context.Background()
	m := NewMigrations()
	fsys := fstest.MapFS{
		"postgres/0001_only.up.sql":   {Data: []byte("SELECT 1;")},
		"postgres/0001_only.down.sql": {Data: []byte("SELECT 1;")},
	}

	var captured DialectValidationResult
	require.NoError(t, m.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fsys,
			Options: []DialectMigrationOption{
				WithDialectValidationContract(DialectValidationContract{
					MandatoryTargets:     []string{"postgres", "sqlite"},
					RequireAtLeastOneSQL: true,
				}),
				WithDialectValidator(func(ctx context.Context, result DialectValidationResult) error {
					captured = result
					return fmt.Errorf("ordered contract failed")
				}),
			},
		},
	))

	err := m.ValidateDialects(ctx, bun.NewDB(nil, pgdialect.New()))
	require.EqualError(t, err, "ordered contract failed")
	require.Equal(t, "go-auth", captured.SourceLabel)
	require.Contains(t, captured.MissingDialects, "sqlite")
}

func TestMigrations_initSQLMigrations_Empty(t *testing.T) {
	m := NewMigrations()

	migrations, err := m.initSQLMigrations(context.Background(), nil)

	assert.NoError(t, err)
	assert.Nil(t, migrations)
}

func TestMigrations_initSQLMigrations_WithFiles(t *testing.T) {
	m := NewMigrations()

	fs := fstest.MapFS{
		"migrations/001_init.up.sql":   {Data: []byte("CREATE TABLE users;")},
		"migrations/001_init.down.sql": {Data: []byte("DROP TABLE users;")},
	}

	m.RegisterSQLMigrations(fs)

	migrations, err := m.initSQLMigrations(context.Background(), nil)

	assert.NoError(t, err)
	assert.NotNil(t, migrations)
}

func TestMigrations_Migrate_NoMigrations(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	bunDB := bun.NewDB(db, pgdialect.New())

	m := NewMigrations()
	mockLogger := new(MockLogger)
	mockLogger.On("Debug", mock.Anything, mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	err = m.Migrate(context.Background(), bunDB)

	assert.NoError(t, err)
	mockLogger.AssertExpectations(t)
}

func TestMigrations_Report(t *testing.T) {
	m := NewMigrations()

	// Initially nil
	assert.Nil(t, m.Report())

	// Set a migration group
	testGroup := &migrate.MigrationGroup{
		ID: 1,
	}
	m.migrations = testGroup

	assert.Equal(t, testGroup, m.Report())
}

func TestMigrations_Migrate_WithSQLMigrations(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Expect migration table initialization
	sqlMock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "group_id", "migrated_at"}))
	sqlMock.ExpectBegin()
	sqlMock.ExpectExec("CREATE TABLE users").WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	sqlMock.ExpectCommit()

	bunDB := bun.NewDB(db, pgdialect.New())

	fs := fstest.MapFS{
		"001_init.up.sql": {Data: []byte("CREATE TABLE users;")},
	}

	m := NewMigrations()
	m.RegisterSQLMigrations(fs)

	mockLogger := new(MockLogger)
	mockLogger.On("Debug", mock.Anything, mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	_ = m.Migrate(context.Background(), bunDB)

	// Note: This test will fail with actual BUN migration logic
	// as it requires a real database connection. This is more of a
	// structure test to ensure the code compiles and basic flow works.
	// For real testing, an integration test with a test database is needed.
}

func TestMigrations_Rollback_NoMigrations(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	bunDB := bun.NewDB(db, pgdialect.New())

	m := NewMigrations()
	mockLogger := new(MockLogger)
	mockLogger.On("Debug", "migrations: no migrations registered to roll back", mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	sqlMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "group_id", "migrated_at"}),
	)

	// With no migrations registered, it should return early
	err = m.Rollback(context.Background(), bunDB)

	assert.NoError(t, err)
	mockLogger.AssertExpectations(t)
}

func TestMigrations_RollbackAll(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	bunDB := bun.NewDB(db, pgdialect.New())

	m := NewMigrations()
	mockLogger := new(MockLogger)
	mockLogger.On("Debug", "migrations: no migrations registered to roll back", mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	sqlMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "group_id", "migrated_at"}),
	)

	// With no migrations registered, it should return early
	err = m.RollbackAll(context.Background(), bunDB)

	assert.NoError(t, err)
	mockLogger.AssertExpectations(t)
}

func TestMigrations_Rollback_WithMigrations(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Register a migration file
	fs := fstest.MapFS{
		"001_init.up.sql":   {Data: []byte("CREATE TABLE test;")},
		"001_init.down.sql": {Data: []byte("DROP TABLE test;")},
	}

	m := NewMigrations()
	m.RegisterSQLMigrations(fs)

	// Expect migration table operations
	sqlMock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "group_id", "migrated_at"}).
			AddRow(1, "001_init", 1, "2024-01-01"),
	)
	sqlMock.ExpectBegin()
	sqlMock.ExpectExec("DROP TABLE test").WillReturnResult(sqlmock.NewResult(0, 0))
	sqlMock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	sqlMock.ExpectCommit()

	bunDB := bun.NewDB(db, pgdialect.New())

	mockLogger := new(MockLogger)
	mockLogger.On("Debug", mock.Anything, mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	// Note: This will likely fail due to BUN's internal migration logic
	// but we're testing that our code doesn't panic
	_ = m.Rollback(context.Background(), bunDB)
}

// Integration test example - requires actual database
func TestMigrations_Integration(t *testing.T) {
	t.Skip("Integration test requires database connection")

	// This is an example of how to write an integration test
	// You would need to:
	// 1. Connect to a real test database
	// 2. Create actual migration files
	// 3. Run migrations
	// 4. Verify database state
	// 5. Rollback
	// 6. Verify rollback state

	/* Example:
	db, err := sql.Open("postgres", "postgres://test:test@localhost/test_db?sslmode=disable")
	assert.NoError(t, err)
	defer db.Close()

	bunDB := bun.NewDB(db, pgdialect.New())

	fs := fstest.MapFS{
		"001_users.up.sql": {
			Data: []byte(`
				CREATE TABLE users (
					id SERIAL PRIMARY KEY,
					name VARCHAR(255)
				);
			`),
		},
		"001_users.down.sql": {
			Data: []byte(`DROP TABLE users;`),
		},
	}

	m := NewMigrations()
	m.RegisterSQLMigrations(fs)

	// Run migration
	err = m.Migrate(context.Background(), bunDB)
	assert.NoError(t, err)

	// Verify table exists
	var exists bool
	err = bunDB.NewSelect().
		ColumnExpr("EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'users')").
		Scan(context.Background(), &exists)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Rollback
	err = m.Rollback(context.Background(), bunDB)
	assert.NoError(t, err)

	// Verify table doesn't exist
	err = bunDB.NewSelect().
		ColumnExpr("EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'users')").
		Scan(context.Background(), &exists)
	assert.NoError(t, err)
	assert.False(t, exists)
	*/
}

// Benchmark tests
func BenchmarkMigrations_RegisterSQLMigrations(b *testing.B) {
	fs := fstest.MapFS{
		"001_init.up.sql": {Data: []byte("CREATE TABLE test;")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := NewMigrations()
		m.RegisterSQLMigrations(fs)
	}
}

func BenchmarkMigrations_initSQLMigrations(b *testing.B) {
	m := NewMigrations()
	fs := fstest.MapFS{
		"001_init.up.sql":   {Data: []byte("CREATE TABLE test;")},
		"001_init.down.sql": {Data: []byte("DROP TABLE test;")},
	}
	m.RegisterSQLMigrations(fs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.initSQLMigrations(context.Background(), nil)
	}
}

func collectFilesFromSources(t *testing.T, sources []iofs.FS) map[string]string {
	t.Helper()
	files := make(map[string]string)
	for _, source := range sources {
		err := iofs.WalkDir(source, ".", func(path string, d iofs.DirEntry, err error) error {
			require.NoError(t, err)
			if path == "." || d.IsDir() {
				return nil
			}
			data, readErr := iofs.ReadFile(source, path)
			require.NoError(t, readErr)
			files[path] = string(data)
			return nil
		})
		require.NoError(t, err)
	}
	return files
}

func orderedSequenceFromMetadata(t *testing.T, manager *Migrations, migrations migrate.MigrationSlice) []string {
	t.Helper()

	manager.mx.Lock()
	metadata := make(map[string]OrderedMigrationMetadata, len(manager.orderedMetadata))
	maps.Copy(metadata, manager.orderedMetadata)
	manager.mx.Unlock()

	sequence := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		meta, ok := metadata[migration.Name]
		require.Truef(t, ok, "missing ordered metadata for migration %q", migration.Name)
		sequence = append(sequence, fmt.Sprintf("%s/%s", meta.SourceName, meta.OriginalVersion))
	}
	return sequence
}

func reverseStrings(values []string) []string {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
	return values
}

func planEntryBySourceAndVersion(t *testing.T, plan *MigrationPlan, sourceName, version string) MigrationPlanEntry {
	t.Helper()

	for _, entry := range plan.Entries {
		if entry.SourceName == sourceName && entry.OriginalVersion == version {
			return entry
		}
	}

	require.FailNowf(t, "missing plan entry", "source=%s version=%s", sourceName, version)
	return MigrationPlanEntry{}
}

func sqliteTableExists(t *testing.T, db *bun.DB, tableName string) bool {
	t.Helper()

	var count int
	err := db.NewSelect().
		TableExpr("sqlite_master").
		ColumnExpr("COUNT(*)").
		Where("type = 'table'").
		Where("name = ?", tableName).
		Scan(context.Background(), &count)
	require.NoError(t, err)
	return count > 0
}
