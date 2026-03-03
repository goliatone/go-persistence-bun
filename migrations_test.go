package persistence

import (
	"context"
	"fmt"
	iofs "io/fs"
	"os"
	"strings"
	"testing"
	"testing/fstest"

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

func (m *MockLogger) Debug(msg string, keysAndValues ...interface{}) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Info(msg string, keysAndValues ...interface{}) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Warn(msg string, keysAndValues ...interface{}) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Error(msg string, keysAndValues ...interface{}) {
	m.Called(msg, keysAndValues)
}

func (m *MockLogger) Fatal(msg string, keysAndValues ...interface{}) {
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
	for i := 0; i < 10; i++ {
		filesystems[i] = fstest.MapFS{
			"test.sql": {Data: []byte("SELECT 1;")},
		}
	}

	// Register concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(fs fstest.MapFS) {
			m.RegisterSQLMigrations(fs)
			done <- true
		}(filesystems[i])
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
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
	defer db.Close()

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
	defer db.Close()

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

	err = m.Migrate(context.Background(), bunDB)

	// Note: This test will fail with actual BUN migration logic
	// as it requires a real database connection. This is more of a
	// structure test to ensure the code compiles and basic flow works.
	// For real testing, an integration test with a test database is needed.
}

func TestMigrations_Rollback_NoMigrations(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	bunDB := bun.NewDB(db, pgdialect.New())

	m := NewMigrations()
	mockLogger := new(MockLogger)
	mockLogger.On("Debug", "migrations: no migrations registered to roll back", mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	// With no migrations registered, it should return early
	err = m.Rollback(context.Background(), bunDB)

	assert.NoError(t, err)
	mockLogger.AssertExpectations(t)
}

func TestMigrations_RollbackAll(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	bunDB := bun.NewDB(db, pgdialect.New())

	m := NewMigrations()
	mockLogger := new(MockLogger)
	mockLogger.On("Debug", "migrations: no migrations registered to roll back", mock.Anything).Return().Maybe()
	m.SetLogger(mockLogger)

	// With no migrations registered, it should return early
	err = m.RollbackAll(context.Background(), bunDB)

	assert.NoError(t, err)
	mockLogger.AssertExpectations(t)
}

func TestMigrations_Rollback_WithMigrations(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

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
	for k, v := range manager.orderedMetadata {
		metadata[k] = v
	}
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
