package persistence

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/migrate"
)

func TestCompileOrderedSourceMigrations_LayeredOverrideAndMetadata(t *testing.T) {
	base := fstest.MapFS{
		"0001_alpha.up.sql":   {Data: []byte("CREATE TABLE base_alpha;")},
		"0001_alpha.down.sql": {Data: []byte("DROP TABLE base_alpha;")},
	}
	override := fstest.MapFS{
		"0001_alpha.up.sql":     {Data: []byte("CREATE TABLE override_alpha;")},
		"0002_bravo.up.sql":     {Data: []byte("CREATE TABLE bravo;")},
		"0002_bravo.down.sql":   {Data: []byte("DROP TABLE bravo;")},
		"README.not-sql":        {Data: []byte("ignored")},
		"nested/ignored.up.txt": {Data: []byte("ignored")},
	}

	migrations, metadata, err := compileOrderedSourceMigrations("go-auth", 0, []fs.FS{base, override})
	require.NoError(t, err)
	require.Len(t, migrations, 2)
	require.Len(t, metadata, 2)

	first := migrations[0]
	assert.Equal(t, "ord_000001_000001", first.Name)
	assert.Equal(t, "go-auth_alpha", first.Comment)
	assert.NotNil(t, first.Up)
	assert.NotNil(t, first.Down)
	firstMeta, ok := metadata[first.Name]
	require.True(t, ok)
	assert.Equal(t, OrderedMigrationMetadata{
		SyntheticName:   "ord_000001_000001",
		SourceName:      "go-auth",
		OriginalVersion: "0001",
		OriginalComment: "alpha",
		UpPath:          "0001_alpha.up.sql",
		DownPath:        "0001_alpha.down.sql",
	}, firstMeta)

	second := migrations[1]
	assert.Equal(t, "ord_000001_000002", second.Name)
	assert.Equal(t, "go-auth_bravo", second.Comment)
	assert.NotNil(t, second.Up)
	assert.NotNil(t, second.Down)
	secondMeta, ok := metadata[second.Name]
	require.True(t, ok)
	assert.Equal(t, OrderedMigrationMetadata{
		SyntheticName:   "ord_000001_000002",
		SourceName:      "go-auth",
		OriginalVersion: "0002",
		OriginalComment: "bravo",
		UpPath:          "0002_bravo.up.sql",
		DownPath:        "0002_bravo.down.sql",
	}, secondMeta)
}

func TestCompileOrderedSourceMigrations_RejectDuplicateIdentity(t *testing.T) {
	layer := fstest.MapFS{
		"0001_alpha.up.sql": {Data: []byte("SELECT 1;")},
		"0001_beta.up.sql":  {Data: []byte("SELECT 1;")},
	}

	_, _, err := compileOrderedSourceMigrations("go-auth", 0, []fs.FS{layer})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate migration identity")
}

func TestCompileOrderedSourceMigrations_MigrationExecutionWiring(t *testing.T) {
	base := fstest.MapFS{
		"0001_alpha.up.sql":   {Data: []byte("CREATE TABLE base_alpha;")},
		"0001_alpha.down.sql": {Data: []byte("DROP TABLE base_alpha;")},
	}
	override := fstest.MapFS{
		"0001_alpha.up.sql": {Data: []byte("CREATE TABLE override_alpha;")},
	}

	compiled, _, err := compileOrderedSourceMigrations("go-auth", 0, []fs.FS{base, override})
	require.NoError(t, err)
	require.Len(t, compiled, 1)

	migration := compiled[0]
	migrations := migrate.NewMigrations()
	migrations.Add(migration)

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockDB.ExpectExec("CREATE TABLE override_alpha").WillReturnResult(sqlmock.NewResult(0, 0))
	mockDB.ExpectExec("DROP TABLE base_alpha").WillReturnResult(sqlmock.NewResult(0, 0))

	bunDB := bun.NewDB(db, pgdialect.New())
	migrator := migrate.NewMigrator(bunDB, migrations)

	require.NoError(t, migration.Up(context.Background(), migrator, &migration))
	require.NoError(t, migration.Down(context.Background(), migrator, &migration))
	require.NoError(t, mockDB.ExpectationsWereMet())
}

func TestOrderedMigrations_MetadataMapping(t *testing.T) {
	manager := NewMigrations()
	require.NoError(t, manager.RegisterOrderedMigrationSources(
		OrderedMigrationSource{
			Name: "go-auth",
			Root: fstest.MapFS{
				"0001_auth.up.sql":   {Data: []byte("CREATE TABLE auth_users;")},
				"0001_auth.down.sql": {Data: []byte("DROP TABLE auth_users;")},
			},
		},
	))

	sqlMigrations, err := manager.initSQLMigrations(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, sqlMigrations)

	sorted := sqlMigrations.Sorted()
	require.Len(t, sorted, 1)
	name := sorted[0].Name

	manager.mx.Lock()
	meta, ok := manager.orderedMetadata[name]
	manager.mx.Unlock()
	require.True(t, ok)
	assert.Equal(t, name, meta.SyntheticName)
	assert.Equal(t, "go-auth", meta.SourceName)
	assert.Equal(t, "0001", meta.OriginalVersion)
	assert.Equal(t, "auth", meta.OriginalComment)
	assert.Equal(t, "0001_auth.up.sql", meta.UpPath)
	assert.Equal(t, "0001_auth.down.sql", meta.DownPath)
}
