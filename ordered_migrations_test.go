package persistence

import (
	"context"
	"errors"
	"sort"
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

	migrations, metadata, err := compileOrderedSourceMigrations(testOrderedRegistration("go-auth", 0), []migrationSourceLayer{
		{fsys: base},
		{fsys: override},
	})
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

	_, _, err := compileOrderedSourceMigrations(testOrderedRegistration("go-auth", 0), []migrationSourceLayer{
		{fsys: layer},
	})
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

	compiled, _, err := compileOrderedSourceMigrations(testOrderedRegistration("go-auth", 0), []migrationSourceLayer{
		{fsys: base},
		{fsys: override},
	})
	require.NoError(t, err)
	require.Len(t, compiled, 1)

	migration := compiled[0]
	migrations := migrate.NewMigrations()
	migrations.Add(migration)

	db, mockDB, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	mockDB.ExpectExec("CREATE TABLE override_alpha").WillReturnResult(sqlmock.NewResult(0, 0))
	mockDB.ExpectExec("DROP TABLE base_alpha").WillReturnResult(sqlmock.NewResult(0, 0))

	bunDB := bun.NewDB(db, pgdialect.New())
	migrator := migrate.NewMigrator(bunDB, migrations)

	require.NoError(t, migration.Up(context.Background(), migrator, &migration))
	require.NoError(t, migration.Down(context.Background(), migrator, &migration))
	require.NoError(t, mockDB.ExpectationsWereMet())
}

func TestCompileOrderedSourceMigrations_SourceStableNames(t *testing.T) {
	registration := orderedSourceRegistration{
		name:             "Go Services",
		sequence:         4,
		identityMode:     OrderedMigrationIdentitySourceStable,
		sourceKey:        "go_services",
		sourceOrder:      50,
		dependsOn:        []string{"go_auth"},
		resolvedPosition: 2,
	}
	fsys := fstest.MapFS{
		"0002_beta.up.sql":   {Data: []byte("SELECT 1;")},
		"0002_beta.down.sql": {Data: []byte("SELECT 1;")},
		"0001_alpha.up.sql":  {Data: []byte("SELECT 1;")},
	}

	migrations, metadata, err := compileOrderedSourceMigrations(registration, []migrationSourceLayer{{fsys: fsys}})
	require.NoError(t, err)
	require.Len(t, migrations, 2)

	names := []string{migrations[0].Name, migrations[1].Name}
	assert.Equal(t, []string{
		"ordsrc_000050_go_services_0001",
		"ordsrc_000050_go_services_0002",
	}, names)

	meta := metadata["ordsrc_000050_go_services_0001"]
	assert.Equal(t, "go_services", meta.SourceKey)
	assert.Equal(t, 50, meta.SourceOrder)
	assert.Equal(t, []string{"go_auth"}, meta.SourceDependsOn)
	assert.Equal(t, 2, meta.ResolvedPosition)
	assert.Equal(t, OrderedMigrationIdentitySourceStable, meta.IdentityMode)
}

func TestNormalizeOrderedSourceKey(t *testing.T) {
	cases := map[string]string{
		"Go Services":         "go_services",
		"go-services":         "go_services",
		"GO_services":         "go_services",
		" billing.api v2 !! ": "billing_api_v2",
	}
	for input, expected := range cases {
		actual, err := normalizeOrderedSourceKey(input)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
	_, err := normalizeOrderedSourceKey("!@#$")
	require.Error(t, err)
}

func TestResolveOrderedSourceGraph_SourceStableOrderingAndErrors(t *testing.T) {
	registrations := []orderedSourceRegistration{
		{name: "users", sequence: 0, identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "users", sourceOrder: 30, dependsOn: []string{"auth"}},
		{name: "auth", sequence: 1, identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "auth", sourceOrder: 10},
		{name: "cms", sequence: 2, identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "cms", sourceOrder: 30},
	}
	resolved, err := resolveOrderedSourceGraph(registrations)
	require.NoError(t, err)
	assert.Equal(t, []string{"auth", "cms", "users"}, []string{resolved[0].sourceKey, resolved[1].sourceKey, resolved[2].sourceKey})
	assert.Equal(t, 3, resolved[2].resolvedPosition)

	_, err = resolveOrderedSourceGraph([]orderedSourceRegistration{
		{name: "a", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "dup", sourceOrder: 10},
		{name: "b", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "dup", sourceOrder: 20},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceDuplicateKey))

	_, err = resolveOrderedSourceGraph([]orderedSourceRegistration{
		{name: "a", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "a", sourceOrder: 10, dependsOn: []string{"missing"}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceUnknownDep))

	_, err = resolveOrderedSourceGraph([]orderedSourceRegistration{
		{name: "a", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "a", sourceOrder: 10},
		{name: "b", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "b", sourceOrder: 10, dependsOn: []string{"a"}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceOrderInversion))
}

func TestResolveOrderedSourceGraph_DetectsCycle(t *testing.T) {
	_, err := resolveOrderedSourceGraph([]orderedSourceRegistration{
		{name: "a", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "a", sourceOrder: 10, dependsOn: []string{"b"}},
		{name: "b", identityMode: OrderedMigrationIdentitySourceStable, sourceKey: "b", sourceOrder: 20, dependsOn: []string{"a"}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceCycle))
}

func TestOrderedStableSyntheticNamesSortLexically(t *testing.T) {
	names := []string{
		orderedStableSyntheticMigrationName(MaxOrderedMigrationSourceOrder, "last", "0001"),
		orderedStableSyntheticMigrationName(20, "users", "0001"),
		orderedStableSyntheticMigrationName(10, "auth", "0002"),
		orderedStableSyntheticMigrationName(10, "auth", "0001"),
		orderedStableSyntheticMigrationName(10, "cms", "0001"),
		orderedStableSyntheticMigrationName(99998, "near_last", "0001"),
	}
	sort.Strings(names)
	assert.Equal(t, []string{
		"ordsrc_000010_auth_0001",
		"ordsrc_000010_auth_0002",
		"ordsrc_000010_cms_0001",
		"ordsrc_000020_users_0001",
		"ordsrc_099998_near_last_0001",
		"ordsrc_999999_last_0001",
	}, names)
}

func TestOrderedStableSourceOrderMaximum(t *testing.T) {
	fsys := fstest.MapFS{"0001_init.up.sql": {Data: []byte("SELECT 1;")}}

	m := NewMigrations()
	require.NoError(t, m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("max", fsys, "max", MaxOrderedMigrationSourceOrder),
	))

	m = NewMigrations()
	err := m.RegisterOrderedMigrationSources(
		NewStableOrderedMigrationSource("overflow", fsys, "overflow", MaxOrderedMigrationSourceOrder+1),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOrderedSourceInvalidConfig))
	assert.Contains(t, err.Error(), "exceeds the source-stable maximum")
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

func testOrderedRegistration(name string, sequence int) orderedSourceRegistration {
	return orderedSourceRegistration{
		name:         name,
		sequence:     sequence,
		identityMode: OrderedMigrationIdentityPositional,
	}
}
