package persistence

import (
	"context"
	"database/sql"
	"embed"

	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/goliatone/go-errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

//go:embed testdata/*
var emptyFS embed.FS

// MockConfig implements Config interface for testing
type MockConfig struct {
	mock.Mock
}

func (m *MockConfig) GetDebug() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockConfig) GetDriver() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConfig) GetServer() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConfig) GetDatabase() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConfig) GetPingTimeout() time.Duration {
	args := m.Called()
	return args.Get(0).(time.Duration)
}

func (m *MockConfig) GetOtelIdentifier() string {
	args := m.Called()
	return args.String(0)
}

func TestNew(t *testing.T) {
	// Create a mock DB with driver
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)
	defer db.Close()
	defer resetInit()

	// Setup mock expectations
	mock.ExpectPing()

	mockConfig := new(MockConfig)
	mockConfig.On("GetPingTimeout").Return(5 * time.Second)

	client, err := New(mockConfig, db, pgdialect.New())

	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.NotNil(t, client.db)
	assert.NotNil(t, client.fixtures)
	assert.NotNil(t, client.migrations)

	mockConfig.AssertExpectations(t)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func resetInit() {
	bunDB = nil
	modelsToRegister = []any{}
}

func TestRegisterModel(t *testing.T) {
	type TestModel struct {
		ID   int64 `bun:"id,pk,autoincrement"`
		Name string
	}

	defer resetInit()

	RegisterModel((*TestModel)(nil))

	assert.Contains(t, modelsToRegister, (*TestModel)(nil))
}

func TestFixtures(t *testing.T) {
	defer resetInit()

	mockDB := bun.NewDB(new(sql.DB), pgdialect.New())
	fixtures := NewSeedManager(mockDB)

	t.Run("FileFilter", func(t *testing.T) {
		assert.True(t, fixtures.FileFilter("test.yml", "test.yml"))
		assert.True(t, fixtures.FileFilter("test.yaml", "test.yaml"))
		assert.False(t, fixtures.FileFilter("test.txt", "test.txt"))
	})

	t.Run("WithFS", func(t *testing.T) {
		fsys := fstest.MapFS{
			"test.yml": &fstest.MapFile{Data: []byte("test: data")},
		}
		fixtures.AddOptions(WithFS(fsys))
		fixtures.init()
		assert.Len(t, fixtures.dirs, 1)
	})

	t.Run("WithTrucateTables", func(t *testing.T) {
		fixtures = NewSeedManager(mockDB) // Reset fixtures
		fixtures.AddOptions(WithTrucateTables())
		fixtures.init()
		assert.True(t, fixtures.truncate)
	})

	t.Run("WithDropTables", func(t *testing.T) {
		fixtures = NewSeedManager(mockDB) // Reset fixtures
		fixtures.AddOptions(WithDropTables())
		fixtures.init()
		assert.True(t, fixtures.drop)
	})
}

func TestClient(t *testing.T) {
	defer resetInit()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectPing()

	mockConfig := new(MockConfig)
	mockConfig.On("GetDebug").Return(true)
	mockConfig.On("GetPingTimeout").Return(5 * time.Second)
	mockConfig.On("GetOtelIdentifier").Return("")
	mockConfig.On("GetDriver").Return("postgres")
	mockConfig.On("GetServer").Return("localhost")
	mockConfig.On("GetDatabase").Return("testdb")

	client, err := New(mockConfig, db, pgdialect.New())
	assert.NoError(t, err)

	t.Run("Name", func(t *testing.T) {
		assert.Equal(t, Name, client.Name())
	})

	t.Run("Priority", func(t *testing.T) {
		assert.Equal(t, Priority, client.Priority())
	})

	t.Run("Start", func(t *testing.T) {
		mock.ExpectPing()
		err := client.Start(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Stop", func(t *testing.T) {
		mock.ExpectClose()
		err := client.Stop(context.Background())
		assert.NoError(t, err)
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

type mockFS struct {
	mock.Mock
	fs.FS
}

func (m *mockFS) Open(name string) (fs.File, error) {
	args := m.Called(name)
	return args.Get(0).(fs.File), args.Error(1)
}

func TestFixturesLoad(t *testing.T) {
	mockDB := bun.NewDB(new(sql.DB), pgdialect.New())
	fixtures := NewSeedManager(mockDB, WithFS(emptyFS))

	t.Run("Load Non-Existent File", func(t *testing.T) {
		err := fixtures.LoadFile(context.Background(), "non-existent.yml")
		assert.True(t, errors.Is(err, fs.ErrNotExist))
	})

	t.Run("Initialize Without Options", func(t *testing.T) {
		fixtures.init()
		assert.NotNil(t, fixtures.fixture)
	})
}
