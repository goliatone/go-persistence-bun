# Go Persistence BUN

A package for managing database connections, migrations, and fixtures using [BUN](https://bun.uptrace.dev/).

## Installation

```bash
go get github.com/goliatone/go-persistence-bun
```

## Usage

### Basic Setup

```go
import (
    "database/sql"
    "time"
    
    persistence "github.com/goliatone/go-persistence-bun"
    "github.com/uptrace/bun/dialect/pgdialect"
    _ "github.com/lib/pq" // PostgreSQL driver
)

// Define your configuration struct that implements the Config interface
type Config struct {
    Debug          bool
    Driver         string
    Server         string
    PingTimeout    time.Duration
    OtelIdentifier string
}

func (c *Config) GetDebug() bool {
    return c.Debug
}

func (c *Config) GetDriver() string {
    return c.Driver
}

func (c *Config) GetServer() string {
    return c.Server
}

func (c *Config) GetPingTimeout() time.Duration {
    return c.PingTimeout
}

func (c *Config) GetOtelIdentifier() string {
    return c.OtelIdentifier
}

// Initialize the client
config := &Config{
    Driver:      persistence.DefaultDriver, // "postgres"
    Server:      "localhost:5432",
    PingTimeout: 5 * time.Second,
}

// Create connection string (example for PostgreSQL)
connectionString := "postgres://user:password@localhost:5432/myapp?sslmode=disable"

db, err := sql.Open(config.GetDriver(), connectionString)
if err != nil {
    log.Fatal(err)
}

client, err := persistence.New(config, db, pgdialect.New())
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Migrations

```go
// SQL migrations
client.RegisterSQLMigrations(migrations.FS)

// Run migrations
if err := client.Migrate(context.Background()); err != nil {
    log.Fatal(err)
}
```

### Fixtures

```go
// Register fixtures
client.RegisterFixtures(fixtures.FS)

// Load fixtures
if err := client.Seed(context.Background()); err != nil {
    log.Fatal(err)
}
```

### Model Registration

Register models before creating the client to ensure they're available for migrations and fixtures:

```go
type User struct {
    ID   int64  `bun:"id,pk,autoincrement"`
    Name string
}

// Register regular models
persistence.RegisterModel((*User)(nil))

// Register many-to-many relationship models
persistence.RegisterMany2ManyModel((*UserGroup)(nil))
```

## Configuration Options

### Config Interface

The `Config` interface requires the following methods:

- `GetDebug() bool`: Enable debug mode with query logging
- `GetDriver() string`: Database driver (default: "postgres")
- `GetServer() string`: Database server address
- `GetPingTimeout() time.Duration`: Connection ping timeout
- `GetOtelIdentifier() string`: OpenTelemetry identifier for tracing

Optional methods that can be implemented:

- `GetMigrationsEnabled() bool`: Enable/disable migrations
- `GetSeedsEnabled() bool`: Enable/disable seeds/fixtures

### Fixture Options

- `WithTruncateTables()`: Truncate tables before loading fixtures
- `WithDropTables()`: Drop tables before loading fixtures
- `WithFS(dir fs.FS)`: Add filesystem for fixtures/migrations
- `WithTemplateFuncs(funcMap template.FuncMap)`: Add template functions for fixtures
- `WithFileFilter(fn func(path, name string) bool)`: Custom file filtering

## API Reference

### Client Methods

- `New(cfg Config, sqlDB *sql.DB, dialect schema.Dialect) (*Client, error)`: Create a new client
- `DB() *bun.DB`: Get the underlying BUN database instance
- `Check() error`: Check database connection
- `MustConnect()`: Panic if connection fails
- `Close() error`: Close database connection
- `SetLogger(logger Logger)`: Set a custom logger

#### Migrations

- `Migrate(ctx context.Context) error`: Run pending migrations
- `RegisterSQLMigrations(migrations ...fs.FS) *Migrations`: Register SQL migrations
- `GetMigrations() *Migrations`: Get migrations manager
- `Rollback(ctx context.Context, opts ...migrate.MigrationOption) error`: Rollback one migration group
- `RollbackAll(ctx context.Context, opts ...migrate.MigrationOption) error`: Rollback all migrations
- `Report() *migrate.MigrationGroup`: Get migration status report

#### Fixtures

- `Seed(ctx context.Context) error`: Load fixtures
- `RegisterFixtures(migrations ...fs.FS) *Fixtures`: Register fixtures
- `GetFixtures() *Fixtures`: Get fixtures manager

#### Service Interface

- `Start(ctx context.Context) error`: Start the service (for service-based architectures)
- `Stop(ctx context.Context) error`: Stop the service
- `Name() string`: Get service name ("persistence")
- `Priority() int`: Get service priority

## Features

- Database connection management with connection pooling
- SQL migrations support via filesystem
- Fixtures/seeds support with template functions
- OpenTelemetry integration for distributed tracing
- Debug mode with comprehensive query logging
- Support for multiple database dialects through BUN
- Model registration for ORM operations
- Many-to-many relationship support
- Transaction support through BUN's API
- Context-aware operations

## License

MIT

Copyright (c) 2024 goliatone
