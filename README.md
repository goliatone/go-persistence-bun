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

### Query Hooks

Custom query hooks are configured via `ClientOption`s passed to `New`. Built-in
hooks are opt-in and use config values when enabled.

```go
client, err := persistence.New(
    config,
    db,
    pgdialect.New(),
    persistence.WithQueryHooks(adm.DebugQueryHook()),
    persistence.WithBundebug(), // uses GetDebug() for verbosity
    persistence.WithBunotel(),  // uses GetOtelIdentifier() for DB name
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

To control registration order, use `WithQueryHooksPriority(priority, hooks...)`.

### Transaction Helper (`validation_runs` + `validation_issues`)

Use `RunInTx` to atomically persist a validation run and all related issues.

```go
type ValidationRun struct {
    bun.BaseModel `bun:"table:validation_runs"`
    ID         int64               `bun:"id,pk,autoincrement"`
    MerchantID string              `bun:"merchant_id,notnull"`
    Channel    string              `bun:"channel,notnull"`
    Status     string              `bun:"status,notnull"`
    Counts     persistence.JSONMap `bun:"counts,type:jsonb"` // use type:json for sqlite
}

type ValidationIssue struct {
    bun.BaseModel `bun:"table:validation_issues"`
    ID        int64  `bun:"id,pk,autoincrement"`
    RunID     int64  `bun:"run_id,notnull"`
    Severity  string `bun:"severity,notnull"`
    IssueCode string `bun:"issue_code,notnull"`
    Message   string `bun:"message"`
    Status    string `bun:"status,notnull"`
}

err := persistence.RunInTx(ctx, client.DB(), func(ctx context.Context, tx bun.Tx) error {
    run := &ValidationRun{
        MerchantID: "merchant-1",
        Channel:    "shopify",
        Status:     "running",
        Counts: persistence.JSONMap{
            "blocker": 1,
            "warning": 2,
            "pass":    5,
        },
    }

    if _, err := tx.NewInsert().Model(run).Exec(ctx); err != nil {
        return err
    }
    runID := run.ID

    issues := []*ValidationIssue{
        {RunID: runID, Severity: "blocker", IssueCode: "missing_tax_id", Message: "Tax ID missing", Status: "open"},
        {RunID: runID, Severity: "warning", IssueCode: "missing_logo", Message: "Logo missing", Status: "open"},
    }

    for _, issue := range issues {
        if _, err := tx.NewInsert().Model(issue).Exec(ctx); err != nil {
            return err
        }
    }
    return nil
})
if err != nil {
    log.Fatal(err)
}
```

### Portable JSON Types

Use `JSONMap` and `JSONStringSlice` to round-trip JSON values across Postgres and SQLite.

```go
type ValidationIssue struct {
    bun.BaseModel `bun:"table:validation_issues"`
    ID      int64                     `bun:"id,pk,autoincrement"`
    Meta    persistence.JSONMap       `bun:"meta,type:jsonb"` // use type:json for sqlite
    Tags    persistence.JSONStringSlice `bun:"tags,type:jsonb"`
}
```

For deterministic grouped counts, use `NewGroupedCountQuery`:

```go
var counts []persistence.GroupCount
err := persistence.NewGroupedCountQuery(client.DB(), (*ValidationIssue)(nil), "severity").
    Where("run_id = ?", runID).
    Scan(ctx, &counts)
```

### Migrations

```go
// SQL migrations from embedded filesystem
//go:embed migrations/*.sql
var migrationsFS embed.FS

// Register migrations
client.RegisterSQLMigrations(migrationsFS)

// Run migrations
if err := client.Migrate(context.Background()); err != nil {
    log.Fatal(err)
}

// Rollback last migration group
if err := client.Rollback(context.Background()); err != nil {
    log.Fatal(err)
}

// Dialect-aware migrations (Postgres + SQLite)
//go:embed data/sql/migrations/**/*
var dialectFS embed.FS

client.RegisterDialectMigrations(
    dialectFS,
    persistence.WithDialectSourceLabel("data/sql/migrations"),
    persistence.WithValidationTargets("postgres", "sqlite"),
)
if err := client.ValidateDialects(context.Background()); err != nil {
    log.Fatal(err)
}
```

For detailed migration documentation, see [MIGRATIONS.md](MIGRATIONS.md).

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

Note: `GetDebug()` and `GetOtelIdentifier()` only affect query hooks when
`WithBundebug()` and `WithBunotel()` are supplied to `New(...)`.

### Client Options

- `WithQueryHooks(hooks ...bun.QueryHook)`: Register custom query hooks
- `WithQueryHooksPriority(priority int, hooks ...bun.QueryHook)`: Register hooks with a custom priority
- `WithQueryHookErrorHandler(handler QueryHookErrorHandler)`: Handle invalid hook registration
- `WithBundebug()`: Enable bundebug query logging (uses `GetDebug()` for verbosity)
- `WithBunotel()`: Enable bunotel tracing (uses `GetOtelIdentifier()` for DB name)

### Fixture Options

- `WithTruncateTables()`: Truncate tables before loading fixtures
- `WithDropTables()`: Drop tables before loading fixtures
- `WithFS(dir fs.FS)`: Add filesystem for fixtures/migrations
- `WithTemplateFuncs(funcMap template.FuncMap)`: Add template functions for fixtures
- `WithFileFilter(fn func(path, name string) bool)`: Custom file filtering

### Fixture Template Functions

The fixture loader supports a small set of template functions when rendering seed files:

- `hashid`: Generate a hashid string from a value.
- `hashpwd`: Generate a bcrypt password hash from a value (non-deterministic across runs).

Example usage in a fixture file:

```yaml
users:
  - email: "admin@example.com"
    password: '{{ hashpwd "admin123" }}'
```

## API Reference

### Client Methods

- `New(cfg Config, sqlDB *sql.DB, dialect schema.Dialect, opts ...ClientOption) (*Client, error)`: Create a new client
- `RunInTx(ctx context.Context, db bun.IDB, fn func(ctx context.Context, tx bun.Tx) error) error`: Run writes in one transaction with rollback safety
- `DB() *bun.DB`: Get the underlying BUN database instance
- `Check() error`: Check database connection
- `MustConnect()`: Panic if connection fails
- `Close() error`: Close database connection
- `SetLogger(logger Logger)`: Set a custom logger

#### Migrations

- `Migrate(ctx context.Context) error`: Run pending migrations
- `RegisterSQLMigrations(migrations ...fs.FS) *Migrations`: Register SQL migrations
- `RegisterOrderedMigrationSources(sources ...OrderedMigrationSource) error`: Register ordered, source-aware SQL migration sources
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
- OpenTelemetry integration for distributed tracing (opt-in via `WithBunotel`)
- Debug mode with comprehensive query logging (opt-in via `WithBundebug`)
- Support for multiple database dialects through BUN
- Model registration for ORM operations
- Many-to-many relationship support
- Transaction support through BUN's API
- Additive transaction helper (`RunInTx`) for portable service-level writes
- Portable JSON wrappers (`JSONMap`, `JSONStringSlice`) for Postgres JSONB and SQLite JSON/TEXT
- Context-aware operations

## License

MIT

Copyright (c) 2024 goliatone
