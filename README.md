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
    persistence.WithDialectValidationContract(persistence.DialectValidationContract{
        MandatoryTargets:                  []string{"postgres", "sqlite"},
        RequireAtLeastOneSQL:              true,
        RequireUpDownPairs:                true,
        RequireVersionParityAcrossTargets: true,
    }),
    persistence.WithValidateOnMigrate(true), // optional: auto-run validation before Migrate
)
if err := client.ValidateDialects(context.Background()); err != nil {
    log.Fatal(err)
}

// Source-stable ordered package migrations
err := client.RegisterOrderedMigrationSources(
    persistence.NewStableOrderedMigrationSource("go-auth", authFS, "go-auth", 10),
    persistence.NewStableOrderedMigrationSource(
        "go-users",
        usersFS,
        "go-users",
        20,
        persistence.WithOrderedMigrationDependencies("go-auth"),
    ),
)
if err != nil {
    log.Fatal(err)
}
```

For detailed migration documentation, see [MIGRATIONS.md](MIGRATIONS.md).

### Fixtures

Fixture directory loading accepts `.yml`, `.yaml`, and `.json` files
case-insensitively. JSON must use Bun's canonical model-and-rows fixture shape,
for example:

```json
[
  {
    "model": "User",
    "rows": [
      { "_id": "admin", "email": "admin@example.com" }
    ]
  }
]
```

```go
// Register fixtures
client.RegisterFixtures(fixtures.FS)

// Load fixtures
if err := client.Seed(context.Background()); err != nil {
    log.Fatal(err)
}
```

Use `WithFixtureTransform` when a source file is not already in Bun's canonical
shape. Transforms run synchronously in registration order after the file is read
and before Bun decodes fields or evaluates templates. Each transform receives
the previous transform's output:

```go
manager := client.RegisterFixtures(fixtures.FS).AddOptions(
    persistence.WithFixtureTransform(decompressFixture),
    persistence.WithFixtureTransform(adaptSourceEnvelope),
)

if err := manager.Load(ctx); err != nil {
    return err
}
```

`FixtureFile.Path` is the exact slash-separated `fs.FS` path and
`FixtureFile.Name` is `path.Base(Path)`. Return `Skip: true` to omit a matched
file after inspecting its content; an empty or nil `Data` value without `Skip`
is still passed to Bun as fixture content. Errors take precedence over `Skip`.

The following application-owned adapter converts a source `{ "data": [...] }`
envelope while skipping an endpoint that does not seed a persistence model:

```go
type customerEnvelope struct {
    Data []struct {
        ExternalID  string `json:"external_id"`
        DisplayName string `json:"display_name"`
    } `json:"data"`
}

func adaptSourceEnvelope(
    ctx context.Context,
    file persistence.FixtureFile,
) (persistence.FixtureTransformResult, error) {
    if err := ctx.Err(); err != nil {
        return persistence.FixtureTransformResult{}, err
    }
    if file.Name == "audit-events.json" {
        return persistence.FixtureTransformResult{Skip: true}, nil
    }
    if file.Name != "customers.json" {
        return persistence.FixtureTransformResult{Data: file.Data}, nil
    }

    var source customerEnvelope
    if err := json.Unmarshal(file.Data, &source); err != nil {
        // Keep returned errors free of fixture values; the cause remains inspectable.
        return persistence.FixtureTransformResult{}, fmt.Errorf("decode customer envelope: %w", err)
    }

    rows := make([]map[string]any, 0, len(source.Data))
    for _, customer := range source.Data {
        rows = append(rows, map[string]any{
            "external_id": customer.ExternalID,
            "name":        customer.DisplayName,
        })
    }
    data, err := json.Marshal([]map[string]any{{
        "model": "Customer",
        "rows":  rows,
    }})
    if err != nil {
        return persistence.FixtureTransformResult{}, fmt.Errorf("encode customer fixture: %w", err)
    }
    return persistence.FixtureTransformResult{Data: data}, nil
}
```

Transforms receive borrowed input bytes, and their successful output becomes
owned by the loading pipeline; callbacks must not retain and mutate either
buffer concurrently. Register all fixture options before the first load.
One manager keeps Bun's table/row state across files and later load calls, and
concurrent loading or option mutation on that manager is unsupported.

When `Load` reports failures from one or more fixture filesystems, all original
causes remain available through `errors.Is` and `errors.As`. Use
`persistence.FixtureFailures(err)` to inspect the safe file, processing stage,
and optional transform index for each failed file. Diagnostic renderers may
allowlist `persistence.FixtureFailuresMetadataKey` to include the same records;
fixture bytes and callback names are never added to this metadata.

`WithFileFilter` remains the authoritative pre-read filter for directory loads
and can exclude unrelated JSON before allocation. Direct `LoadFile` calls use
the exact named path and do not consult the directory filter. Because JSON is
now eligible by default, keep fixtures in a dedicated filesystem or install a
custom filter when other JSON files are colocated.

Fixture transforms are intended for seed/model construction. If imported data
requires domain normalization, provenance, authorization, or state-transition
rules, dispatch the application's business commands instead of writing models
directly through fixtures.

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
- `WithTrucateTables()`: Deprecated compatibility alias for `WithTruncateTables()`
- `WithDropTables()`: Drop tables before loading fixtures
- `WithFS(dir fs.FS)`: Add filesystem for fixtures/migrations
- `WithTemplateFuncs(funcMap template.FuncMap)`: Add template functions for fixtures
- `WithFileFilter(fn func(path, name string) bool)`: Custom file filtering
- `WithFixtureTransform(transform FixtureTransform)`: Inspect, transform, or explicitly skip fixture content before Bun decoding

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
- `MigrateSources(ctx context.Context, sourceNames ...string) error`: Run pending migrations for a selected source subset
- `Plan(ctx context.Context) (*MigrationPlan, error)`: Resolve the current execution plan for all registered sources
- `PlanSources(ctx context.Context, sourceNames ...string) (*MigrationPlan, error)`: Resolve the plan for a selected source subset
- `LastPlan() *MigrationPlan`: Return the last resolved migration plan
- `RegisterSQLMigrations(migrations ...fs.FS) *Migrations`: Register SQL migrations
- `RegisterOrderedMigrationSources(sources ...OrderedMigrationSource) error`: Register ordered, source-aware SQL migration sources
- `NewStableOrderedMigrationSource(...) OrderedMigrationSource`: Build a source-stable ordered source with explicit key/order/dependencies
- `BackfillStableOrderedMigrationMarkers(...) error`: Repair legacy positional ordered markers for source-stable adoption
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
