# Go Persistence BUN

A package for managing database connections, migrations, and fixtures using [BUN](https://bun.uptrace.dev/).

## Installation

```bash
go get github.com/goliatone/go-persistence-bun
```

## Usage

### Basic Setup

```go
type Config struct {
    Debug         bool
    Driver        string
    Server        string
    Database      string
    PingTimeout   time.Duration
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

func (c *Config) GetDatabase() string {
    return c.Database
}

func (c *Config) GetPingTimeout() time.Duration {
    return c.PingTimeout
}

func (c *Config) GetOtelIdentifier() string {
    return c.OtelIdentifier
}

config := &Config{
    Driver: "postgres",
    Server: "localhost:5432",
    Database: "myapp",
    PingTimeout: 5 * time.Second,
}

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

```go
type User struct {
    ID   int64  `bun:"id,pk,autoincrement"`
    Name string
}

persistence.RegisterModel((*User)(nil))
```

## Configuration Options

- `WithTruncateTables()`: Truncate tables before loading fixtures
- `WithDropTables()`: Drop tables before loading fixtures
- `WithFS(dir fs.FS)`: Add filesystem for fixtures/migrations
- `WithTemplateFuncs(funcMap TplMap)`: Add template functions for fixtures
- `WithFileFilter(fn func(path, name string) bool)`: Custom file filtering

## Features

- Database connection management
- SQL and programmatic migrations
- Fixtures support
- OpenTelemetry integration
- Debug mode with query logging

## License

MIT

Copyright (c) 2024 goliatone
