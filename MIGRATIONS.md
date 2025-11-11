# Migrations Documentation

## Overview

The `go-persistence-bun` package provides SQL-based database migrations using the BUN migration system. Migrations allow you to version control your database schema and apply changes in a controlled, reversible manner.

## How Migrations Work

### Migration Lifecycle

1. **Registration**: Migration files are registered from embedded filesystems using `RegisterSQLMigrations()`
2. **Discovery**: The system discovers all `.sql` files in the registered filesystems
3. **Initialization**: Migration tables are created in the database to track applied migrations
4. **Execution**: Migrations are applied in order based on their filenames
5. **Tracking**: Applied migrations are recorded in the database

### File Structure

Migrations use a naming convention to determine execution order:

```
migrations/
├── 00001_create_users_table.up.sql
├── 00001_create_users_table.down.sql
├── 00002_add_email_to_users.up.sql
├── 00002_add_email_to_users.down.sql
└── 00003_create_posts_table.up.sql
└── 00003_create_posts_table.down.sql
```

- **Prefix**: Numeric prefix (e.g., `00001`) determines execution order
- **Name**: Descriptive name for the migration
- **Direction**: `.up.sql` for forward migrations, `.down.sql` for rollbacks
- **Extension**: Must be `.sql`

### Dialect Aware Layouts

When a package needs to support multiple database engines (for example Postgres in production and SQLite for demos), place driver specific SQL in subdirectories. The loader will merge the folders in this order: `common/` → root files → `<dialect>/`.

```
data/sql/migrations/
├── common/
│   ├── 0000_common.up.sql
│   └── 0000_common.down.sql
├── 0001_widget.up.sql
├── 0001_widget.down.sql
├── 0003_annotation.up.sql           # optional annotations (see below)
├── 0003_annotation.down.sql
├── postgres/
│   ├── 0002_traits.up.sql
│   └── 0002_traits.down.sql
└── sqlite/
    ├── 0002_traits.up.sql
    └── 0002_traits.down.sql
```

Statements in the root folder are universal unless you add an annotation to scope them:

> See `testdata/migrations/dialect` for a working example that the unit tests load directly.

```sql
---bun:dialect:postgres
ALTER TABLE widgets ADD COLUMN search tsvector;
```

Comma separated lists are allowed (for example `---bun:dialect:postgres,sqlite`).

## Usage

### Basic Setup

```go
package main

import (
    "context"
    "database/sql"
    "embed"

    persistence "github.com/goliatone/go-persistence-bun"
    "github.com/uptrace/bun/dialect/pgdialect"
    _ "github.com/lib/pq"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func main() {
    // Setup database connection
    db, err := sql.Open("postgres", "postgres://user:pass@localhost/dbname?sslmode=disable")
    if err != nil {
        panic(err)
    }

    // Create persistence client
    client, err := persistence.New(config, db, pgdialect.New())
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Register migrations
    client.RegisterSQLMigrations(migrationsFS)

    // Run migrations
    ctx := context.Background()
    if err := client.Migrate(ctx); err != nil {
        panic(err)
    }
}
```

### Writing Migration Files

#### Up Migration (forward)

`00001_create_users_table.up.sql`:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

#### Down Migration (rollback)

`00001_create_users_table.down.sql`:
```sql
DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
```

### Multiple Migration Sources

You can register migrations from multiple embedded filesystems:

```go
//go:embed core_migrations/*.sql
var coreMigrations embed.FS

//go:embed feature_migrations/*.sql
var featureMigrations embed.FS

// Register both
client.RegisterSQLMigrations(coreMigrations, featureMigrations)
```

### Dialect Specific Registration

When you want the loader to automatically select Postgres or SQLite migrations, use `RegisterDialectMigrations` instead of (or in addition to) `RegisterSQLMigrations`.

```go
//go:embed data/sql/migrations
var migrationsFS embed.FS

dialectFS, err := fs.Sub(migrationsFS, "data/sql/migrations")
if err != nil {
    panic(err)
}

client.RegisterDialectMigrations(
    dialectFS,
    persistence.WithDialectSourceLabel("data/sql/migrations"),
    persistence.WithValidationTargets("postgres", "sqlite"),
)

// optional safety check during startup
if err := client.ValidateDialects(ctx); err != nil {
    log.Fatalf("dialect validation failed: %v", err)
}
```

> **Tip:** Embed the entire `data/sql/migrations` directory (not just `*.sql` files) so the loader can see nested folders such as `common/` or `sqlite/`. Always scope the embedded FS via `fs.Sub(..., "data/sql/migrations")` before registering; the dialect resolver expects its root to map directly to the migrations layout.

By default the loader inspects `db.Dialect().Name()` to pick the correct folder, but you can override it via `WithDialectName` or `WithDialectResolver`.

#### Validation Hooks

`WithValidationTargets` declares which dialects must be present. If validation fails, the default callback panics with a message that lists the missing directories/files. To soften the behavior, supply your own function:

```go
client.RegisterDialectMigrations(
    migrationsFS,
    persistence.WithValidationTargets("postgres", "sqlite"),
    persistence.WithDialectValidator(func(ctx context.Context, result persistence.DialectValidationResult) error {
        for dialect, reasons := range result.MissingDialects {
            log.Printf("dialect %s is incomplete: %v", dialect, reasons)
        }
        return nil // swallow error in development
    }),
)
```

### Rollback Operations

#### Rollback Last Migration Group

```go
err := client.Rollback(context.Background())
if err != nil {
    log.Fatal("Failed to rollback:", err)
}
```

#### Rollback All Migrations

```go
err := client.RollbackAll(context.Background())
if err != nil {
    log.Fatal("Failed to rollback all:", err)
}
```

### Migration Status

Get information about the last executed migration group:

```go
report := client.Report()
if report != nil && !report.IsZero() {
    fmt.Printf("Last migration group: %s\n", report.String())
    for _, m := range report.Migrations {
        fmt.Printf("  - %s\n", m.Name)
    }
}
```

## Configuration

### Disabling Migrations

You can disable migrations through the config:

```go
type Config struct {
    // ... other fields
    MigrationsEnabled bool
}

func (c *Config) GetMigrationsEnabled() bool {
    return c.MigrationsEnabled
}
```

### Custom Logger

Set a custom logger for migration output:

```go
client.SetLogger(customLogger)
```

## Best Practices

### 1. Always Write Rollback Migrations

For every `.up.sql` file, create a corresponding `.down.sql` file that reverses the changes.

### 2. Keep Migrations Idempotent

Use `IF NOT EXISTS` and `IF EXISTS` clauses:

```sql
-- Good
CREATE TABLE IF NOT EXISTS users (...);
DROP TABLE IF EXISTS users;

-- Avoid
CREATE TABLE users (...);
DROP TABLE users;
```

### 3. Test Migrations

Always test both up and down migrations:

```go
// Run migration
err := client.Migrate(ctx)

// Test rollback
err = client.Rollback(ctx)

// Re-apply
err = client.Migrate(ctx)
```

### 4. Use Transactions

BUN automatically wraps each migration in a transaction when supported by the database.

### 5. Version Control

Keep migration files in version control and never modify existing migrations that have been deployed.

### 6. Sequential Numbering

Use zero-padded numbers for consistent ordering:
- ✅ `00001_`, `00002_`, `00003_`
- ❌ `1_`, `2_`, `10_` (will sort incorrectly)

## Migration Groups

BUN groups migrations that are run together. This allows for atomic rollbacks of related changes.

When you run `Migrate()`, all pending migrations are executed as a single group. When you `Rollback()`, the entire last group is rolled back together.

## Error Handling

The migration system provides detailed error messages with context:

```go
if err := client.Migrate(ctx); err != nil {
    // Error will include details about which migration failed
    log.Printf("Migration failed: %v", err)
}
```

Common errors:
- **"no new migrations"**: Not an error, just indicates all migrations are already applied
- **SQL syntax errors**: Check your migration SQL files
- **Connection errors**: Verify database connectivity
- **Permission errors**: Ensure database user has necessary privileges

## Database Support

Migrations work with any database supported by BUN:
- PostgreSQL
- MySQL
- SQLite
- MSSQL

The dialect is specified when creating the client:

```go
// PostgreSQL
client, _ := persistence.New(config, db, pgdialect.New())

// MySQL
client, _ := persistence.New(config, db, mysqldialect.New())

// SQLite
client, _ := persistence.New(config, db, sqlitedialect.New())
```

## Troubleshooting

### Migrations Not Found

Ensure your embed directive is correct:
```go
//go:embed migrations/*.sql  // Note: no space between // and go:embed
var migrationsFS embed.FS
```

### Migration Order Issues

Check file naming - migrations are sorted lexicographically:
```bash
ls -1 migrations/ | sort  # This is the execution order
```

### Rollback Fails

- Ensure `.down.sql` files exist
- Check that down migrations properly reverse up migrations
- Verify no data dependencies prevent rollback

### Migration Table

BUN creates a `bun_migrations` table to track applied migrations. You can query it directly:

```sql
SELECT * FROM bun_migrations ORDER BY id;
```

## Advanced Usage

### Custom Migration Options

Pass options to migration operations:

```go
import "github.com/uptrace/bun/migrate"

opts := []migrate.MigrationOption{
    migrate.WithTableName("custom_migrations_table"),
}

err := client.Rollback(ctx, opts...)
```

### Programmatic Migration Discovery

For advanced use cases, you can work directly with the Migrations struct:

```go
migrations := client.GetMigrations()
// Custom migration logic
```

## Thread Safety

The Migrations struct uses a mutex to ensure thread safe registration of migration filesystems. However, migration execution should be coordinated at the application level to avoid concurrent migration attempts.
