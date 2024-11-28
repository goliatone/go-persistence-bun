package persistence

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"log"
	"sync"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/extra/bunotel"
	"github.com/uptrace/bun/migrate"
	"github.com/uptrace/bun/schema"
)

var (
	bunDB            *bun.DB
	bunMtx           sync.Mutex
	modelsToRegister []any
)

// DefaultDriver is the Postgres driver
const DefaultDriver = "postgres"

// Priority is the module's loading priority
var Priority int

// Name is the string identifier of the module
const Name = "persistence"

// Config has values for configurable properties
type Config interface {
	GetDebug() bool
	GetDriver() string
	GetServer() string
	GetDatabase() string
	GetPingTimeout() time.Duration
	// }

	// type OpenTelemetryConfig interface {
	GetOtelIdentifier() string
}

// Client is the persistence client
type Client struct {
	config     Config
	context    context.Context
	cancel     context.CancelFunc
	db         *bun.DB
	sqlDB      *sql.DB
	migrations *Migrations
	fixtures   *Fixtures
}

// RegisterModel registers a model in Bun or,
// if the global instance is not yet initialized,
// will enqueue the models, which will be registered
// once the global instance is initialized.
// RegisterModel registers models by name so they
// can be referenced in table relations and fixtures.
// persistence.RegisterModel((*models.User)(nil))
// persistence.RegisterModel(&model.User{})
func RegisterModel(model ...any) {
	bunMtx.Lock()
	defer bunMtx.Unlock()

	if bunDB != nil {
		bunDB.RegisterModel(model...)
	} else {
		modelsToRegister = append(modelsToRegister, model...)
	}
}

// New creates a new client
func New(cfg Config, sqlDB *sql.DB, dialect schema.Dialect) (*Client, error) {
	//var err error
	client := Client{
		config:     cfg,
		migrations: &Migrations{},
	}

	// Create a Bun db on top of it.
	bunDB = bun.NewDB(sqlDB, dialect)

	if cfg.GetDebug() {
		// Print every query we run
		bunDB.AddQueryHook(bundebug.NewQueryHook(
			bundebug.WithVerbose(true),
		))
	} else {
		// Print only the failed queries
		bunDB.AddQueryHook(bundebug.NewQueryHook())
	}

	if cfg.GetOtelIdentifier() != "" {
		bunDB.AddQueryHook(
			bunotel.NewQueryHook(
				bunotel.WithDBName(cfg.GetOtelIdentifier()),
			),
		)
	}

	bunDB.RegisterModel(modelsToRegister...)

	modelsToRegister = nil

	client.db = bunDB

	client.fixtures = NewSeeManager(bunDB)

	return &client, client.Check()
}

// Seed will run seeds
func (c Client) Seed(ctx context.Context) error {
	return c.fixtures.Load(ctx)
}

// GetFixtures will return fixtures
func (c Client) GetFixtures() *Fixtures {
	return c.fixtures
}

// GetMigrations will migrate db
func (c Client) GetMigrations() *Migrations {
	return c.migrations
}

// Migrate will migrate db
func (c Client) Migrate(ctx context.Context) error {
	return c.migrations.Migrate(ctx, c.db)
}

// RegisterFixtures adds file based fixtures
func (c Client) RegisterFixtures(migrations ...fs.FS) *Fixtures {
	for _, f := range migrations {
		c.fixtures.AddOptions(WithFS(f))
	}
	return c.GetFixtures()
}

// RegisterSQLMigrations adds SQL based migrations
func (c Client) RegisterSQLMigrations(migrations ...fs.FS) *Migrations {
	return c.migrations.RegisterSQLMigrations(migrations...)
}

// RegisterFuncMigrations adds SQL based migrations
func (c Client) RegisterFuncMigrations(migrations ...MigratorFunc) *Migrations {
	return c.migrations.RegisterFuncMigrations(migrations...)
}

// Rollback previously executed migrations.
// It will rollback a group at a time.
// See https://bun.uptrace.dev/guide/migrations.html#migration-groups-and-rollbacks.
func (c Client) Rollback(ctx context.Context, opts ...migrate.MigrationOption) error {
	return c.migrations.Rollback(ctx, c.db, opts...)
}

// RollbackAll rollbacks every registered migration group.
func (c Client) RollbackAll(ctx context.Context, db *bun.DB, opts ...migrate.MigrationOption) error {
	return c.migrations.RollbackAll(ctx, c.db, opts...)
}

// Report returns the status of migrations.
// It returns nil if Execute has not been called
// or has failed.
func (c Client) Report() *migrate.MigrationGroup {
	return c.migrations.Report()
}

// DB returns a database
func (c Client) DB() bun.IDB {
	return c.db
}

// Check will check connection
func (c Client) Check() error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, c.config.GetPingTimeout())
	defer cancel()
	return c.db.PingContext(ctx)
}

// MustConnect will panic if no connection
func (c Client) MustConnect() {
	if err := c.Check(); err != nil {
		log.Fatalf("persistence client connect: %s", err)
	}
	// defer c.db.Close()
}

// Close will close the client
func (c Client) Close() error {
	// TODO: wrap errors
	c.db.Close()
	return c.sqlDB.Close()
}

// Start will start the service
func (c *Client) Start(ctx context.Context) error {
	log.Printf("Initializing database withtimeout %d...\n", c.config.GetPingTimeout())

	ctx, cancel := context.WithTimeout(ctx, c.config.GetPingTimeout())
	c.cancel = cancel

	return c.db.PingContext(ctx)
}

// Stop will stop the service
func (c *Client) Stop(ctx context.Context) error {
	log.Println("Stopping database...")
	if c.cancel != nil {
		c.cancel()
	}

	var err error

	select {
	case <-ctx.Done():
		err = errors.New("max time exeeded")
	default:
		err = c.db.Close()
	}

	return err
}

// Name will return the module name
func (c *Client) Name() string {
	return Name
}

// Priority will return the module priority
func (c *Client) Priority() int {
	return Priority
}
