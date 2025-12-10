package persistence

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"sync"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/extra/bunotel"
	"github.com/uptrace/bun/migrate"
	"github.com/uptrace/bun/schema"
)

var (
	bunDB               *bun.DB
	bunMtx              sync.Mutex
	modelsToRegister    []any
	m2mModelsToRegister []any
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
	GetPingTimeout() time.Duration
	GetOtelIdentifier() string
	// GetDatabase() string
}

// Client is the persistence client
type Client struct {
	config            Config
	context           context.Context
	cancel            context.CancelFunc
	db                *bun.DB
	sqlDB             *sql.DB
	migrations        *Migrations
	fixtures          *Fixtures
	migrationsEnabled bool
	seedsEnabled      bool
	lgr               Logger
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

	// TODO: Should we panic if we do this after New?
	modelsToRegister = append(modelsToRegister, model...)
}

func RegisterMany2ManyModel(model ...any) {
	bunMtx.Lock()
	defer bunMtx.Unlock()
	// TODO: Should we panic if we do this after New?
	m2mModelsToRegister = append(m2mModelsToRegister, model...)
}

// New creates a new client
// Optionally if Config has defined these methods they will configure the
// related functionality:
// - GetSeedsEnabled
// - GetMigrationsEnabled
func New(cfg Config, sqlDB *sql.DB, dialect schema.Dialect) (*Client, error) {
	//var err error
	client := Client{
		config:            cfg,
		migrations:        NewMigrations(),
		lgr:               &defaultLogger{},
		seedsEnabled:      true,
		migrationsEnabled: true,
		sqlDB:             sqlDB,
	}

	// our config can optionally configure migrations enablement
	if cmgr, ok := cfg.(interface{ GetMigrationsEnabled() bool }); ok {
		client.migrationsEnabled = cmgr.GetMigrationsEnabled()
	}

	// our config can optionally configure seed enablement
	if smgr, ok := cfg.(interface{ GetSeedsEnabled() bool }); ok {
		client.seedsEnabled = smgr.GetSeedsEnabled()
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

	// NOTE: m2m models should be registered first!
	bunDB.RegisterModel(m2mModelsToRegister...)

	bunDB.RegisterModel(modelsToRegister...)

	modelsToRegister = nil

	client.db = bunDB

	client.fixtures = NewSeedManager(bunDB)

	return &client, client.Check()
}

func (c *Client) SetLogger(logger Logger) {
	c.lgr = logger
	if c.migrations != nil {
		c.migrations.SetLogger(logger)
	}
}

// Seed will run seeds
func (c Client) Seed(ctx context.Context) error {
	if !c.seedsEnabled {
		c.lgr.Warn("persistence seed is disabled")
		return nil
	}
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
	if !c.migrationsEnabled {
		c.lgr.Warn("[WARN] persistence migrations are disabled")
		return nil
	}

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

// RegisterDialectMigrations adds dialect-aware SQL migrations.
func (c Client) RegisterDialectMigrations(root fs.FS, opts ...DialectMigrationOption) *Migrations {
	return c.migrations.RegisterDialectMigrations(root, opts...)
}

// ValidateDialects runs validation callbacks for registered dialect migrations.
func (c Client) ValidateDialects(ctx context.Context) error {
	return c.migrations.ValidateDialects(ctx, c.db)
}

// Rollback previously executed migrations.
// It will rollback a group at a time.
// See https://bun.uptrace.dev/guide/migrations.html#migration-groups-and-rollbacks.
func (c Client) Rollback(ctx context.Context, opts ...migrate.MigrationOption) error {
	return c.migrations.Rollback(ctx, c.db, opts...)
}

// RollbackAll rollbacks every registered migration group.
func (c Client) RollbackAll(ctx context.Context, opts ...migrate.MigrationOption) error {
	return c.migrations.RollbackAll(ctx, c.db, opts...)
}

// Report returns the status of migrations.
// It returns nil if Execute has not been called
// or has failed.
func (c Client) Report() *migrate.MigrationGroup {
	return c.migrations.Report()
}

// DB returns a database
func (c Client) DB() *bun.DB {
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
		c.lgr.Fatal("persistence client connect", err)
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
	c.lgr.Info("Initializing database", "timeout", c.config.GetPingTimeout())

	ctx, cancel := context.WithTimeout(ctx, c.config.GetPingTimeout())
	c.cancel = cancel

	return c.db.PingContext(ctx)
}

// Stop will stop the service
func (c *Client) Stop(ctx context.Context) error {
	c.lgr.Debug("Stopping database...")
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
