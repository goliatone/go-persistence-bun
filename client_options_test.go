package persistence

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/extra/bunotel"
)

type staticConfig struct {
	debug          bool
	otelIdentifier string
	pingTimeout    time.Duration
}

func (c staticConfig) GetDebug() bool {
	return c.debug
}

func (c staticConfig) GetDriver() string {
	return ""
}

func (c staticConfig) GetServer() string {
	return ""
}

func (c staticConfig) GetPingTimeout() time.Duration {
	return c.pingTimeout
}

func (c staticConfig) GetOtelIdentifier() string {
	return c.otelIdentifier
}

type countingHook struct {
	before int32
	after  int32
}

func (h *countingHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	atomic.AddInt32(&h.before, 1)
	return ctx
}

func (h *countingHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	atomic.AddInt32(&h.after, 1)
}

type keyedHook struct {
	key    string
	before int32
	after  int32
}

func (h *keyedHook) QueryHookKey() string {
	return h.key
}

func (h *keyedHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	atomic.AddInt32(&h.before, 1)
	return ctx
}

func (h *keyedHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	atomic.AddInt32(&h.after, 1)
}

type orderHook struct {
	id string
}

func (h *orderHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	return ctx
}

func (h *orderHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {}

func newTestClient(
	t *testing.T,
	cfg Config,
	opts ...ClientOption,
) (*Client, sqlmock.Sqlmock, func()) {
	t.Helper()
	resetInit()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)

	mock.ExpectPing()
	client, err := New(cfg, db, pgdialect.New(), opts...)
	require.NoError(t, err)

	cleanup := func() {
		_ = db.Close()
		resetInit()
	}

	return client, mock, cleanup
}

func getQueryHooks(db *bun.DB) []bun.QueryHook {
	if db == nil {
		return nil
	}
	value := reflect.ValueOf(db).Elem().FieldByName("queryHooks")
	if !value.IsValid() {
		return nil
	}
	// Read the unexported queryHooks slice for test assertions.
	raw := reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem()
	hooks, ok := raw.Interface().([]bun.QueryHook)
	if !ok {
		return nil
	}
	return append([]bun.QueryHook(nil), hooks...)
}

func hookOrderNames(hooks []bun.QueryHook) []string {
	names := make([]string, 0, len(hooks))
	for _, hook := range hooks {
		switch h := hook.(type) {
		case *orderHook:
			names = append(names, h.id)
		case *bundebug.QueryHook:
			names = append(names, "bundebug")
		case *bunotel.QueryHook:
			names = append(names, "bunotel")
		default:
			names = append(names, reflect.TypeOf(h).String())
		}
	}
	return names
}

func TestWithQueryHooks_AttachesCustomHook(t *testing.T) {
	cfg := staticConfig{pingTimeout: 5 * time.Second}
	hook := &countingHook{}

	client, mock, cleanup := newTestClient(t, cfg, WithQueryHooks(hook))
	defer cleanup()

	mock.ExpectQuery("SELECT 1").WillReturnRows(
		sqlmock.NewRows([]string{"value"}).AddRow(1),
	)

	var out int
	err := client.DB().NewSelect().ColumnExpr("1 AS value").Scan(context.Background(), &out)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&hook.before))
	assert.Equal(t, int32(1), atomic.LoadInt32(&hook.after))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryHooks_NilHooksInvokeHandler(t *testing.T) {
	cfg := staticConfig{pingTimeout: 5 * time.Second}
	var nilHook bun.QueryHook
	var nilPointerHook *countingHook
	validHook := &countingHook{}

	var errs []error
	handler := func(db *bun.DB, hook bun.QueryHook, err error) {
		errs = append(errs, err)
	}

	client, mock, cleanup := newTestClient(
		t,
		cfg,
		WithQueryHookErrorHandler(handler),
		WithQueryHooks(nilHook, nilPointerHook, validHook),
	)
	defer cleanup()

	assert.Len(t, getQueryHooks(client.DB()), 1)
	if assert.Len(t, errs, 2) {
		assert.ErrorIs(t, errs[0], ErrQueryHookNil)
		assert.ErrorIs(t, errs[1], ErrQueryHookNilPointer)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryHooks_Dedupe(t *testing.T) {
	cfg := staticConfig{pingTimeout: 5 * time.Second}

	t.Run("key", func(t *testing.T) {
		hookA := &keyedHook{key: "dup"}
		hookB := &keyedHook{key: "dup"}

		client, mock, cleanup := newTestClient(t, cfg, WithQueryHooks(hookA, hookB))
		defer cleanup()

		mock.ExpectQuery("SELECT 1").WillReturnRows(
			sqlmock.NewRows([]string{"value"}).AddRow(1),
		)

		var out int
		err := client.DB().NewSelect().ColumnExpr("1 AS value").Scan(context.Background(), &out)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), atomic.LoadInt32(&hookA.before))
		assert.Equal(t, int32(0), atomic.LoadInt32(&hookB.before))
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("pointer", func(t *testing.T) {
		hook := &countingHook{}

		client, mock, cleanup := newTestClient(t, cfg, WithQueryHooks(hook, hook))
		defer cleanup()

		mock.ExpectQuery("SELECT 1").WillReturnRows(
			sqlmock.NewRows([]string{"value"}).AddRow(1),
		)

		var out int
		err := client.DB().NewSelect().ColumnExpr("1 AS value").Scan(context.Background(), &out)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), atomic.LoadInt32(&hook.before))
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestQueryHooks_BuiltinsOptIn(t *testing.T) {
	cfg := staticConfig{
		debug:          true,
		otelIdentifier: "otel-service",
		pingTimeout:    5 * time.Second,
	}

	t.Run("without options", func(t *testing.T) {
		client, mock, cleanup := newTestClient(t, cfg)
		defer cleanup()

		assert.Len(t, getQueryHooks(client.DB()), 0)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with options", func(t *testing.T) {
		client, mock, cleanup := newTestClient(t, cfg, WithBundebug(), WithBunotel())
		defer cleanup()

		hooks := getQueryHooks(client.DB())
		assert.Len(t, hooks, 2)
		assert.Contains(t, hookOrderNames(hooks), "bundebug")
		assert.Contains(t, hookOrderNames(hooks), "bunotel")
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestQueryHooks_OrderAndPriority(t *testing.T) {
	cfg := staticConfig{
		debug:          true,
		otelIdentifier: "otel-service",
		pingTimeout:    5 * time.Second,
	}

	hookA := &orderHook{id: "A"}
	hookB := &orderHook{id: "B"}
	hookC := &orderHook{id: "C"}

	client, mock, cleanup := newTestClient(
		t,
		cfg,
		WithBunotel(),
		WithQueryHooks(hookA),
		WithBundebug(),
		WithQueryHooksPriority(5, hookB),
		WithQueryHooks(hookC),
	)
	defer cleanup()

	hooks := getQueryHooks(client.DB())
	assert.Equal(t, []string{"A", "C", "B", "bundebug", "bunotel"}, hookOrderNames(hooks))
	assert.NoError(t, mock.ExpectationsWereMet())
}
