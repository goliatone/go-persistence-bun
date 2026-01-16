package persistence

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/extra/bunotel"
)

// ClientOption configures the persistence client.
type ClientOption func(*clientOptions)

// QueryHookKeyer allows hooks to provide a stable identity for deduplication.
type QueryHookKeyer interface {
	QueryHookKey() string
}

// QueryHookErrorHandler handles invalid query hook registrations.
type QueryHookErrorHandler func(db *bun.DB, hook bun.QueryHook, err error)

var (
	ErrQueryHookNil        = errors.New("query hook is nil")
	ErrQueryHookNilPointer = errors.New("query hook is a nil pointer")
)

const (
	defaultQueryHookPriority = 0
	defaultBundebugPriority  = 10
	defaultBunotelPriority   = 20
)

type hookEntry struct {
	hook     bun.QueryHook
	priority int
	order    int
}

type clientOptions struct {
	hookOrder        int
	hooks            []hookEntry
	hookErrorHandler QueryHookErrorHandler

	bundebugEnabled  bool
	bundebugPriority int
	bundebugOrder    int

	bunotelEnabled  bool
	bunotelPriority int
	bunotelOrder    int
}

// WithQueryHooks registers custom query hooks with default priority.
func WithQueryHooks(hooks ...bun.QueryHook) ClientOption {
	return WithQueryHooksPriority(defaultQueryHookPriority, hooks...)
}

// WithQueryHooksPriority registers custom hooks with the given priority.
func WithQueryHooksPriority(priority int, hooks ...bun.QueryHook) ClientOption {
	return func(opts *clientOptions) {
		if opts == nil {
			return
		}
		for _, hook := range hooks {
			opts.hookOrder++
			opts.hooks = append(opts.hooks, hookEntry{
				hook:     hook,
				priority: priority,
				order:    opts.hookOrder,
			})
		}
	}
}

// WithQueryHookErrorHandler sets the hook registration error handler.
func WithQueryHookErrorHandler(handler QueryHookErrorHandler) ClientOption {
	return func(opts *clientOptions) {
		if opts == nil {
			return
		}
		opts.hookErrorHandler = handler
	}
}

// WithBundebug enables bundebug query hook registration.
func WithBundebug() ClientOption {
	return func(opts *clientOptions) {
		if opts == nil {
			return
		}
		opts.hookOrder++
		opts.bundebugEnabled = true
		opts.bundebugPriority = defaultBundebugPriority
		opts.bundebugOrder = opts.hookOrder
	}
}

// WithBunotel enables bunotel query hook registration.
func WithBunotel() ClientOption {
	return func(opts *clientOptions) {
		if opts == nil {
			return
		}
		opts.hookOrder++
		opts.bunotelEnabled = true
		opts.bunotelPriority = defaultBunotelPriority
		opts.bunotelOrder = opts.hookOrder
	}
}

// LogQueryHookErrorHandler logs and skips invalid query hooks.
func LogQueryHookErrorHandler(db *bun.DB, hook bun.QueryHook, err error) {
	log.Printf("persistence: query hook skipped: %v (type=%T)", err, hook)
}

// PanicQueryHookErrorHandler panics on invalid query hooks.
func PanicQueryHookErrorHandler(db *bun.DB, hook bun.QueryHook, err error) {
	panic(fmt.Sprintf("persistence: query hook error: %v (type=%T)", err, hook))
}

type hookRegistryEntry struct {
	mu      sync.Mutex
	keys    map[string]struct{}
	handler QueryHookErrorHandler
}

var hookRegistry sync.Map

func applyQueryHooks(db *bun.DB, cfg Config, opts *clientOptions) {
	if db == nil || opts == nil {
		return
	}

	if opts.hookErrorHandler != nil {
		setQueryHookErrorHandler(db, opts.hookErrorHandler)
	}

	entries := append([]hookEntry{}, opts.hooks...)
	if opts.bundebugEnabled {
		if hook := bundebugHook(cfg); hook != nil {
			entries = append(entries, hookEntry{
				hook:     hook,
				priority: opts.bundebugPriority,
				order:    opts.bundebugOrder,
			})
		}
	}
	if opts.bunotelEnabled {
		if hook := bunotelHook(cfg); hook != nil {
			entries = append(entries, hookEntry{
				hook:     hook,
				priority: opts.bunotelPriority,
				order:    opts.bunotelOrder,
			})
		}
	}

	if len(entries) == 0 {
		return
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].priority == entries[j].priority {
			return entries[i].order < entries[j].order
		}
		return entries[i].priority < entries[j].priority
	})

	hooks := make([]bun.QueryHook, 0, len(entries))
	for _, entry := range entries {
		hooks = append(hooks, entry.hook)
	}
	registerQueryHooks(db, hooks...)
}

func bundebugHook(cfg Config) bun.QueryHook {
	if cfg == nil {
		return nil
	}
	if cfg.GetDebug() {
		return bundebug.NewQueryHook(bundebug.WithVerbose(true))
	}
	return bundebug.NewQueryHook()
}

func bunotelHook(cfg Config) bun.QueryHook {
	if cfg == nil {
		return nil
	}
	identifier := cfg.GetOtelIdentifier()
	if identifier == "" {
		return nil
	}
	return bunotel.NewQueryHook(bunotel.WithDBName(identifier))
}

func registerQueryHooks(db *bun.DB, hooks ...bun.QueryHook) {
	if db == nil || len(hooks) == 0 {
		return
	}

	entry := getHookRegistryEntry(db)
	if entry == nil {
		return
	}

	entry.mu.Lock()
	handler := entry.handler
	entry.mu.Unlock()
	if handler == nil {
		handler = LogQueryHookErrorHandler
	}

	validHooks := make([]bun.QueryHook, 0, len(hooks))
	for _, hook := range hooks {
		if err := validateQueryHook(hook); err != nil {
			handler(db, hook, err)
			continue
		}
		validHooks = append(validHooks, hook)
	}
	if len(validHooks) == 0 {
		return
	}

	localKeys := make(map[string]struct{}, len(validHooks))

	entry.mu.Lock()
	defer entry.mu.Unlock()

	for _, hook := range validHooks {
		if key, ok := queryHookKey(hook); ok {
			if _, seen := localKeys[key]; seen {
				continue
			}
			if _, exists := entry.keys[key]; exists {
				continue
			}
			localKeys[key] = struct{}{}
			entry.keys[key] = struct{}{}
		}
		db.AddQueryHook(hook)
	}
}

func getHookRegistryEntry(db *bun.DB) *hookRegistryEntry {
	if db == nil {
		return nil
	}
	if entry, ok := hookRegistry.Load(db); ok {
		return entry.(*hookRegistryEntry)
	}
	entry := &hookRegistryEntry{
		keys:    make(map[string]struct{}),
		handler: LogQueryHookErrorHandler,
	}
	actual, _ := hookRegistry.LoadOrStore(db, entry)
	return actual.(*hookRegistryEntry)
}

func setQueryHookErrorHandler(db *bun.DB, handler QueryHookErrorHandler) {
	if db == nil {
		return
	}
	entry := getHookRegistryEntry(db)
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if handler == nil {
		entry.handler = LogQueryHookErrorHandler
		return
	}
	entry.handler = handler
}

func validateQueryHook(hook bun.QueryHook) error {
	if hook == nil {
		return ErrQueryHookNil
	}
	value := reflect.ValueOf(hook)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return ErrQueryHookNilPointer
	}
	return nil
}

func queryHookKey(hook bun.QueryHook) (string, bool) {
	if hook == nil {
		return "", false
	}
	if keyer, ok := hook.(QueryHookKeyer); ok {
		key := strings.TrimSpace(keyer.QueryHookKey())
		if key != "" {
			return fmt.Sprintf("%T:%s", hook, key), true
		}
	}
	value := reflect.ValueOf(hook)
	if value.Kind() == reflect.Ptr && !value.IsNil() {
		return fmt.Sprintf("%T:%x", hook, value.Pointer()), true
	}
	return "", false
}
