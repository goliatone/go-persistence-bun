package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	apierrors "github.com/goliatone/go-errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

type fixtureBoundaryRecord struct {
	bun.BaseModel `bun:"table:fixture_boundary_records"`

	ID   int64  `bun:"id,pk,autoincrement"`
	Name string `bun:"name,notnull"`
}

type fixtureStateParent struct {
	bun.BaseModel `bun:"table:fixture_state_parents"`

	ID   int64  `bun:"id,pk,autoincrement"`
	Name string `bun:"name,notnull"`
}

type fixtureStateChild struct {
	bun.BaseModel `bun:"table:fixture_state_children"`

	ID       int64  `bun:"id,pk,autoincrement"`
	ParentID int64  `bun:"parent_id,notnull"`
	Label    string `bun:"label,notnull"`
}

type fixtureMixedRecord struct {
	bun.BaseModel `bun:"table:fixture_mixed_records"`

	ID        int64          `bun:"id,pk"`
	Name      string         `bun:"name,notnull"`
	Quantity  int            `bun:"quantity,notnull"`
	Note      sql.NullString `bun:"note"`
	CreatedAt time.Time      `bun:"created_at,notnull"`
}

type fixtureSourceCustomer struct {
	bun.BaseModel `bun:"table:fixture_source_customers"`

	ID         int64  `bun:"id,pk,autoincrement"`
	ExternalID string `bun:"external_id,notnull"`
	Name       string `bun:"name,notnull"`
	Active     bool   `bun:"active,notnull"`
}

func TestFixturesSharedTransformLoading(t *testing.T) {
	t.Run("Load skips one file and transforms a later file", func(t *testing.T) {
		ctx := context.Background()
		db, cleanup := newFixtureBoundaryDB(t, ctx)
		defer cleanup()

		var paths []string
		fixtures := NewSeedManager(db,
			WithFS(fstest.MapFS{
				"01-skip.json": {Data: []byte(`{"name":"skip"}`)},
				"02-load.json": {Data: []byte(`{"name":"loaded"}`)},
			}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				paths = append(paths, file.Path+":"+file.Name)
				if file.Path == "01-skip.json" {
					return FixtureTransformResult{Skip: true}, nil
				}
				return FixtureTransformResult{Data: canonicalBoundaryFixture("from-load")}, nil
			}),
		)

		require.NoError(t, fixtures.Load(ctx))
		assert.Equal(t, []string{
			"01-skip.json:01-skip.json",
			"02-load.json:02-load.json",
		}, paths)
		assert.Equal(t, []string{"from-load"}, fixtureBoundaryNames(t, db, ctx))
	})

	t.Run("LoadFile searches only through not-found filesystems", func(t *testing.T) {
		ctx := context.Background()
		db, cleanup := newFixtureBoundaryDB(t, ctx)
		defer cleanup()

		var received FixtureFile
		fixtures := NewSeedManager(db,
			WithFS(fstest.MapFS{"other.json": {Data: []byte(`[]`)}}),
			WithFS(fstest.MapFS{"nested/source.json": {Data: []byte(`{"data":[]}`)}}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				received = file
				return FixtureTransformResult{Data: canonicalBoundaryFixture("from-load-file")}, nil
			}),
		)

		require.NoError(t, fixtures.LoadFile(ctx, "nested/source.json"))
		assert.Equal(t, "nested/source.json", received.Path)
		assert.Equal(t, "source.json", received.Name)
		assert.Equal(t, []string{"from-load-file"}, fixtureBoundaryNames(t, db, ctx))
	})

	t.Run("LoadFile skip is terminal for the first matching filesystem", func(t *testing.T) {
		ctx := context.Background()
		db, cleanup := newFixtureBoundaryDB(t, ctx)
		defer cleanup()

		transformCalls := 0
		fixtures := NewSeedManager(db,
			WithFS(fstest.MapFS{"same.json": {Data: []byte(`{"source":"first"}`)}}),
			WithFS(fstest.MapFS{"same.json": {Data: canonicalBoundaryFixture("must-not-load")}}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				transformCalls++
				return FixtureTransformResult{Skip: true}, nil
			}),
		)

		require.NoError(t, fixtures.LoadFile(ctx, "same.json"))
		assert.Equal(t, 1, transformCalls)
		assert.Empty(t, fixtureBoundaryNames(t, db, ctx))
	})
}

func TestFixturesTransformCancellation(t *testing.T) {
	t.Run("before read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		source := &observedFixtureFS{MapFS: fstest.MapFS{
			"record.json": {Data: canonicalBoundaryFixture("never")},
		}}
		fixtures := NewSeedManager(nil,
			WithFS(source),
			WithFixtureTransform(fixtureTransformAdapter{}.Transform),
		)

		err := fixtures.LoadFile(ctx, "record.json")

		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 0, source.openCalls)
		assertFixtureErrorStage(t, err, "record.json", "read", nil)
	})

	t.Run("between callbacks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		secondCalled := false
		fixtures := NewSeedManager(nil,
			WithFS(fstest.MapFS{"record.json": {Data: []byte(`[]`)}}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				cancel()
				return FixtureTransformResult{Data: file.Data}, nil
			}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				secondCalled = true
				return FixtureTransformResult{Data: file.Data}, nil
			}),
		)

		err := fixtures.LoadFile(ctx, "record.json")

		require.ErrorIs(t, err, context.Canceled)
		assert.False(t, secondCalled)
		index := 1
		assertFixtureErrorStage(t, err, "record.json", "transform", &index)
	})

	t.Run("after final callback before consume", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		db, cleanup := newFixtureBoundaryDB(t, context.Background())
		defer cleanup()
		fixtures := NewSeedManager(db,
			WithFS(fstest.MapFS{"record.json": {Data: []byte(`[]`)}}),
			WithFixtureTransform(func(_ context.Context, _ FixtureFile) (FixtureTransformResult, error) {
				cancel()
				return FixtureTransformResult{Data: canonicalBoundaryFixture("never")}, nil
			}),
		)

		err := fixtures.LoadFile(ctx, "record.json")

		require.ErrorIs(t, err, context.Canceled)
		assertFixtureErrorStage(t, err, "record.json", "consume", nil)
		assert.Empty(t, fixtureBoundaryNames(t, db, context.Background()))
	})
}

func TestFixturesTransformEmptyOutputReachesBun(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "nil", data: nil},
		{name: "empty", data: []byte{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, cleanup := newFixtureBoundaryDB(t, context.Background())
			defer cleanup()
			fixtures := NewSeedManager(db,
				WithFS(fstest.MapFS{"record.json": {Data: []byte(`[]`)}}),
				WithFixtureTransform(func(context.Context, FixtureFile) (FixtureTransformResult, error) {
					return FixtureTransformResult{Data: test.data}, nil
				}),
			)

			err := fixtures.LoadFile(context.Background(), "record.json")

			require.Error(t, err)
			assertFixtureErrorStage(t, err, "record.json", "consume", nil)
		})
	}
}

func TestFixturesNoTransformDelegatesOriginalFilesystem(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newFixtureBoundaryDB(t, ctx)
	defer cleanup()
	source := &observedFixtureFS{MapFS: fstest.MapFS{
		"record.yaml": {Data: canonicalBoundaryFixture("direct")},
	}}
	fixtures := NewSeedManager(db, WithFS(source))

	require.NoError(t, fixtures.LoadFile(ctx, "record.yaml"))
	assert.Equal(t, 1, source.openCalls)
	assert.Equal(t, []string{"direct"}, fixtureBoundaryNames(t, db, ctx))
}

func TestFixturesDefaultFileFilter(t *testing.T) {
	fixtures := NewSeedManager(nil)
	for _, test := range []struct {
		path string
		want bool
	}{
		{path: "fixtures/users.yml", want: true},
		{path: "fixtures/users.YML", want: true},
		{path: "fixtures/users.yaml", want: true},
		{path: "fixtures/users.YaMl", want: true},
		{path: "fixtures/users.json", want: true},
		{path: "fixtures/users.JSON", want: true},
		{path: "fixtures/users.yaml.bak", want: false},
		{path: "fixtures/usersjson", want: false},
		{path: "fixtures/users.txt", want: false},
		{path: "fixtures/users", want: false},
	} {
		t.Run(test.path, func(t *testing.T) {
			assert.Equal(t, test.want, fixtures.FileFilter(test.path, "ignored"))
		})
	}
}

func TestFixturesCustomFilterDirectoryAndLoadFileBehavior(t *testing.T) {
	ctx := context.Background()
	source := fstest.MapFS{
		"record.json": {Data: canonicalBoundaryFixture("selected-directly")},
	}

	t.Run("custom filter is authoritative for directory loading", func(t *testing.T) {
		db, cleanup := newFixtureBoundaryDB(t, ctx)
		defer cleanup()
		transformCalled := false
		fixtures := NewSeedManager(db,
			WithFS(source),
			WithFileFilter(func(filePath, name string) bool {
				assert.Equal(t, "record.json", filePath)
				assert.Equal(t, "record.json", name)
				return false
			}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				transformCalled = true
				return FixtureTransformResult{Data: file.Data}, nil
			}),
		)

		require.NoError(t, fixtures.Load(ctx))
		assert.False(t, transformCalled)
		assert.Empty(t, fixtureBoundaryNames(t, db, ctx))
	})

	t.Run("LoadFile does not consult the directory filter", func(t *testing.T) {
		db, cleanup := newFixtureBoundaryDB(t, ctx)
		defer cleanup()
		filterCalled := false
		fixtures := NewSeedManager(db,
			WithFS(source),
			WithFileFilter(func(string, string) bool {
				filterCalled = true
				return false
			}),
		)

		require.NoError(t, fixtures.LoadFile(ctx, "record.json"))
		assert.False(t, filterCalled)
		assert.Equal(t, []string{"selected-directly"}, fixtureBoundaryNames(t, db, ctx))
	})
}

func TestFixturesPreserveBunStateAcrossTransformedFiles(t *testing.T) {
	for _, mode := range []struct {
		name   string
		option FixtureOption
	}{
		{name: "truncate", option: WithTruncateTables()},
		{name: "recreate", option: WithDropTables()},
	} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			db, cleanup := newFixtureBoundaryDB(t, ctx)
			defer cleanup()
			_, err := db.NewInsert().Model(&fixtureBoundaryRecord{Name: "remove-me"}).Exec(ctx)
			require.NoError(t, err)

			fixtures := NewSeedManager(db,
				WithFS(fstest.MapFS{
					"01.json": {Data: []byte(`{"source":1}`)},
					"02.json": {Data: []byte(`{"source":2}`)},
				}),
				mode.option,
				WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
					if file.Path == "01.json" {
						return FixtureTransformResult{Data: canonicalBoundaryFixture("first")}, nil
					}
					return FixtureTransformResult{Data: canonicalBoundaryFixture("second")}, nil
				}),
			)

			require.NoError(t, fixtures.Load(ctx))
			assert.Equal(t, []string{"first", "second"}, fixtureBoundaryNames(t, db, ctx))
		})
	}
}

func TestFixturesPreserveBunStateAcrossLoadCalls(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newFixtureBoundaryDB(t, ctx)
	defer cleanup()
	fixtures := NewSeedManager(db,
		WithFS(fstest.MapFS{
			"first.json":  {Data: []byte(`{"source":1}`)},
			"second.json": {Data: []byte(`{"source":2}`)},
		}),
		WithTruncateTables(),
		WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
			if file.Path == "first.json" {
				return FixtureTransformResult{Data: canonicalBoundaryFixture("first-call")}, nil
			}
			return FixtureTransformResult{Data: canonicalBoundaryFixture("second-call")}, nil
		}),
	)

	require.NoError(t, fixtures.LoadFile(ctx, "first.json"))
	require.NoError(t, fixtures.LoadFile(ctx, "second.json"))
	assert.Equal(t, []string{"first-call", "second-call"}, fixtureBoundaryNames(t, db, ctx))
}

func TestFixturesPreserveRowsAndTemplateFunctionsAcrossTransformedFiles(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newFixtureStateDB(t, ctx)
	defer cleanup()
	var templateInput string
	fixtures := NewSeedManager(db,
		WithFS(fstest.MapFS{
			"01-parent.json": {Data: []byte(`{"source":"parent"}`)},
			"02-child.json":  {Data: []byte(`{"source":"child"}`)},
		}),
		WithTemplateFuncs(map[string]any{
			"decorate": func(value string) string {
				templateInput = value
				return "decorated:" + value
			},
		}),
		WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
			switch file.Path {
			case "01-parent.json":
				return FixtureTransformResult{Data: []byte(`[
  {"model":"FixtureStateParent","rows":[{"_id":"parent","name":"parent"}]}
]`)}, nil
			default:
				return FixtureTransformResult{Data: []byte(`[
  {"model":"FixtureStateChild","rows":[
    {"parent_id":"{{ $.FixtureStateParent.parent.ID }}","label":"{{ decorate \"source-value\" }}"}
  ]}
]`)}, nil
			}
		}),
	)

	require.NoError(t, fixtures.Load(ctx))
	var parent fixtureStateParent
	require.NoError(t, db.NewSelect().Model(&parent).Scan(ctx))
	var child fixtureStateChild
	require.NoError(t, db.NewSelect().Model(&child).Scan(ctx))
	assert.Equal(t, parent.ID, child.ParentID)
	assert.Equal(t, "source-value", templateInput)
	assert.Equal(t, "decorated:source-value", child.Label)
}

func TestFixturesFailureClassification(t *testing.T) {
	t.Run("read failure", func(t *testing.T) {
		cause := errors.New("read denied")
		fixtures := NewSeedManager(nil,
			WithFS(&fixtureReadErrorFS{
				MapFS: fstest.MapFS{"record.json": {Data: []byte(`[]`)}},
				err:   cause,
			}),
			WithFixtureTransform(fixtureTransformAdapter{}.Transform),
		)

		err := fixtures.LoadFile(context.Background(), "record.json")

		require.ErrorIs(t, err, cause)
		assertFixtureErrorStage(t, err, "record.json", "read", nil)
	})

	t.Run("transform failure is not treated as not found", func(t *testing.T) {
		cause := errors.New("transform failed")
		fixtures := NewSeedManager(nil,
			WithFS(fstest.MapFS{"record.json": {Data: []byte(`[]`)}}),
			WithFS(fstest.MapFS{"record.json": {Data: canonicalBoundaryFixture("must-not-load")}}),
			WithFixtureTransform(fixtureTransformAdapter{}.Transform),
			WithFixtureTransform(func(context.Context, FixtureFile) (FixtureTransformResult, error) {
				return FixtureTransformResult{}, cause
			}),
		)

		err := fixtures.LoadFile(context.Background(), "record.json")

		require.ErrorIs(t, err, cause)
		assert.False(t, apierrors.IsCategory(err, apierrors.CategoryNotFound))
		index := 1
		assertFixtureErrorStage(t, err, "record.json", "transform", &index)
	})

	t.Run("consume failure", func(t *testing.T) {
		db, cleanup := newFixtureBoundaryDB(t, context.Background())
		defer cleanup()
		fixtures := NewSeedManager(db,
			WithFS(fstest.MapFS{"record.json": {Data: []byte(`{"not":"a canonical fixture"}`)}}),
			WithFixtureTransform(fixtureTransformAdapter{}.Transform),
		)

		err := fixtures.LoadFile(context.Background(), "record.json")

		require.Error(t, err)
		assertFixtureErrorStage(t, err, "record.json", "consume", nil)
	})

	t.Run("final lookup failure", func(t *testing.T) {
		fixtures := NewSeedManager(nil,
			WithFS(fstest.MapFS{"other.json": {Data: []byte(`[]`)}}),
			WithFS(fstest.MapFS{}),
		)

		err := fixtures.LoadFile(context.Background(), "missing.json")

		require.ErrorIs(t, err, fs.ErrNotExist)
		var richErr *apierrors.Error
		require.ErrorAs(t, err, &richErr)
		assert.Equal(t, apierrors.CategoryNotFound, richErr.Category)
		assert.Equal(t, "missing.json", richErr.Metadata["file"])
	})
}

func TestFixturesLoadAggregatesFilesystemErrors(t *testing.T) {
	firstCause := errors.New("first fixture source failed")
	secondCause := errors.New("second fixture source failed")
	fixtures := NewSeedManager(nil,
		WithFS(fstest.MapFS{"first.json": {Data: []byte("first")}}),
		WithFS(fstest.MapFS{"second.json": {Data: []byte("second")}}),
		WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
			if string(file.Data) == "first" {
				return FixtureTransformResult{}, firstCause
			}
			return FixtureTransformResult{}, secondCause
		}),
	)

	err := fixtures.Load(context.Background())

	require.ErrorIs(t, err, firstCause)
	require.ErrorIs(t, err, secondCause)
	assert.True(t, apierrors.IsCategory(err, apierrors.CategoryOperation))
}

func TestFixturesControlledErrorsAndLogsDoNotLeakContent(t *testing.T) {
	const sentinel = "SENTINEL-FIXTURE-CONTENT"
	callbackCause := errors.New("adapter rejected input")
	logger := &fixtureCaptureLogger{}
	fixtures := NewSeedManager(nil,
		WithFS(fstest.MapFS{"private/source.json": {Data: []byte(sentinel)}}),
		WithFixtureTransform(func(context.Context, FixtureFile) (FixtureTransformResult, error) {
			return FixtureTransformResult{}, callbackCause
		}),
	)
	fixtures.lgr = logger

	err := fixtures.LoadFile(context.Background(), "private/source.json")

	require.ErrorIs(t, err, callbackCause)
	publicJSON, marshalErr := json.Marshal(err)
	require.NoError(t, marshalErr)
	renderer, rendererErr := apierrors.NewRenderer(
		apierrors.OutputDiagnostic,
		apierrors.WithMetadataAllowlist("file", "stage", "transform_index"),
	)
	require.NoError(t, rendererErr)
	diagnostic, renderErr := renderer.FormatDiagnostic(err)
	require.NoError(t, renderErr)
	controlledOutput := err.Error() + string(publicJSON) + diagnostic + strings.Join(logger.entries, " ")
	assert.NotContains(t, controlledOutput, sentinel)
	assert.Contains(t, diagnostic, "private/source.json")
	assert.Contains(t, diagnostic, "transform")
	assert.Contains(t, diagnostic, "transform_index")
}

func TestFixturesCanonicalJSONAndMixedFormats(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()
	db.RegisterModel((*fixtureMixedRecord)(nil))
	_, err := db.NewCreateTable().Model((*fixtureMixedRecord)(nil)).Exec(ctx)
	require.NoError(t, err)

	source := fstest.MapFS{
		"01-canonical.json": {Data: []byte(`[
  {"model":"FixtureMixedRecord","rows":[
    {"id":1,"name":"json","quantity":3,"note":"from-json","created_at":"2026-08-08T10:00:00Z"}
  ]}
]`)},
		"02-canonical.yaml": {Data: []byte(`
- model: FixtureMixedRecord
  rows:
    - id: 2
      name: yaml
      quantity: 5
      note: from-yaml
      created_at: 2026-08-08T11:00:00Z
`)},
		"03-canonical.yml": {Data: []byte(`
- model: FixtureMixedRecord
  rows:
    - id: 3
      name: yml
      quantity: 8
      note: null
      created_at: 2026-08-08T12:00:00Z
`)},
	}
	fixtures := NewSeedManager(db, WithFS(source))

	require.NoError(t, fixtures.Load(ctx))
	var records []fixtureMixedRecord
	require.NoError(t, db.NewSelect().Model(&records).Order("id ASC").Scan(ctx))
	require.Len(t, records, 3)
	assert.Equal(t, []string{"json", "yaml", "yml"}, []string{records[0].Name, records[1].Name, records[2].Name})
	assert.Equal(t, []int{3, 5, 8}, []int{records[0].Quantity, records[1].Quantity, records[2].Quantity})
	assert.Equal(t, "from-json", records[0].Note.String)
	assert.True(t, records[0].Note.Valid)
	assert.Equal(t, "from-yaml", records[1].Note.String)
	assert.True(t, records[1].Note.Valid)
	assert.False(t, records[2].Note.Valid)
	assert.True(t, time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC).Equal(records[0].CreatedAt))
	assert.True(t, time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC).Equal(records[2].CreatedAt))
}

func TestFixturesCallerOwnedSourceEnvelopeAdapter(t *testing.T) {
	type sourceCustomer struct {
		ExternalID  string `json:"external_id"`
		DisplayName string `json:"display_name"`
		Active      bool   `json:"active"`
	}
	type sourceEnvelope struct {
		Data []sourceCustomer `json:"data"`
	}
	type fixtureCustomerRow struct {
		ExternalID string `json:"external_id"`
		Name       string `json:"name"`
		Active     bool   `json:"active"`
	}
	type fixtureDocument struct {
		Model string               `json:"model"`
		Rows  []fixtureCustomerRow `json:"rows"`
	}

	ctx := context.Background()
	db, cleanup := newSQLiteTestDB(t)
	defer cleanup()
	db.RegisterModel((*fixtureSourceCustomer)(nil))
	_, err := db.NewCreateTable().Model((*fixtureSourceCustomer)(nil)).Exec(ctx)
	require.NoError(t, err)

	var visited []string
	source := fstest.MapFS{
		"api/customers.json": {Data: []byte(`{"data":[
  {"external_id":"cus-1","display_name":"Alice","active":true},
  {"external_id":"cus-2","display_name":"Bob","active":false}
]}`)},
		"api/audit-events.json": {Data: []byte(`{"data":[{"event":"ignored"}]}`)},
	}
	fixtures := NewSeedManager(db,
		WithFS(source),
		WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
			visited = append(visited, file.Path)
			if file.Name == "audit-events.json" {
				return FixtureTransformResult{Skip: true}, nil
			}
			if file.Name != "customers.json" {
				return FixtureTransformResult{Data: file.Data}, nil
			}

			var envelope sourceEnvelope
			if err := json.Unmarshal(file.Data, &envelope); err != nil {
				return FixtureTransformResult{}, fmt.Errorf("decode source customer envelope: %w", err)
			}
			rows := make([]fixtureCustomerRow, 0, len(envelope.Data))
			for _, sourceCustomer := range envelope.Data {
				rows = append(rows, fixtureCustomerRow{
					ExternalID: sourceCustomer.ExternalID,
					Name:       sourceCustomer.DisplayName,
					Active:     sourceCustomer.Active,
				})
			}
			data, err := json.Marshal([]fixtureDocument{{Model: "FixtureSourceCustomer", Rows: rows}})
			if err != nil {
				return FixtureTransformResult{}, fmt.Errorf("encode customer fixture: %w", err)
			}
			return FixtureTransformResult{Data: data}, nil
		}),
	)

	require.NoError(t, fixtures.Load(ctx))
	assert.Equal(t, []string{"api/audit-events.json", "api/customers.json"}, visited)
	var customers []fixtureSourceCustomer
	require.NoError(t, db.NewSelect().Model(&customers).Order("external_id ASC").Scan(ctx))
	require.Len(t, customers, 2)
	assert.Equal(t, "cus-1", customers[0].ExternalID)
	assert.Equal(t, "Alice", customers[0].Name)
	assert.True(t, customers[0].Active)
	assert.Equal(t, "cus-2", customers[1].ExternalID)
	assert.Equal(t, "Bob", customers[1].Name)
	assert.False(t, customers[1].Active)
}

type observedFixtureFS struct {
	fstest.MapFS
	openCalls int
}

type fixtureReadErrorFS struct {
	fstest.MapFS
	err error
}

func (f *fixtureReadErrorFS) ReadFile(name string) ([]byte, error) {
	return nil, &fs.PathError{Op: "read", Path: name, Err: f.err}
}

type fixtureCaptureLogger struct {
	entries []string
}

func (l *fixtureCaptureLogger) append(format string, args ...any) {
	l.entries = append(l.entries, fmt.Sprint(append([]any{format}, args...)...))
}

func (l *fixtureCaptureLogger) Debug(format string, args ...any) { l.append(format, args...) }
func (l *fixtureCaptureLogger) Info(format string, args ...any)  { l.append(format, args...) }
func (l *fixtureCaptureLogger) Warn(format string, args ...any)  { l.append(format, args...) }
func (l *fixtureCaptureLogger) Error(format string, args ...any) { l.append(format, args...) }
func (l *fixtureCaptureLogger) Fatal(format string, args ...any) { l.append(format, args...) }

func (f *observedFixtureFS) Open(name string) (fs.File, error) {
	f.openCalls++
	return f.MapFS.Open(name)
}

func canonicalBoundaryFixture(name string) []byte {
	return []byte(`[{"model":"FixtureBoundaryRecord","rows":[{"name":"` + name + `"}]}]`)
}

func newFixtureBoundaryDB(t *testing.T, ctx context.Context) (*bun.DB, func()) {
	t.Helper()
	db, cleanup := newSQLiteTestDB(t)
	db.RegisterModel((*fixtureBoundaryRecord)(nil))
	_, err := db.NewCreateTable().Model((*fixtureBoundaryRecord)(nil)).IfNotExists().Exec(ctx)
	require.NoError(t, err)
	return db, cleanup
}

func newFixtureStateDB(t *testing.T, ctx context.Context) (*bun.DB, func()) {
	t.Helper()
	db, cleanup := newSQLiteTestDB(t)
	db.RegisterModel((*fixtureStateParent)(nil), (*fixtureStateChild)(nil))
	_, err := db.NewCreateTable().Model((*fixtureStateParent)(nil)).IfNotExists().Exec(ctx)
	require.NoError(t, err)
	_, err = db.NewCreateTable().Model((*fixtureStateChild)(nil)).IfNotExists().Exec(ctx)
	require.NoError(t, err)
	return db, cleanup
}

func fixtureBoundaryNames(t *testing.T, db *bun.DB, ctx context.Context) []string {
	t.Helper()
	var names []string
	err := db.NewSelect().Model((*fixtureBoundaryRecord)(nil)).Column("name").Order("id ASC").Scan(ctx, &names)
	require.NoError(t, err)
	return names
}

func assertFixtureErrorStage(
	t *testing.T,
	err error,
	filePath string,
	stage string,
	transformIndex *int,
) {
	t.Helper()
	var richErr *apierrors.Error
	require.True(t, errors.As(err, &richErr))
	assert.Equal(t, apierrors.CategoryOperation, richErr.Category)
	assert.Equal(t, filePath, richErr.Metadata["file"])
	assert.Equal(t, stage, richErr.Metadata["stage"])
	if transformIndex == nil {
		assert.NotContains(t, richErr.Metadata, "transform_index")
	} else {
		assert.Equal(t, *transformIndex, richErr.Metadata["transform_index"])
	}
}
