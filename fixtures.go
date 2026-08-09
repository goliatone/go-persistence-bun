package persistence

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"text/template"

	apierrors "github.com/goliatone/go-errors"
	"github.com/goliatone/hashid/pkg/hashid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dbfixture"
	"golang.org/x/crypto/bcrypt"
)

// Fixtures manages YAML and JSON fixture files and their shared Bun fixture state.
type Fixtures struct {
	dirs        []fs.FS
	db          *bun.DB
	truncate    bool
	drop        bool
	funcMap     template.FuncMap
	fixture     *dbfixture.Fixture
	opts        []FixtureOption
	optsApplied bool
	transforms  []FixtureTransform
	FileFilter  func(path, name string) bool
	lgr         Logger
}

// FixtureOption configures the seed manager
type FixtureOption func(s *Fixtures)

// FixtureFile is the content and stable identity passed to a fixture transform.
// Path is the exact slash-separated fs.FS path and Name is path.Base(Path).
// Data is borrowed for the duration of the callback and must not be retained and
// mutated concurrently.
type FixtureFile struct {
	Path string
	Name string
	Data []byte
}

// FixtureTransformResult is the outcome of a fixture content transform.
// Skip explicitly omits the matched file; nil or empty Data without Skip is
// passed to Bun as real fixture content.
type FixtureTransformResult struct {
	Data []byte
	Skip bool
}

// FixtureTransform inspects or transforms fixture bytes after reading and
// before Bun decodes templates and fields. The returned Data becomes owned by
// the loading pipeline and must not be retained and mutated concurrently.
// Transform errors must not contain fixture content because the cause remains
// available through errors.Is and errors.As.
type FixtureTransform func(context.Context, FixtureFile) (FixtureTransformResult, error)

// FixtureFailuresMetadataKey is the rich-error metadata key containing the
// []FixtureFailure records collected by a directory Load.
const FixtureFailuresMetadataKey = "fixture_failures"

// FixtureFailure identifies one safely reportable fixture-processing failure.
// TransformIndex is set only when Stage is "transform".
type FixtureFailure struct {
	File           string `json:"file"`
	Stage          string `json:"stage"`
	TransformIndex *int   `json:"transform_index,omitempty"`
}

// FixtureFailures returns defensive copies of the fixture-processing failures
// represented by err, in discovery order. It supports errors returned by both
// Load and LoadFile.
func FixtureFailures(err error) []FixtureFailure {
	var failures []FixtureFailure
	collectFixtureFailures(err, &failures)
	return cloneFixtureFailures(failures)
}

// WithFS adds a fixture filesystem.
func WithFS(dir fs.FS) FixtureOption {
	return func(s *Fixtures) {
		s.dirs = append(s.dirs, dir)
	}
}

// WithTruncateTables truncates fixture tables before loading.
func WithTruncateTables() FixtureOption {
	return func(s *Fixtures) {
		s.truncate = true
	}
}

// WithTrucateTables truncates fixture tables before loading.
// Deprecated: use WithTruncateTables.
func WithTrucateTables() FixtureOption {
	return WithTruncateTables()
}

// WithDropTables drops and recreates fixture tables before loading.
func WithDropTables() FixtureOption {
	return func(l *Fixtures) {
		l.drop = true
	}
}

// WithTemplateFuncs adds functions used while evaluating fixture templates.
func WithTemplateFuncs(funcMap template.FuncMap) FixtureOption {
	return func(s *Fixtures) {
		maps.Copy(s.funcMap, funcMap)
	}
}

// WithFileFilter replaces the pre-read filter used by directory Load calls.
// Each discovered path is passed to fn; returning false prevents the file from
// being read. LoadFile does not consult this filter.
func WithFileFilter(fn func(path, name string) bool) FixtureOption {
	return func(s *Fixtures) {
		s.FileFilter = fn
	}
}

// WithFixtureTransform appends a fixture content transform. Transforms run
// synchronously in registration order. Configure transforms before the first
// Load or LoadFile call; options added after lazy initialization are not applied.
func WithFixtureTransform(transform FixtureTransform) FixtureOption {
	return func(s *Fixtures) {
		s.transforms = append(s.transforms, transform)
	}
}

// NewSeedManager creates a lazily initialized fixture manager.
func NewSeedManager(db *bun.DB, opts ...FixtureOption) *Fixtures {
	s := &Fixtures{
		db:      db,
		opts:    opts,
		funcMap: defaultFuncs(),
		lgr:     &defaultLogger{},
		FileFilter: func(filePath, _ string) bool {
			switch strings.ToLower(path.Ext(filePath)) {
			case ".yml", ".yaml", ".json":
				return true
			default:
				return false
			}
		},
	}

	return s
}

func (s *Fixtures) init() error {
	if !s.optsApplied {
		for _, o := range s.opts {
			if o == nil {
				continue
			}
			o(s)
		}
		s.optsApplied = true
	}
	for index, transform := range s.transforms {
		if transform == nil {
			return apierrors.New("invalid fixture transform configuration", apierrors.CategoryBadInput).
				WithMetadata(map[string]any{
					"stage":           "configuration",
					"transform_index": index,
				})
		}
	}

	opts := []dbfixture.FixtureOption{}
	if s.drop {
		s.lgr.Debug("dropping tables...")
		opts = append(opts, dbfixture.WithRecreateTables())
	} else if s.truncate {
		s.lgr.Debug("truncating tables...")
		opts = append(opts, dbfixture.WithTruncateTables())
	}

	opts = append(opts, dbfixture.WithTemplateFuncs(s.funcMap))

	// Recreate will drop existing table
	s.fixture = dbfixture.New(s.db, opts...)
	return nil
}

// AddOptions appends options before lazy initialization. Options added after the
// first Load or LoadFile call are not applied.
func (s *Fixtures) AddOptions(opts ...FixtureOption) *Fixtures {
	s.opts = append(s.opts, opts...)
	return s
}

// Load will load all fixtures from all configured directories.
// It returns a rich error if any part of the process fails.
func (s *Fixtures) Load(ctx context.Context) error {
	if s.fixture == nil {
		if err := s.init(); err != nil {
			return err
		}
	}

	var allErrors []error
	for _, dir := range s.dirs {
		if err := s.load(ctx, dir); err != nil {
			allErrors = append(allErrors, err)
		}
	}

	if len(allErrors) > 0 {
		joinedErr := apierrors.Join(allErrors...)
		loadErr := apierrors.Wrap(joinedErr, apierrors.CategoryOperation, "one or more errors occurred during fixture loading")
		if failures := FixtureFailures(joinedErr); len(failures) > 0 {
			loadErr = loadErr.WithMetadata(map[string]any{FixtureFailuresMetadataKey: failures})
		}
		return loadErr
	}

	return nil
}

// load walks a single directory and loads all valid fixture files within it.
// This is the internal method where the logical bug was fixed.
func (s *Fixtures) load(ctx context.Context, dir fs.FS) error {
	return fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return apierrors.Wrap(err, apierrors.CategoryInternal, "error walking directory").WithMetadata(map[string]any{"path": path})
		}

		if d.IsDir() {
			return nil
		}

		if !s.FileFilter(path, d.Name()) {
			s.lgr.Debug("skipping file due to filter", "path", path)
			return nil
		}

		skipped, loadErr := s.loadFixtureFile(ctx, dir, path)
		if loadErr != nil {
			return loadErr
		}
		if skipped {
			s.lgr.Debug("skipping fixture file due to transform", "file", path)
		} else {
			s.lgr.Debug("loading fixture file", "file", path)
		}

		return nil
	})
}

// LoadFile will search for and load a single file across all configured directories.
func (s *Fixtures) LoadFile(ctx context.Context, file string) error {
	if s.fixture == nil {
		if err := s.init(); err != nil {
			return err
		}
	}

	if len(s.dirs) == 0 {
		return apierrors.Wrap(fs.ErrNotExist, apierrors.CategoryBadInput, "no filesystems configured to search for file").
			WithMetadata(map[string]any{"file": file})
	}

	var lastErr error
	for _, dir := range s.dirs {
		skipped, err := s.loadFixtureFile(ctx, dir, file)
		if err == nil {
			if skipped {
				s.lgr.Debug("skipping fixture file due to transform", "file", file)
			} else {
				s.lgr.Debug("loading fixture file", "file", file)
			}
			return nil
		}

		if !apierrors.Is(err, os.ErrNotExist) {
			return err
		}

		lastErr = err
	}

	return apierrors.Wrap(lastErr, apierrors.CategoryNotFound, "fixture file not found in any configured directory").
		WithMetadata(map[string]any{
			"file": file,
		})
}

func defaultFuncs() template.FuncMap {
	return template.FuncMap{
		"hashid": func(identifier reflect.Value) (string, error) {
			str := toString(identifier)
			out, err := hashid.New(str)
			if err != nil {
				return "", fmt.Errorf("failed to generate hashid for value '%s': %w", str, err)
			}
			return out, nil
		},
		"hashpwd": func(identifier reflect.Value) (string, error) {
			str := toString(identifier)
			out, err := bcrypt.GenerateFromPassword([]byte(str), bcrypt.DefaultCost)
			if err != nil {
				return "", fmt.Errorf("failed to generate password hash for value '%s': %w", str, err)
			}
			return string(out), nil
		},
	}
}

func toString(v reflect.Value) string {
	switch v.Kind() {
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64)
	}
	return fmt.Sprintf("%v", v.Interface())
}
