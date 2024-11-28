package persistence

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dbfixture"
)

// Fixtures manages fixtures and seeds
type Fixtures struct {
	dirs       []fs.FS
	db         *bun.DB
	truncate   bool
	drop       bool
	funcMap    TplMap
	fixture    *dbfixture.Fixture
	opts       []FixtureOption
	FileFilter func(path, name string) bool
}

// FixtureOption configures the seed manager
type FixtureOption func(s *Fixtures)

// WithFS will truncate tables
func WithFS(dir fs.FS) FixtureOption {
	return func(s *Fixtures) {
		s.dirs = append(s.dirs, dir)
	}
}

// WithTrucateTables will truncate tables
func WithTrucateTables() FixtureOption {
	return func(s *Fixtures) {
		s.truncate = true
	}
}

// WithDropTables will drop tables
func WithDropTables() FixtureOption {
	return func(l *Fixtures) {
		l.drop = true
	}
}

// TplMap hosts function maps
type TplMap map[string]any

// WithTemplateFuncs are used to solve functions in seed file
func WithTemplateFuncs(funcMap TplMap) FixtureOption {
	return func(s *Fixtures) {
		for k, v := range funcMap {
			s.funcMap[k] = v
		}
	}
}

// WithFileFilter will add a file filter function.
// Each file found in the given dir will be passed throu
// this function, and if it returns false the file will
// be filtered out.
func WithFileFilter(fn func(path, name string) bool) FixtureOption {
	return func(s *Fixtures) {
		s.FileFilter = fn
	}
}

// NewSeeManager generates a new seed manger
func NewSeeManager(db *bun.DB, opts ...FixtureOption) *Fixtures {
	s := &Fixtures{
		db:      db,
		opts:    opts,
		funcMap: make(TplMap),
		FileFilter: func(path, name string) bool {
			return strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")
		},
	}

	return s
}

func (s *Fixtures) init() {
	for _, o := range s.opts {
		o(s)
	}

	opts := []dbfixture.FixtureOption{}
	if s.drop {
		opts = append(opts, dbfixture.WithRecreateTables())
	} else if s.truncate {
		opts = append(opts, dbfixture.WithTruncateTables())
	}

	// Recreate will drop existing table
	s.fixture = dbfixture.New(s.db, opts...)
}

// AddOptions will configure options
func (s *Fixtures) AddOptions(opts ...FixtureOption) *Fixtures {
	s.opts = append(s.opts, opts...)
	return s
}

// Load will load a file
func (s *Fixtures) Load(ctx context.Context) error {

	if s.fixture == nil {
		s.init()
	}

	var err error
	for _, dir := range s.dirs {
		err = errors.Join(s.load(ctx, dir))
	}

	return err
}

func (s *Fixtures) load(ctx context.Context, dir fs.FS) error {
	return fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if !s.FileFilter(path, d.Name()) {
			return nil
		}

		if err := s.LoadFile(ctx, path); err != nil {
			return err
		}

		return nil
	})
}

// LoadFile will load a file
func (s *Fixtures) LoadFile(ctx context.Context, file string) error {
	var err error
	for _, dir := range s.dirs {
		err = s.fixture.Load(ctx, dir, file)
		if err == nil {
			return nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return os.ErrNotExist
}
