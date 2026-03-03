package persistence

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	apierrors "github.com/goliatone/go-errors"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

var orderedMigrationNameRE = regexp.MustCompile(`^(\d{1,14})_([0-9a-z_\-]+)\.`)

// OrderedMigrationSource defines one named migration source in an explicit order.
type OrderedMigrationSource struct {
	Name    string
	Root    fs.FS
	Options []DialectMigrationOption
}

// OrderedMigrationMetadata keeps the mapping from synthetic migration names
// back to source and original files for debug/reporting.
type OrderedMigrationMetadata struct {
	SyntheticName   string
	SourceName      string
	OriginalVersion string
	OriginalComment string
	UpPath          string
	DownPath        string
}

type orderedSourceRegistration struct {
	name         string
	registration dialectRegistration
}

type orderedDirection uint8

const (
	orderedDirectionUnknown orderedDirection = iota
	orderedDirectionUp
	orderedDirectionDown
)

type orderedSourceEntry struct {
	migration       migrate.Migration
	version         string
	comment         string
	upPath          string
	downPath        string
	commentLayer    int
	commentLayerSet bool
}

type orderedLayerIdentity struct {
	version   string
	direction orderedDirection
}

func buildOrderedMigrations(
	ctx context.Context,
	db *bun.DB,
	registrations []orderedSourceRegistration,
) ([]migrate.Migration, map[string]OrderedMigrationMetadata, error) {
	if len(registrations) == 0 {
		return nil, map[string]OrderedMigrationMetadata{}, nil
	}

	out := make([]migrate.Migration, 0)
	metadata := make(map[string]OrderedMigrationMetadata)

	for sourceIdx, source := range registrations {
		buildResult, err := source.registration.buildFileSystems(ctx, db)
		if err != nil {
			return nil, nil, apierrors.Wrap(err,
				apierrors.CategoryInternal,
				"failed to prepare ordered source dialect migrations",
			).WithMetadata(map[string]any{"source_index": sourceIdx, "source_name": source.name})
		}

		sourceMigrations, sourceMeta, err := compileOrderedSourceMigrations(source.name, sourceIdx, buildResult.fileSystems)
		if err != nil {
			return nil, nil, apierrors.Wrap(err,
				apierrors.CategoryInternal,
				"failed to compile ordered source migrations",
			).WithMetadata(map[string]any{"source_index": sourceIdx, "source_name": source.name})
		}

		out = append(out, sourceMigrations...)
		for syntheticName, meta := range sourceMeta {
			metadata[syntheticName] = meta
		}
	}

	return out, metadata, nil
}

func compileOrderedSourceMigrations(
	sourceName string,
	sourceIdx int,
	layerFS []fs.FS,
) ([]migrate.Migration, map[string]OrderedMigrationMetadata, error) {
	entries := make(map[string]*orderedSourceEntry)

	for layerIdx, currentFS := range layerFS {
		layerSeen := make(map[orderedLayerIdentity]string)
		layerComments := make(map[string]string)

		err := fs.WalkDir(currentFS, ".", func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == "." || d.IsDir() {
				return nil
			}

			version, comment, direction, ok, err := parseOrderedMigrationFile(path)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}

			identity := orderedLayerIdentity{
				version:   version,
				direction: direction,
			}
			if prev, exists := layerSeen[identity]; exists {
				return fmt.Errorf("duplicate migration identity in source %q: version %q direction %q in %q and %q",
					sourceName, version, direction.String(), prev, path)
			}
			layerSeen[identity] = path

			if prevComment, exists := layerComments[version]; exists && prevComment != comment {
				return fmt.Errorf("duplicate migration identity in source %q: version %q has conflicting comments %q and %q",
					sourceName, version, prevComment, comment)
			}
			layerComments[version] = comment

			entry := entries[version]
			if entry == nil {
				entry = &orderedSourceEntry{
					version: version,
					comment: comment,
					migration: migrate.Migration{
						Comment: comment,
					},
				}
				entries[version] = entry
			}

			migrationFunc := migrate.NewSQLMigrationFunc(currentFS, path)
			switch direction {
			case orderedDirectionUp:
				entry.migration.Up = migrationFunc
				entry.upPath = path
			case orderedDirectionDown:
				entry.migration.Down = migrationFunc
				entry.downPath = path
			}

			if !entry.commentLayerSet || layerIdx >= entry.commentLayer {
				entry.comment = comment
				entry.commentLayer = layerIdx
				entry.commentLayerSet = true
				entry.migration.Comment = comment
			}

			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}

	if len(entries) == 0 {
		return nil, map[string]OrderedMigrationMetadata{}, nil
	}

	versions := make([]string, 0, len(entries))
	for version := range entries {
		versions = append(versions, version)
	}
	sort.Strings(versions)

	migrations := make([]migrate.Migration, 0, len(versions))
	metadata := make(map[string]OrderedMigrationMetadata, len(versions))
	for migrationIdx, version := range versions {
		entry := entries[version]
		syntheticName := orderedSyntheticMigrationName(sourceIdx, migrationIdx)
		entry.migration.Name = syntheticName
		entry.migration.Comment = fmt.Sprintf("%s_%s", sourceName, entry.comment)

		migrations = append(migrations, entry.migration)
		metadata[syntheticName] = OrderedMigrationMetadata{
			SyntheticName:   syntheticName,
			SourceName:      sourceName,
			OriginalVersion: version,
			OriginalComment: entry.comment,
			UpPath:          entry.upPath,
			DownPath:        entry.downPath,
		}
	}

	return migrations, metadata, nil
}

func parseOrderedMigrationFile(path string) (string, string, orderedDirection, bool, error) {
	base := strings.ToLower(filepath.Base(path))

	direction := orderedDirectionUnknown
	if strings.HasSuffix(base, ".up.sql") {
		direction = orderedDirectionUp
	} else if strings.HasSuffix(base, ".down.sql") {
		direction = orderedDirectionDown
	} else {
		return "", "", orderedDirectionUnknown, false, nil
	}

	matches := orderedMigrationNameRE.FindStringSubmatch(base)
	if matches == nil {
		return "", "", orderedDirectionUnknown, false, fmt.Errorf("unsupported migration name format: %q", filepath.Base(path))
	}

	return matches[1], matches[2], direction, true, nil
}

func orderedSyntheticMigrationName(sourceIdx, migrationIdx int) string {
	return fmt.Sprintf("ord_%06d_%06d", sourceIdx+1, migrationIdx+1)
}

func (d orderedDirection) String() string {
	switch d {
	case orderedDirectionUp:
		return "up"
	case orderedDirectionDown:
		return "down"
	default:
		return "unknown"
	}
}
