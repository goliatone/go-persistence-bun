package persistence

import (
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"github.com/uptrace/bun/migrate"
)

// OrderedMigrationIdentityMode controls how ordered migration source names are
// converted into Bun migration names.
type OrderedMigrationIdentityMode int

const (
	// OrderedMigrationIdentityPositional preserves the legacy registration-position
	// identity format: ord_000001_000001.
	OrderedMigrationIdentityPositional OrderedMigrationIdentityMode = iota
	// OrderedMigrationIdentitySourceStable uses source keys and explicit source
	// order values for durable migration identities.
	OrderedMigrationIdentitySourceStable
)

func (mode OrderedMigrationIdentityMode) String() string {
	switch mode {
	case OrderedMigrationIdentityPositional:
		return "positional"
	case OrderedMigrationIdentitySourceStable:
		return "source-stable"
	default:
		return fmt.Sprintf("unknown(%d)", int(mode))
	}
}

// OrderedMigrationSource defines one named migration source in an explicit order.
type OrderedMigrationSource struct {
	Name         string
	Root         fs.FS
	Options      []DialectMigrationOption
	IdentityMode OrderedMigrationIdentityMode
	SourceKey    string
	Order        int
	DependsOn    []string
}

// OrderedMigrationSourceOption configures an ordered migration source.
type OrderedMigrationSourceOption func(*OrderedMigrationSource)

// NewStableOrderedMigrationSource builds a source-stable ordered migration source.
func NewStableOrderedMigrationSource(
	name string,
	root fs.FS,
	sourceKey string,
	order int,
	opts ...OrderedMigrationSourceOption,
) OrderedMigrationSource {
	source := OrderedMigrationSource{
		Name:         name,
		Root:         root,
		IdentityMode: OrderedMigrationIdentitySourceStable,
		SourceKey:    sourceKey,
		Order:        order,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&source)
		}
	}
	return source
}

// WithOrderedMigrationDependencies declares dependencies by source key.
func WithOrderedMigrationDependencies(sourceKeys ...string) OrderedMigrationSourceOption {
	return func(source *OrderedMigrationSource) {
		if source == nil {
			return
		}
		source.DependsOn = append(source.DependsOn, sourceKeys...)
	}
}

// WithOrderedMigrationDialectOptions attaches dialect options to an ordered source.
func WithOrderedMigrationDialectOptions(opts ...DialectMigrationOption) OrderedMigrationSourceOption {
	return func(source *OrderedMigrationSource) {
		if source == nil {
			return
		}
		source.Options = append(source.Options, opts...)
	}
}

// OrderedMigrationMetadata keeps the mapping from synthetic migration names
// back to source and original files for debug/reporting.
type OrderedMigrationMetadata struct {
	SyntheticName    string
	SourceName       string
	SourceKey        string
	SourceOrder      int
	SourceDependsOn  []string
	ResolvedPosition int
	IdentityMode     OrderedMigrationIdentityMode
	OriginalVersion  string
	OriginalComment  string
	UpPath           string
	DownPath         string
}

type orderedSourceRegistration struct {
	name             string
	sequence         int
	registration     dialectRegistration
	identityMode     OrderedMigrationIdentityMode
	sourceKey        string
	sourceOrder      int
	dependsOn        []string
	resolvedPosition int
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

//nolint:funlen,gocyclo // Ordered migration compilation intentionally keeps duplicate detection and layer overrides together.
func compileOrderedSourceMigrations(
	source orderedSourceRegistration,
	sourceLayers []migrationSourceLayer,
) ([]migrate.Migration, map[string]OrderedMigrationMetadata, error) {
	entries := make(map[string]*orderedSourceEntry)

	for layerIdx, layer := range sourceLayers {
		discovered, err := discoverLayerMigrations(layer.fsys)
		if err != nil {
			return nil, nil, err
		}

		layerSeen := make(map[orderedLayerIdentity]string)
		layerComments := make(map[string]string)

		err = fs.WalkDir(layer.fsys, ".", func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == "." || d.IsDir() {
				return nil
			}

			version, comment, direction, ok, parseErr := parseOrderedMigrationFile(path)
			if parseErr != nil {
				return parseErr
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
					source.name, version, direction.String(), prev, path)
			}
			layerSeen[identity] = path

			if prevComment, exists := layerComments[version]; exists && prevComment != comment {
				return fmt.Errorf("duplicate migration identity in source %q: version %q has conflicting comments %q and %q",
					source.name, version, prevComment, comment)
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

			layerMigration, exists := discovered[version]
			if !exists {
				return fmt.Errorf("missing discovered migration in source %q for version %q and path %q", source.name, version, path)
			}

			switch direction {
			case orderedDirectionUp:
				if layerMigration.Up == nil {
					return fmt.Errorf("missing up migration function in source %q for version %q and path %q", source.name, version, path)
				}
				migrationFunc := layerMigration.Up
				entry.migration.Up = migrationFunc
				entry.upPath = qualifyLayerPath(layer.pathPrefix, path)
			case orderedDirectionDown:
				if layerMigration.Down == nil {
					return fmt.Errorf("missing down migration function in source %q for version %q and path %q", source.name, version, path)
				}
				migrationFunc := layerMigration.Down
				entry.migration.Down = migrationFunc
				entry.downPath = qualifyLayerPath(layer.pathPrefix, path)
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
		syntheticName := orderedSyntheticMigrationName(source, migrationIdx, version)
		entry.migration.Name = syntheticName
		entry.migration.Comment = fmt.Sprintf("%s_%s", source.name, entry.comment)

		migrations = append(migrations, entry.migration)
		metadata[syntheticName] = OrderedMigrationMetadata{
			SyntheticName:    syntheticName,
			SourceName:       source.name,
			SourceKey:        source.sourceKey,
			SourceOrder:      source.sourceOrder,
			SourceDependsOn:  append([]string(nil), source.dependsOn...),
			ResolvedPosition: source.resolvedPosition,
			IdentityMode:     source.identityMode,
			OriginalVersion:  version,
			OriginalComment:  entry.comment,
			UpPath:           entry.upPath,
			DownPath:         entry.downPath,
		}
	}

	return migrations, metadata, nil
}

func discoverLayerMigrations(layer fs.FS) (map[string]migrate.Migration, error) {
	migrations := migrate.NewMigrations()
	if err := migrations.Discover(layer); err != nil {
		return nil, err
	}

	sorted := migrations.Sorted()
	out := make(map[string]migrate.Migration, len(sorted))
	for _, migration := range sorted {
		out[migration.Name] = migration
	}
	return out, nil
}

func parseOrderedMigrationFile(path string) (string, string, orderedDirection, bool, error) {
	return parseSQLMigrationFile(path)
}

func orderedSyntheticMigrationName(source orderedSourceRegistration, migrationIdx int, originalVersion string) string {
	if source.identityMode == OrderedMigrationIdentitySourceStable {
		return orderedStableSyntheticMigrationName(source.sourceOrder, source.sourceKey, originalVersion)
	}
	return orderedPositionalSyntheticMigrationName(source.sequence, migrationIdx)
}

func orderedPositionalSyntheticMigrationName(sourceIdx, migrationIdx int) string {
	return fmt.Sprintf("ord_%06d_%06d", sourceIdx+1, migrationIdx+1)
}

func orderedStableSyntheticMigrationName(sourceOrder int, sourceKey, originalVersion string) string {
	return fmt.Sprintf("ordsrc_%06d_%s_%s", sourceOrder, sourceKey, originalVersion)
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

var sourceKeyInvalidRE = regexp.MustCompile(`[^a-z0-9_]+`)

func normalizeOrderedSourceKey(value string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(value))
	key = strings.ReplaceAll(key, "-", "_")
	key = strings.Join(strings.Fields(key), "_")
	key = sourceKeyInvalidRE.ReplaceAllString(key, "_")
	key = strings.Trim(key, "_")
	for strings.Contains(key, "__") {
		key = strings.ReplaceAll(key, "__", "_")
	}
	if key == "" {
		return "", errors.New("ordered migration source key is empty after normalization")
	}
	return key, nil
}
