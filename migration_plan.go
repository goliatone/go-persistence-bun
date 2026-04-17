package persistence

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

var sqlMigrationNameRE = regexp.MustCompile(`^(\d{1,14})_([0-9a-z_\-]+)\.`)

// MigrationSourceKind identifies the registration model that produced a plan entry.
type MigrationSourceKind string

const (
	MigrationSourceKindSQL     MigrationSourceKind = "sql"
	MigrationSourceKindDialect MigrationSourceKind = "dialect"
	MigrationSourceKindOrdered MigrationSourceKind = "ordered"
)

// MigrationPlan describes the resolved migration order for a source selection.
type MigrationPlan struct {
	SelectedSources []string             `json:"selected_sources,omitempty"`
	Entries         []MigrationPlanEntry `json:"entries"`
}

// MigrationPlanEntry captures one resolved migration in execution order.
type MigrationPlanEntry struct {
	SyntheticName   string              `json:"synthetic_name"`
	SourceName      string              `json:"source_name"`
	SourceKind      MigrationSourceKind `json:"source_kind"`
	SourceLabel     string              `json:"source_label,omitempty"`
	OriginalVersion string              `json:"original_version"`
	OriginalComment string              `json:"original_comment"`
	UpPath          string              `json:"up_path,omitempty"`
	DownPath        string              `json:"down_path,omitempty"`
	ExecutionOrder  int                 `json:"execution_order"`
	Dialect         string              `json:"dialect,omitempty"`
	Applied         bool                `json:"applied"`
	AppliedGroupID  int64               `json:"applied_group_id,omitempty"`
	AppliedAt       time.Time           `json:"applied_at,omitempty"`
}

type sourcePlanSpec struct {
	sourceName  string
	sourceLabel string
	sourceKind  MigrationSourceKind
	sqlFS       fs.FS
	dialect     dialectRegistration
	ordered     orderedSourceRegistration
}

type resolvedMigrationPlan struct {
	plan        *MigrationPlan
	migrations  *migrate.Migrations
	entryByName map[string]MigrationPlanEntry
}

func (m *Migrations) Plan(ctx context.Context, db *bun.DB) (*MigrationPlan, error) {
	resolved, err := m.resolvePlan(ctx, db, nil)
	if err != nil {
		return nil, err
	}
	m.cacheResolvedPlan(resolved)
	return cloneMigrationPlan(resolved.plan), nil
}

func (m *Migrations) PlanSources(
	ctx context.Context,
	db *bun.DB,
	sourceNames ...string,
) (*MigrationPlan, error) {
	if len(sourceNames) == 0 {
		return nil, fmt.Errorf("at least one source name is required")
	}

	resolved, err := m.resolvePlan(ctx, db, sourceNames)
	if err != nil {
		return nil, err
	}
	m.cacheResolvedPlan(resolved)
	return cloneMigrationPlan(resolved.plan), nil
}

func (m *Migrations) LastPlan() *MigrationPlan {
	m.mx.Lock()
	defer m.mx.Unlock()
	return cloneMigrationPlan(m.lastPlan)
}

func (m *Migrations) resolvePlan(
	ctx context.Context,
	db *bun.DB,
	sourceNames []string,
) (*resolvedMigrationPlan, error) {
	specs := m.sourcePlanSpecs()
	selectedSpecs, selectedNames, err := selectSourcePlanSpecs(specs, sourceNames)
	if err != nil {
		return nil, err
	}

	out := &resolvedMigrationPlan{
		plan: &MigrationPlan{
			SelectedSources: selectedNames,
			Entries:         make([]MigrationPlanEntry, 0),
		},
		migrations:  migrate.NewMigrations(),
		entryByName: make(map[string]MigrationPlanEntry),
	}

	for _, spec := range selectedSpecs {
		compiled, err := compileSourcePlan(ctx, db, spec)
		if err != nil {
			return nil, err
		}
		for _, migration := range compiled.migrations.Sorted() {
			entry := compiled.entryByName[migration.Name]
			if previous, exists := out.entryByName[migration.Name]; exists {
				return nil, fmt.Errorf(
					"ambiguous migration composition for %q: source %q conflicts with source %q; use RegisterOrderedMigrationSources for overlapping version trees or plan a narrower source subset",
					migration.Name,
					previous.SourceName,
					entry.SourceName,
				)
			}
			out.migrations.Add(migration)
			out.entryByName[migration.Name] = entry
		}
	}

	sorted := out.migrations.Sorted()
	if db != nil && len(sorted) > 0 {
		migrator := migrate.NewMigrator(db, out.migrations)
		withStatus, err := migrator.MigrationsWithStatus(ctx)
		if err != nil {
			if !isMissingMigrationsTableError(err) {
				return nil, err
			}
		} else {
			sorted = withStatus
		}
	}

	for idx, migration := range sorted {
		entry, ok := out.entryByName[migration.Name]
		if !ok {
			continue
		}
		entry.ExecutionOrder = idx + 1
		if migration.IsApplied() {
			entry.Applied = true
			entry.AppliedGroupID = migration.GroupID
			entry.AppliedAt = migration.MigratedAt
		}
		out.entryByName[migration.Name] = entry
		out.plan.Entries = append(out.plan.Entries, entry)
	}

	return out, nil
}

func (m *Migrations) sourcePlanSpecs() []sourcePlanSpec {
	m.mx.Lock()
	files := append([]fs.FS(nil), m.Files...)
	sqlSourceNames := append([]string(nil), m.sqlSourceNames...)
	dialectRegistrations := append([]dialectRegistration(nil), m.dialectRegistrations...)
	orderedRegistrations := append([]orderedSourceRegistration(nil), m.orderedRegistrations...)
	m.mx.Unlock()

	specs := make([]sourcePlanSpec, 0, len(files)+len(dialectRegistrations)+len(orderedRegistrations))
	for idx, migrationFS := range files {
		sourceName := defaultSQLSourceName(idx)
		if idx < len(sqlSourceNames) && strings.TrimSpace(sqlSourceNames[idx]) != "" {
			sourceName = strings.TrimSpace(sqlSourceNames[idx])
		}
		specs = append(specs, sourcePlanSpec{
			sourceName:  sourceName,
			sourceLabel: sourceName,
			sourceKind:  MigrationSourceKindSQL,
			sqlFS:       migrationFS,
		})
	}
	for idx, registration := range dialectRegistrations {
		sourceName := strings.TrimSpace(registration.sourceName)
		if sourceName == "" {
			sourceName = defaultDialectSourceName(idx)
		}
		specs = append(specs, sourcePlanSpec{
			sourceName:  sourceName,
			sourceLabel: strings.TrimSpace(registration.opts.sourceLabel),
			sourceKind:  MigrationSourceKindDialect,
			dialect:     registration,
		})
	}
	for _, registration := range orderedRegistrations {
		specs = append(specs, sourcePlanSpec{
			sourceName:  registration.name,
			sourceLabel: registration.name,
			sourceKind:  MigrationSourceKindOrdered,
			ordered:     registration,
		})
	}
	return specs
}

func selectSourcePlanSpecs(
	specs []sourcePlanSpec,
	sourceNames []string,
) ([]sourcePlanSpec, []string, error) {
	if len(sourceNames) == 0 {
		allNames := make([]string, 0, len(specs))
		for _, spec := range specs {
			allNames = append(allNames, spec.sourceName)
		}
		return specs, allNames, nil
	}

	available := make(map[string]sourcePlanSpec, len(specs))
	for _, spec := range specs {
		available[spec.sourceName] = spec
	}

	seen := make(map[string]struct{}, len(sourceNames))
	selectedSpecs := make([]sourcePlanSpec, 0, len(sourceNames))
	selectedNames := make([]string, 0, len(sourceNames))
	for _, rawName := range sourceNames {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return nil, nil, fmt.Errorf("source names must not be empty")
		}
		if _, exists := seen[name]; exists {
			continue
		}
		spec, ok := available[name]
		if !ok {
			return nil, nil, fmt.Errorf("unknown migration source %q (available: %s)", name, strings.Join(availableSourceNames(specs), ", "))
		}
		seen[name] = struct{}{}
		selectedSpecs = append(selectedSpecs, spec)
		selectedNames = append(selectedNames, name)
	}

	return selectedSpecs, selectedNames, nil
}

func availableSourceNames(specs []sourcePlanSpec) []string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, spec.sourceName)
	}
	return names
}

func compileSourcePlan(
	ctx context.Context,
	db *bun.DB,
	spec sourcePlanSpec,
) (*resolvedMigrationPlan, error) {
	switch spec.sourceKind {
	case MigrationSourceKindSQL:
		return compileLayeredSourcePlan(
			spec.sourceName,
			spec.sourceLabel,
			spec.sourceKind,
			"",
			[]fs.FS{spec.sqlFS},
		)
	case MigrationSourceKindDialect:
		buildResult, err := spec.dialect.buildFileSystems(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare dialect migration source %q: %w", spec.sourceName, err)
		}
		return compileLayeredSourcePlan(
			spec.sourceName,
			spec.sourceLabel,
			spec.sourceKind,
			buildResult.dialect,
			buildResult.fileSystems,
		)
	case MigrationSourceKindOrdered:
		buildResult, err := spec.ordered.registration.buildFileSystems(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare ordered migration source %q: %w", spec.sourceName, err)
		}
		migrations, metadata, err := compileOrderedSourceMigrations(
			spec.ordered.name,
			spec.ordered.sequence,
			buildResult.fileSystems,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to compile ordered migration source %q: %w", spec.sourceName, err)
		}
		out := &resolvedMigrationPlan{
			migrations:  migrate.NewMigrations(),
			entryByName: make(map[string]MigrationPlanEntry, len(metadata)),
		}
		for _, migration := range migrations {
			out.migrations.Add(migration)
			meta := metadata[migration.Name]
			out.entryByName[migration.Name] = MigrationPlanEntry{
				SyntheticName:   meta.SyntheticName,
				SourceName:      spec.sourceName,
				SourceKind:      spec.sourceKind,
				SourceLabel:     spec.sourceLabel,
				OriginalVersion: meta.OriginalVersion,
				OriginalComment: meta.OriginalComment,
				UpPath:          meta.UpPath,
				DownPath:        meta.DownPath,
				Dialect:         buildResult.dialect,
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported migration source kind %q", spec.sourceKind)
	}
}

func compileLayeredSourcePlan(
	sourceName string,
	sourceLabel string,
	sourceKind MigrationSourceKind,
	dialect string,
	layerFS []fs.FS,
) (*resolvedMigrationPlan, error) {
	migrations, entries, err := compileLayeredSourceMigrations(sourceName, layerFS)
	if err != nil {
		return nil, fmt.Errorf("failed to compile migration source %q: %w", sourceName, err)
	}

	out := &resolvedMigrationPlan{
		migrations:  migrate.NewMigrations(),
		entryByName: make(map[string]MigrationPlanEntry, len(entries)),
	}
	for _, migration := range migrations {
		out.migrations.Add(migration)
		entry := entries[migration.Name]
		entry.SourceName = sourceName
		entry.SourceKind = sourceKind
		entry.SourceLabel = sourceLabel
		entry.Dialect = dialect
		out.entryByName[migration.Name] = entry
	}
	return out, nil
}

func compileLayeredSourceMigrations(
	sourceName string,
	layerFS []fs.FS,
) ([]migrate.Migration, map[string]MigrationPlanEntry, error) {
	entries := make(map[string]*orderedSourceEntry)

	for layerIdx, currentFS := range layerFS {
		discovered, err := discoverLayerMigrations(currentFS)
		if err != nil {
			return nil, nil, err
		}

		layerSeen := make(map[orderedLayerIdentity]string)
		layerComments := make(map[string]string)

		err = fs.WalkDir(currentFS, ".", func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == "." || d.IsDir() {
				return nil
			}

			version, comment, direction, ok, err := parseSQLMigrationFile(path)
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
				return fmt.Errorf(
					"duplicate migration identity in source %q: version %q direction %q in %q and %q",
					sourceName,
					version,
					direction.String(),
					prev,
					path,
				)
			}
			layerSeen[identity] = path

			if prevComment, exists := layerComments[version]; exists && prevComment != comment {
				return fmt.Errorf(
					"duplicate migration identity in source %q: version %q has conflicting comments %q and %q",
					sourceName,
					version,
					prevComment,
					comment,
				)
			}
			layerComments[version] = comment

			entry := entries[version]
			if entry == nil {
				entry = &orderedSourceEntry{
					version: version,
					comment: comment,
					migration: migrate.Migration{
						Name:    version,
						Comment: comment,
					},
				}
				entries[version] = entry
			}

			layerMigration, exists := discovered[version]
			if !exists {
				return fmt.Errorf("missing discovered migration in source %q for version %q and path %q", sourceName, version, path)
			}

			switch direction {
			case orderedDirectionUp:
				if layerMigration.Up == nil {
					return fmt.Errorf("missing up migration function in source %q for version %q and path %q", sourceName, version, path)
				}
				entry.migration.Up = layerMigration.Up
				entry.upPath = path
			case orderedDirectionDown:
				if layerMigration.Down == nil {
					return fmt.Errorf("missing down migration function in source %q for version %q and path %q", sourceName, version, path)
				}
				entry.migration.Down = layerMigration.Down
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
		return nil, map[string]MigrationPlanEntry{}, nil
	}

	versions := make([]string, 0, len(entries))
	for version := range entries {
		versions = append(versions, version)
	}
	sort.Strings(versions)

	migrations := make([]migrate.Migration, 0, len(versions))
	metadata := make(map[string]MigrationPlanEntry, len(versions))
	for _, version := range versions {
		entry := entries[version]
		entry.migration.Name = version
		entry.migration.Comment = entry.comment
		migrations = append(migrations, entry.migration)
		metadata[version] = MigrationPlanEntry{
			SyntheticName:   version,
			OriginalVersion: version,
			OriginalComment: entry.comment,
			UpPath:          entry.upPath,
			DownPath:        entry.downPath,
		}
	}

	return migrations, metadata, nil
}

func cloneMigrationPlan(plan *MigrationPlan) *MigrationPlan {
	if plan == nil {
		return nil
	}
	clone := &MigrationPlan{
		SelectedSources: append([]string(nil), plan.SelectedSources...),
		Entries:         make([]MigrationPlanEntry, len(plan.Entries)),
	}
	copy(clone.Entries, plan.Entries)
	return clone
}

func isMissingMigrationsTableError(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	if !strings.Contains(value, "bun_migrations") {
		return false
	}
	return strings.Contains(value, "does not exist") ||
		strings.Contains(value, "no such table") ||
		strings.Contains(value, "unknown table")
}

func defaultSQLSourceName(idx int) string {
	return fmt.Sprintf("sql[%d]", idx+1)
}

func defaultDialectSourceName(idx int) string {
	return fmt.Sprintf("dialect[%d]", idx+1)
}

func parseSQLMigrationFile(path string) (string, string, orderedDirection, bool, error) {
	base := strings.ToLower(filepath.Base(path))

	direction := orderedDirectionUnknown
	if strings.HasSuffix(base, ".up.sql") {
		direction = orderedDirectionUp
	} else if strings.HasSuffix(base, ".down.sql") {
		direction = orderedDirectionDown
	} else {
		return "", "", orderedDirectionUnknown, false, nil
	}

	matches := sqlMigrationNameRE.FindStringSubmatch(base)
	if matches == nil {
		return "", "", orderedDirectionUnknown, false, fmt.Errorf("unsupported migration name format: %q", filepath.Base(path))
	}

	return matches[1], matches[2], direction, true, nil
}
