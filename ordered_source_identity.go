package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

var (
	ErrOrderedSourceDrift                 = errors.New("ordered migration source graph drift")
	ErrOrderedSourceRepair                = errors.New("ordered migration source repair failed")
	ErrOrderedSourceRepairMissingMapping  = errors.New("ordered migration source repair missing legacy mapping")
	ErrOrderedSourceRepairAmbiguousMarker = errors.New("ordered migration source repair ambiguous legacy marker")
	ErrOrderedSourceRepairMarkerMismatch  = errors.New("ordered migration source repair marker/source mismatch")
)

type OrderedSourceDriftError struct {
	SourceKey string
	Field     string
	Expected  string
	Observed  string
}

func (e *OrderedSourceDriftError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf(
		"%v for source %q field %s: expected %s, observed %s; run a compatibility repair/backfill or deliberately update source identity metadata",
		ErrOrderedSourceDrift,
		e.SourceKey,
		e.Field,
		e.Expected,
		e.Observed,
	)
}

func (e *OrderedSourceDriftError) Unwrap() error {
	return ErrOrderedSourceDrift
}

type OrderedSourceRepairError struct {
	Kind        error
	LegacyName  string
	SourceName  string
	SourceKey   string
	Expected    string
	Observed    string
	Remediation string
	Message     string
}

func (e *OrderedSourceRepairError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	parts := []string{ErrOrderedSourceRepair.Error()}
	if e.Kind != nil && e.Kind != ErrOrderedSourceRepair {
		parts = append(parts, e.Kind.Error())
	}
	if e.LegacyName != "" {
		parts = append(parts, fmt.Sprintf("legacy marker %q", e.LegacyName))
	}
	if e.SourceName != "" {
		parts = append(parts, fmt.Sprintf("source %q", e.SourceName))
	}
	if e.SourceKey != "" {
		parts = append(parts, fmt.Sprintf("source key %q", e.SourceKey))
	}
	if e.Expected != "" || e.Observed != "" {
		parts = append(parts, fmt.Sprintf("expected %s, observed %s", e.Expected, e.Observed))
	}
	if e.Remediation != "" {
		parts = append(parts, e.Remediation)
	}
	return strings.Join(parts, ": ")
}

func (e *OrderedSourceRepairError) Unwrap() error {
	if e == nil || e.Kind == nil {
		return ErrOrderedSourceRepair
	}
	return e.Kind
}

func (e *OrderedSourceRepairError) Is(target error) bool {
	return target == ErrOrderedSourceRepair || target == e.Kind
}

type orderedSourceIdentityRow struct {
	bun.BaseModel `bun:"table:bun_ordered_migration_sources"`

	SourceKey        string          `bun:"source_key,pk"`
	SourceName       string          `bun:"source_name,notnull"`
	SourceOrder      int             `bun:"source_order,notnull"`
	Dependencies     JSONStringSlice `bun:"dependencies,type:json"`
	ResolvedPosition int             `bun:"resolved_position,notnull"`
	IdentityMode     string          `bun:"identity_mode,notnull"`
	GraphFingerprint string          `bun:"graph_fingerprint,notnull"`
	CreatedAt        time.Time       `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt        time.Time       `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

type orderedSourceAliasRow struct {
	bun.BaseModel `bun:"table:bun_ordered_migration_aliases"`

	LegacyName string    `bun:"legacy_name,pk"`
	StableName string    `bun:"stable_name,notnull,unique"`
	SourceKey  string    `bun:"source_key,notnull"`
	CreatedAt  time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

type OrderedMigrationRepairOptions struct {
	CleanupLegacyMarkers bool
}

type orderedRepairOperation struct {
	legacyName string
	stableName string
	sourceKey  string
	groupID    int64
}

type appliedMigrationRow struct {
	Name       string    `bun:"name"`
	GroupID    int64     `bun:"group_id"`
	MigratedAt time.Time `bun:"migrated_at"`
}

func (m *Migrations) verifyOrderedSourceGraph(ctx context.Context, db *bun.DB, graph []orderedSourceRegistration) error {
	if db == nil || !orderedSourcesUseStableIdentity(graph) {
		return nil
	}
	if tableErr := ensureOrderedSourceIdentityTables(ctx, db); tableErr != nil {
		return tableErr
	}

	currentRows, fingerprint, err := orderedSourceIdentityRows(graph)
	if err != nil {
		return err
	}

	persisted, err := persistedOrderedSourceIdentityRows(ctx, db)
	if err != nil {
		return err
	}
	return validatePersistedOrderedSourceRows(persisted, currentRows, fingerprint)
}

func persistedOrderedSourceIdentityRows(ctx context.Context, db *bun.DB) ([]orderedSourceIdentityRow, error) {
	var persisted []orderedSourceIdentityRow
	if err := db.NewSelect().Model(&persisted).Scan(ctx); err != nil {
		if isMissingOrderedIdentityTableError(err) {
			return nil, nil
		}
		return nil, err
	}
	return persisted, nil
}

func validatePersistedOrderedSourceRows(
	persisted []orderedSourceIdentityRow,
	currentRows []orderedSourceIdentityRow,
	fingerprint string,
) error {
	if len(persisted) == 0 {
		return nil
	}

	currentByKey := make(map[string]orderedSourceIdentityRow, len(currentRows))
	for _, row := range currentRows {
		currentByKey[row.SourceKey] = row
	}

	for _, observed := range persisted {
		expected, exists := currentByKey[observed.SourceKey]
		if !exists {
			return &OrderedSourceDriftError{
				SourceKey: observed.SourceKey,
				Field:     "source_key",
				Expected:  "registered source",
				Observed:  "missing",
			}
		}
		if err := compareOrderedSourceIdentityRow(observed, expected); err != nil {
			return err
		}
	}

	for _, observed := range persisted {
		if observed.GraphFingerprint != "" && observed.GraphFingerprint != fingerprint {
			return driftError(observed.SourceKey, "graph_fingerprint", fingerprint, observed.GraphFingerprint)
		}
	}

	return nil
}

func compareOrderedSourceIdentityRow(observed, expected orderedSourceIdentityRow) error {
	if observed.SourceName != expected.SourceName {
		return driftError(observed.SourceKey, "source_name", expected.SourceName, observed.SourceName)
	}
	if observed.SourceOrder != expected.SourceOrder {
		return driftError(observed.SourceKey, "source_order", fmt.Sprint(expected.SourceOrder), fmt.Sprint(observed.SourceOrder))
	}
	if observed.ResolvedPosition != expected.ResolvedPosition {
		return driftError(observed.SourceKey, "resolved_position", fmt.Sprint(expected.ResolvedPosition), fmt.Sprint(observed.ResolvedPosition))
	}
	if observed.IdentityMode != expected.IdentityMode {
		return driftError(observed.SourceKey, "identity_mode", expected.IdentityMode, observed.IdentityMode)
	}
	if !sameStrings([]string(observed.Dependencies), []string(expected.Dependencies)) {
		return driftError(observed.SourceKey, "dependencies", joinStrings([]string(expected.Dependencies)), joinStrings([]string(observed.Dependencies)))
	}
	return nil
}

func (m *Migrations) persistOrderedSourceGraph(ctx context.Context, db *bun.DB, graph []orderedSourceRegistration) error {
	if db == nil || !orderedSourcesUseStableIdentity(graph) {
		return nil
	}
	if err := ensureOrderedSourceIdentityTables(ctx, db); err != nil {
		return err
	}
	rows, _, err := orderedSourceIdentityRows(graph)
	if err != nil {
		return err
	}
	for _, row := range rows {
		row.UpdatedAt = time.Now()
		if _, err := db.NewInsert().
			Model(&row).
			On("CONFLICT (source_key) DO UPDATE").
			Set("source_name = EXCLUDED.source_name").
			Set("source_order = EXCLUDED.source_order").
			Set("dependencies = EXCLUDED.dependencies").
			Set("resolved_position = EXCLUDED.resolved_position").
			Set("identity_mode = EXCLUDED.identity_mode").
			Set("graph_fingerprint = EXCLUDED.graph_fingerprint").
			Set("updated_at = EXCLUDED.updated_at").
			Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func ensureOrderedSourceIdentityTables(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateTable().Model((*orderedSourceIdentityRow)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewCreateTable().Model((*orderedSourceAliasRow)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewCreateIndex().
		Model((*orderedSourceAliasRow)(nil)).
		Index("bun_ordered_migration_aliases_stable_name_unique").
		Column("stable_name").
		Unique().
		IfNotExists().
		Exec(ctx); err != nil {
		return err
	}
	return nil
}

func orderedSourceIdentityRows(graph []orderedSourceRegistration) ([]orderedSourceIdentityRow, string, error) {
	type fingerprintSource struct {
		SourceKey        string   `json:"source_key"`
		SourceName       string   `json:"source_name"`
		SourceOrder      int      `json:"source_order"`
		Dependencies     []string `json:"dependencies"`
		ResolvedPosition int      `json:"resolved_position"`
		IdentityMode     string   `json:"identity_mode"`
	}

	values := make([]fingerprintSource, 0, len(graph))
	for _, registration := range graph {
		if registration.identityMode != OrderedMigrationIdentitySourceStable {
			continue
		}
		deps := append([]string(nil), registration.dependsOn...)
		sort.Strings(deps)
		values = append(values, fingerprintSource{
			SourceKey:        registration.sourceKey,
			SourceName:       registration.name,
			SourceOrder:      registration.sourceOrder,
			Dependencies:     deps,
			ResolvedPosition: registration.resolvedPosition,
			IdentityMode:     registration.identityMode.String(),
		})
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].SourceKey < values[j].SourceKey
	})

	payload, err := json.Marshal(values)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(sum[:])

	rows := make([]orderedSourceIdentityRow, 0, len(values))
	for _, value := range values {
		rows = append(rows, orderedSourceIdentityRow{
			SourceKey:        value.SourceKey,
			SourceName:       value.SourceName,
			SourceOrder:      value.SourceOrder,
			Dependencies:     JSONStringSlice(value.Dependencies),
			ResolvedPosition: value.ResolvedPosition,
			IdentityMode:     value.IdentityMode,
			GraphFingerprint: fingerprint,
		})
	}
	return rows, fingerprint, nil
}

func (m *Migrations) orderedSourceAliases(ctx context.Context, db *bun.DB) (map[string]string, error) {
	out := make(map[string]string)
	if db == nil {
		return out, nil
	}
	if err := ensureOrderedSourceIdentityTables(ctx, db); err != nil {
		return nil, err
	}
	var rows []orderedSourceAliasRow
	if err := db.NewSelect().Model(&rows).Scan(ctx); err != nil {
		if isMissingOrderedIdentityTableError(err) {
			return out, nil
		}
		return nil, err
	}
	for _, row := range rows {
		out[row.LegacyName] = row.StableName
	}
	return out, nil
}

func (m *Migrations) BackfillStableOrderedMigrationMarkers(
	ctx context.Context,
	db *bun.DB,
	legacySources []OrderedMigrationSource,
	opts ...OrderedMigrationRepairOption,
) error {
	cfg := orderedMigrationRepairOptions(opts)
	if db == nil {
		return &OrderedSourceRepairError{
			Kind:        ErrOrderedSourceRepair,
			Expected:    "database handle",
			Observed:    "nil",
			Remediation: "provide a database handle so repair can inspect and write migration markers",
		}
	}

	resolved, err := m.resolveStableOrderedRepairPlan(ctx, db)
	if err != nil {
		return err
	}

	_, legacyMetadata, err := compileLegacyOrderedSources(ctx, db, legacySources)
	if err != nil {
		return err
	}

	migrator, applied, err := newOrderedRepairMigrator(ctx, db, resolved.migrations)
	if err != nil {
		return err
	}
	operations, err := orderedRepairOperations(applied, legacyMetadata, resolved.entryByName)
	if err != nil {
		return err
	}
	if err := applyOrderedRepairOperations(ctx, db, migrator, operations, cfg); err != nil {
		return err
	}

	return m.persistOrderedSourceGraph(ctx, db, resolved.orderedGraph)
}

func orderedMigrationRepairOptions(opts []OrderedMigrationRepairOption) OrderedMigrationRepairOptions {
	cfg := OrderedMigrationRepairOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func (m *Migrations) resolveStableOrderedRepairPlan(ctx context.Context, db *bun.DB) (*resolvedMigrationPlan, error) {
	resolved, err := m.resolvePlan(ctx, db, resolvePlanOptions{})
	if err != nil {
		return nil, err
	}
	if !orderedSourcesUseStableIdentity(resolved.orderedGraph) {
		return nil, &OrderedSourceRepairError{
			Kind:        ErrOrderedSourceRepair,
			Expected:    "current ordered graph uses source-stable identity",
			Observed:    "positional or empty ordered graph",
			Remediation: "register current ordered sources with source-stable identity before running repair",
		}
	}
	if tableErr := ensureOrderedSourceIdentityTables(ctx, db); tableErr != nil {
		return nil, tableErr
	}
	return resolved, nil
}

func newOrderedRepairMigrator(
	ctx context.Context,
	db *bun.DB,
	migrations *migrate.Migrations,
) (*migrate.Migrator, migrate.MigrationSlice, error) {
	migrator := migrate.NewMigrator(db, migrations, migrate.WithUpsert(true))
	if initErr := migrator.Init(ctx); initErr != nil {
		return nil, nil, initErr
	}
	applied, err := allAppliedMigrations(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	return migrator, applied, nil
}

func orderedRepairOperations(
	applied migrate.MigrationSlice,
	legacyMetadata map[string]OrderedMigrationMetadata,
	entryByName map[string]MigrationPlanEntry,
) ([]orderedRepairOperation, error) {
	appliedLegacyNames := appliedLegacyOrderedMarkerNames(applied)
	if err := validateAppliedLegacyMappings(appliedLegacyNames, legacyMetadata); err != nil {
		return nil, err
	}

	appliedByName := migrationNameSet(applied)
	currentBySourceVersion := currentOrderedEntriesBySourceVersion(entryByName)
	operations := make([]orderedRepairOperation, 0, len(appliedLegacyNames))
	for legacyName, legacyMeta := range legacyMetadata {
		if _, ok := appliedByName[legacyName]; !ok {
			continue
		}
		current, ok := currentBySourceVersion[legacyMeta.SourceName+"/"+legacyMeta.OriginalVersion]
		if !ok {
			return nil, repairMarkerMismatchError(legacyName, legacyMeta)
		}
		operations = append(operations, orderedRepairOperation{
			legacyName: legacyName,
			stableName: current.SyntheticName,
			sourceKey:  current.SourceKey,
			groupID:    legacyAppliedGroup(applied, legacyName),
		})
	}
	return operations, nil
}

func validateAppliedLegacyMappings(
	appliedLegacyNames []string,
	legacyMetadata map[string]OrderedMigrationMetadata,
) error {
	for _, legacyName := range appliedLegacyNames {
		if _, ok := legacyMetadata[legacyName]; ok {
			continue
		}
		return &OrderedSourceRepairError{
			Kind:        ErrOrderedSourceRepairMissingMapping,
			LegacyName:  legacyName,
			Expected:    "caller-provided legacy source mapping includes applied positional marker",
			Observed:    "marker missing from compiled legacy mapping",
			Remediation: "include every historical ordered source in its original positional order before retrying repair",
		}
	}
	return nil
}

func currentOrderedEntriesBySourceVersion(entryByName map[string]MigrationPlanEntry) map[string]MigrationPlanEntry {
	currentBySourceVersion := make(map[string]MigrationPlanEntry, len(entryByName))
	for _, entry := range entryByName {
		if entry.SourceKind != MigrationSourceKindOrdered {
			continue
		}
		currentBySourceVersion[entry.SourceName+"/"+entry.OriginalVersion] = entry
	}
	return currentBySourceVersion
}

func repairMarkerMismatchError(legacyName string, legacyMeta OrderedMigrationMetadata) error {
	return &OrderedSourceRepairError{
		Kind:        ErrOrderedSourceRepairMarkerMismatch,
		LegacyName:  legacyName,
		SourceName:  legacyMeta.SourceName,
		Expected:    "current source-stable migration for source/version",
		Observed:    legacyMeta.OriginalVersion,
		Remediation: "register the matching current source-stable source or provide the historical source mapping that matches this database",
	}
}

func applyOrderedRepairOperations(
	ctx context.Context,
	db *bun.DB,
	migrator *migrate.Migrator,
	operations []orderedRepairOperation,
	cfg OrderedMigrationRepairOptions,
) error {
	for _, operation := range operations {
		if err := markOrderedRepairApplied(ctx, migrator, operation); err != nil {
			return err
		}
		if err := upsertOrderedRepairAlias(ctx, db, operation); err != nil {
			return err
		}
		if cfg.CleanupLegacyMarkers {
			if err := cleanupOrderedRepairLegacyMarker(ctx, db, operation); err != nil {
				return err
			}
		}
	}
	return nil
}

func markOrderedRepairApplied(
	ctx context.Context,
	migrator *migrate.Migrator,
	operation orderedRepairOperation,
) error {
	migration := migrate.Migration{
		Name:    operation.stableName,
		GroupID: operation.groupID,
	}
	if err := migrator.MarkApplied(ctx, &migration); err != nil {
		return &OrderedSourceRepairError{
			Kind:        ErrOrderedSourceRepair,
			LegacyName:  operation.legacyName,
			SourceKey:   operation.sourceKey,
			Expected:    "source-stable applied marker can be written",
			Observed:    err.Error(),
			Remediation: "inspect bun_migrations constraints and retry after resolving the write failure",
		}
	}
	return nil
}

func upsertOrderedRepairAlias(ctx context.Context, db *bun.DB, operation orderedRepairOperation) error {
	alias := orderedSourceAliasRow{
		LegacyName: operation.legacyName,
		StableName: operation.stableName,
		SourceKey:  operation.sourceKey,
	}
	if _, err := db.NewInsert().
		Model(&alias).
		On("CONFLICT (legacy_name) DO UPDATE").
		Set("stable_name = EXCLUDED.stable_name").
		Set("source_key = EXCLUDED.source_key").
		Exec(ctx); err != nil {
		return &OrderedSourceRepairError{
			Kind:        ErrOrderedSourceRepair,
			LegacyName:  operation.legacyName,
			SourceKey:   operation.sourceKey,
			Expected:    "legacy alias can be written",
			Observed:    err.Error(),
			Remediation: "inspect bun_ordered_migration_aliases constraints and retry after resolving the write failure",
		}
	}
	return nil
}

func cleanupOrderedRepairLegacyMarker(ctx context.Context, db *bun.DB, operation orderedRepairOperation) error {
	if _, err := db.NewDelete().
		Model((*migrate.Migration)(nil)).
		ModelTableExpr("bun_migrations").
		Where("name = ?", operation.legacyName).
		Exec(ctx); err != nil {
		return &OrderedSourceRepairError{
			Kind:        ErrOrderedSourceRepair,
			LegacyName:  operation.legacyName,
			SourceKey:   operation.sourceKey,
			Expected:    "legacy marker cleanup succeeds",
			Observed:    err.Error(),
			Remediation: "retry without cleanup or inspect bun_migrations before retrying cleanup",
		}
	}
	return nil
}

type OrderedMigrationRepairOption func(*OrderedMigrationRepairOptions)

func WithOrderedMigrationRepairCleanupLegacyMarkers(enabled bool) OrderedMigrationRepairOption {
	return func(opts *OrderedMigrationRepairOptions) {
		if opts != nil {
			opts.CleanupLegacyMarkers = enabled
		}
	}
}

func compileLegacyOrderedSources(
	ctx context.Context,
	db *bun.DB,
	sources []OrderedMigrationSource,
) (*migrate.Migrations, map[string]OrderedMigrationMetadata, error) {
	migrations := migrate.NewMigrations()
	metadata := make(map[string]OrderedMigrationMetadata)
	for idx, source := range sources {
		name := strings.TrimSpace(source.Name)
		if name == "" {
			return nil, nil, &OrderedSourceRepairError{
				Kind:        ErrOrderedSourceRepair,
				Expected:    fmt.Sprintf("legacy source at index %d has a name", idx),
				Observed:    "empty name",
				Remediation: "pass the complete historical ordered source list in its original order",
			}
		}
		if source.Root == nil {
			return nil, nil, &OrderedSourceRepairError{
				Kind:        ErrOrderedSourceRepair,
				SourceName:  name,
				Expected:    "legacy source root filesystem",
				Observed:    "nil",
				Remediation: "provide the filesystem used to compile this historical ordered source",
			}
		}
		registration, err := normalizeOrderedSourceRegistration(OrderedMigrationSource{Name: name, Root: source.Root}, name, idx)
		if err != nil {
			return nil, nil, err
		}
		opts := defaultDialectOptions()
		for _, opt := range source.Options {
			if opt != nil {
				opt(&opts)
			}
		}
		registration.registration = dialectRegistration{root: source.Root, opts: opts}
		buildResult, err := registration.registration.buildFileSystems(ctx, db)
		if err != nil {
			return nil, nil, err
		}
		compiled, meta, err := compileOrderedSourceMigrations(registration, buildResult.sourceLayers)
		if err != nil {
			return nil, nil, err
		}
		for _, migration := range compiled {
			migrations.Add(migration)
		}
		for key, value := range meta {
			if previous, exists := metadata[key]; exists {
				return nil, nil, &OrderedSourceRepairError{
					Kind:       ErrOrderedSourceRepairAmbiguousMarker,
					LegacyName: key,
					SourceName: value.SourceName,
					Expected:   "one legacy source/version per positional marker",
					Observed: fmt.Sprintf(
						"sources %q version %q and %q version %q",
						previous.SourceName,
						previous.OriginalVersion,
						value.SourceName,
						value.OriginalVersion,
					),
					Remediation: "check the provided historical source ordering and migration files before retrying repair",
				}
			}
			metadata[key] = value
		}
	}
	return migrations, metadata, nil
}

func legacyAppliedGroup(applied migrate.MigrationSlice, name string) int64 {
	for _, migration := range applied {
		if migration.Name == name {
			return migration.GroupID
		}
	}
	return 0
}

func allAppliedMigrations(ctx context.Context, db *bun.DB) (migrate.MigrationSlice, error) {
	var rows []appliedMigrationRow
	err := db.NewSelect().
		Model(&rows).
		ModelTableExpr("bun_migrations").
		ColumnExpr("name").
		ColumnExpr("group_id").
		ColumnExpr("migrated_at").
		Scan(ctx)
	if err != nil {
		if isMissingMigrationsTableError(err) {
			return nil, nil
		}
		return nil, err
	}
	applied := make(migrate.MigrationSlice, 0, len(rows))
	for _, row := range rows {
		applied = append(applied, migrate.Migration{
			Name:       row.Name,
			GroupID:    row.GroupID,
			MigratedAt: row.MigratedAt,
		})
	}
	return applied, nil
}

func appliedLegacyOrderedMarkerNames(applied migrate.MigrationSlice) []string {
	names := make([]string, 0)
	for _, migration := range applied {
		if isLegacyOrderedMarkerName(migration.Name) {
			names = append(names, migration.Name)
		}
	}
	sort.Strings(names)
	return names
}

func isLegacyOrderedMarkerName(name string) bool {
	if len(name) != len("ord_000001_000001") {
		return false
	}
	if !strings.HasPrefix(name, "ord_") {
		return false
	}
	for idx, ch := range name {
		switch {
		case idx < 3:
			continue
		case idx == 3 || idx == 10:
			if ch != '_' {
				return false
			}
		case ch < '0' || ch > '9':
			return false
		}
	}
	return true
}

func driftError(sourceKey, field, expected, observed string) error {
	return &OrderedSourceDriftError{
		SourceKey: sourceKey,
		Field:     field,
		Expected:  expected,
		Observed:  observed,
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	for idx := range a {
		if a[idx] != b[idx] {
			return false
		}
	}
	return true
}

func joinStrings(values []string) string {
	values = append([]string(nil), values...)
	sort.Strings(values)
	payload, _ := json.Marshal(values)
	return string(payload)
}

func isMissingOrderedIdentityTableError(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "bun_ordered_migration_sources") &&
		(strings.Contains(value, "does not exist") || strings.Contains(value, "no such table") || strings.Contains(value, "unknown table"))
}
