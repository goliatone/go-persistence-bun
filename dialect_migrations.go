package persistence

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing/fstest"

	apierrors "github.com/goliatone/go-errors"
	"github.com/uptrace/bun"
)

const (
	commonDirName           = "common"
	defaultDialectName      = "postgres"
	dialectAnnotationPrefix = "---bun:dialect:"
	sqlFileExtension        = ".sql"
)

// DialectValidationFunc is invoked when validation detects missing coverage.
type DialectValidationFunc func(ctx context.Context, result DialectValidationResult) error

// DialectValidationResult summarizes the dialect coverage outcome for a registration.
type DialectValidationResult struct {
	SourceLabel      string
	RegistrationIdx  int
	CheckedDialects  []string
	MissingDialects  map[string][]string
	DialectAliases   map[string]string
	AvailableLayers  []layerDiagnostic
	RequestedTargets []string
}

var defaultDialectAliases = map[string]string{
	"postgres":   "postgres",
	"postgresql": "postgres",
	"pg":         "postgres",
	"pgdialect":  "postgres",
	"sqlite":     "sqlite",
	"sqlite3":    "sqlite",
	"sqldialect": "sqlite",
}

// DialectMigrationOption configures dialect-aware migration registration.
type DialectMigrationOption func(*dialectOptions)

// DialectResolver allows callers to supply a dialect name dynamically.
type DialectResolver func(ctx context.Context, db *bun.DB) (string, error)

type migrationLayer int

const (
	layerRoot migrationLayer = iota
	layerCommon
	layerDialect
)

type dialectOptions struct {
	explicitDialect string
	defaultDialect  string
	aliases         map[string]string
	resolver        DialectResolver
	validator       DialectValidationFunc
	validateDefault bool
	rawTargets      []string
	sourceLabel     string
}

type dialectRegistration struct {
	root fs.FS
	opts dialectOptions
}

type dialectBuildResult struct {
	dialect     string
	fileSystems []fs.FS
	diagnostics []layerDiagnostic
}

func (r dialectBuildResult) hasSQL() bool {
	for _, diag := range r.diagnostics {
		if diag.Files > 0 {
			return true
		}
	}
	return false
}

type layerDiagnostic struct {
	Layer  migrationLayer
	Name   string
	Files  int
	Reason string
}

func defaultDialectOptions() dialectOptions {
	return dialectOptions{
		defaultDialect: defaultDialectName,
		aliases:        copyDialectAliases(defaultDialectAliases),
		sourceLabel:    "<embedded fs>",
	}
}

func copyDialectAliases(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[strings.ToLower(k)] = strings.ToLower(v)
	}
	return dst
}

// WithDialectName forces a specific dialect to be used for this registration.
func WithDialectName(name string) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		opts.explicitDialect = opts.normalize(name)
	}
}

// WithDefaultDialect overrides the fallback dialect used when detection fails.
func WithDefaultDialect(name string) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		if normalized := opts.normalize(name); normalized != "" {
			opts.defaultDialect = normalized
		}
	}
}

// WithDialectAliases extends or overrides the built-in alias map.
func WithDialectAliases(overrides map[string]string) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		for alias, canonical := range overrides {
			a := strings.ToLower(strings.TrimSpace(alias))
			c := strings.ToLower(strings.TrimSpace(canonical))
			if a == "" || c == "" {
				continue
			}
			opts.aliases[a] = c
			if _, ok := opts.aliases[c]; !ok {
				opts.aliases[c] = c
			}
		}
	}
}

// WithDialectResolver sets a callback that resolves the active dialect at runtime.
func WithDialectResolver(resolver DialectResolver) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		opts.resolver = resolver
	}
}

// WithValidationTargets enables dialect validation for the provided targets.
// Passing no names causes the resolved dialect to be validated.
func WithValidationTargets(names ...string) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		if len(names) == 0 {
			opts.validateDefault = true
			opts.rawTargets = nil
			return
		}
		opts.rawTargets = append([]string(nil), names...)
		opts.validateDefault = false
	}
}

// WithDialectValidator overrides the default panic-on-failure behavior.
func WithDialectValidator(fn DialectValidationFunc) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		opts.validator = fn
	}
}

// WithDialectSourceLabel sets a human-readable label used in validation errors.
func WithDialectSourceLabel(label string) DialectMigrationOption {
	return func(opts *dialectOptions) {
		if opts == nil {
			return
		}
		opts.sourceLabel = strings.TrimSpace(label)
		if opts.sourceLabel == "" {
			opts.sourceLabel = "<embedded fs>"
		}
	}
}

func (o dialectOptions) normalize(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "" {
		return ""
	}
	if canonical, ok := o.aliases[n]; ok {
		return canonical
	}
	return n
}

func (o dialectOptions) validationTargets() []string {
	if len(o.rawTargets) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	targets := make([]string, 0, len(o.rawTargets))
	for _, raw := range o.rawTargets {
		if normalized := o.normalize(raw); normalized != "" {
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			targets = append(targets, normalized)
		}
	}
	return targets
}

func (o dialectOptions) candidateDirectories(name string) []string {
	canonical := o.normalize(name)
	if canonical == "" {
		return nil
	}
	seen := map[string]struct{}{}
	var dirs []string
	add := func(value string) {
		if value == "" {
			return
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		dirs = append(dirs, value)
	}
	add(canonical)
	for alias, target := range o.aliases {
		if target == canonical {
			add(alias)
		}
	}
	return dirs
}

func (o dialectOptions) extractDialects(data []byte) []string {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var dialects []string
	seen := map[string]struct{}{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !strings.HasPrefix(strings.ToLower(line), dialectAnnotationPrefix) {
			continue
		}
		value := strings.TrimSpace(line[len(dialectAnnotationPrefix):])
		if value == "" {
			continue
		}
		fields := strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ';' || r == ' ' || r == '\t'
		})
		for _, field := range fields {
			if normalized := o.normalize(field); normalized != "" {
				if _, ok := seen[normalized]; ok {
					continue
				}
				seen[normalized] = struct{}{}
				dialects = append(dialects, normalized)
			}
		}
	}
	return dialects
}

func (r dialectRegistration) buildFileSystems(ctx context.Context, db *bun.DB) (dialectBuildResult, error) {
	dialectName, err := r.resolveDialect(ctx, db)
	if err != nil {
		return dialectBuildResult{}, err
	}

	return r.buildForDialect(dialectName)
}

func (r dialectRegistration) buildForDialect(name string) (dialectBuildResult, error) {
	builder := dialectFSBuilder{
		root:    r.root,
		dialect: name,
		opts:    r.opts,
	}
	return builder.build()
}

func (r dialectRegistration) resolveDialect(ctx context.Context, db *bun.DB) (string, error) {
	if r.opts.explicitDialect != "" {
		return r.opts.explicitDialect, nil
	}

	if r.opts.resolver != nil {
		name, err := r.opts.resolver(ctx, db)
		if err != nil {
			return "", apierrors.Wrap(err, apierrors.CategoryInternal, "dialect resolver failed")
		}
		if normalized := r.opts.normalize(name); normalized != "" {
			return normalized, nil
		}
	}

	if db != nil && db.Dialect() != nil {
		if dialectName := db.Dialect().Name().String(); dialectName != "" {
			if normalized := r.opts.normalize(dialectName); normalized != "" {
				return normalized, nil
			}
		}
	}

	if r.opts.defaultDialect != "" {
		return r.opts.defaultDialect, nil
	}

	return defaultDialectName, nil
}

func (r dialectRegistration) validate(ctx context.Context, db *bun.DB, idx int) error {
	targets := r.opts.validationTargets()
	targetSet := map[string]struct{}{}
	normalizedTargets := make([]string, 0, len(targets))

	for _, target := range targets {
		if target == "" {
			continue
		}
		if _, ok := targetSet[target]; ok {
			continue
		}
		targetSet[target] = struct{}{}
		normalizedTargets = append(normalizedTargets, target)
	}

	if r.opts.validateDefault {
		resolved, err := r.resolveDialect(ctx, db)
		if err != nil {
			return err
		}
		if resolved != "" {
			if _, ok := targetSet[resolved]; !ok {
				targetSet[resolved] = struct{}{}
				normalizedTargets = append(normalizedTargets, resolved)
			}
		}
	}

	if len(normalizedTargets) == 0 {
		return nil
	}

	result := DialectValidationResult{
		SourceLabel:      r.opts.sourceLabel,
		RegistrationIdx:  idx,
		CheckedDialects:  make([]string, 0, len(normalizedTargets)),
		MissingDialects:  map[string][]string{},
		DialectAliases:   copyDialectAliases(r.opts.aliases),
		RequestedTargets: append([]string(nil), normalizedTargets...),
	}

	for _, target := range normalizedTargets {
		buildResult, err := r.buildForDialect(target)
		if err != nil {
			return err
		}
		result.CheckedDialects = append(result.CheckedDialects, target)
		if buildResult.hasSQL() {
			continue
		}
		result.AvailableLayers = append(result.AvailableLayers, buildResult.diagnostics...)
		result.MissingDialects[target] = reasonsFromDiagnostics(buildResult.diagnostics)
	}

	if len(result.MissingDialects) == 0 {
		return nil
	}

	validator := r.opts.validator
	if validator == nil {
		validator = defaultDialectValidator
	}
	if err := validator(ctx, result); err != nil {
		return err
	}
	return nil
}

type dialectFSBuilder struct {
	root    fs.FS
	dialect string
	opts    dialectOptions
}

func (b dialectFSBuilder) build() (dialectBuildResult, error) {
	result := dialectBuildResult{
		dialect:     b.dialect,
		fileSystems: make([]fs.FS, 0, 3),
		diagnostics: make([]layerDiagnostic, 0, 3),
	}

	if fsCommon, diag, err := b.buildCommonLayer(); err != nil {
		result.diagnostics = append(result.diagnostics, diag)
		return result, err
	} else {
		result.diagnostics = append(result.diagnostics, diag)
		if fsCommon != nil {
			result.fileSystems = append(result.fileSystems, fsCommon)
		}
	}

	if fsRoot, diag, err := b.buildRootLayer(); err != nil {
		result.diagnostics = append(result.diagnostics, diag)
		return result, err
	} else {
		result.diagnostics = append(result.diagnostics, diag)
		if fsRoot != nil {
			result.fileSystems = append(result.fileSystems, fsRoot)
		}
	}

	if fsDialect, diag, err := b.buildDialectLayer(); err != nil {
		result.diagnostics = append(result.diagnostics, diag)
		return result, err
	} else {
		result.diagnostics = append(result.diagnostics, diag)
		if fsDialect != nil {
			result.fileSystems = append(result.fileSystems, fsDialect)
		}
	}

	return result, nil
}

func (b dialectFSBuilder) buildCommonLayer() (fs.FS, layerDiagnostic, error) {
	diag := layerDiagnostic{
		Layer: layerCommon,
		Name:  commonDirName,
	}
	sub, exists, err := openSubFS(b.root, commonDirName)
	if err != nil {
		diag.Reason = err.Error()
		return nil, diag, err
	}
	if !exists {
		diag.Reason = "directory not found"
		return nil, diag, nil
	}
	fsCommon, detail, err := b.collectLayer(sub, layerCommon, commonDirName, false)
	if err != nil {
		return nil, detail, err
	}
	return fsCommon, detail, nil
}

func (b dialectFSBuilder) buildRootLayer() (fs.FS, layerDiagnostic, error) {
	return b.collectLayer(b.root, layerRoot, "root", true)
}

func (b dialectFSBuilder) buildDialectLayer() (fs.FS, layerDiagnostic, error) {
	diag := layerDiagnostic{
		Layer: layerDialect,
		Name:  b.dialect,
	}
	candidates := b.opts.candidateDirectories(b.dialect)
	for _, candidate := range candidates {
		sub, exists, err := openSubFS(b.root, candidate)
		if err != nil {
			diag.Reason = err.Error()
			return nil, diag, err
		}
		if !exists {
			continue
		}
		fsDialect, detail, err := b.collectLayer(sub, layerDialect, candidate, false)
		if err != nil {
			return nil, detail, err
		}
		return fsDialect, detail, nil
	}
	if len(candidates) > 0 {
		diag.Reason = fmt.Sprintf("no dialect-specific directory found (searched: %s)", strings.Join(candidates, ", "))
	} else {
		diag.Reason = "no dialect directory configured"
	}
	return nil, diag, nil
}

func (b dialectFSBuilder) collectLayer(fsys fs.FS, layer migrationLayer, name string, skipSubdirs bool) (fs.FS, layerDiagnostic, error) {
	diag := layerDiagnostic{
		Layer: layer,
		Name:  name,
	}
	files := fstest.MapFS{}
	totalCandidates := 0

	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}
		if d.IsDir() {
			if skipSubdirs {
				return fs.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(strings.ToLower(path), sqlFileExtension) {
			return nil
		}

		totalCandidates++

		data, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		if !b.shouldInclude(data) {
			return nil
		}

		files[path] = &fstest.MapFile{
			Data: data,
			Mode: 0o644,
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			diag.Reason = "directory not found"
			return nil, diag, nil
		}
		diag.Reason = err.Error()
		return nil, diag, err
	}

	diag.Files = len(files)
	if diag.Files == 0 {
		if totalCandidates == 0 {
			diag.Reason = fmt.Sprintf("no SQL files found in %s", name)
		} else {
			diag.Reason = fmt.Sprintf("SQL files exist but none match dialect %q", b.dialect)
		}
		return nil, diag, nil
	}

	return files, diag, nil
}

func (b dialectFSBuilder) shouldInclude(data []byte) bool {
	dialects := b.opts.extractDialects(data)
	if len(dialects) == 0 {
		return true
	}
	for _, dialect := range dialects {
		if dialect == b.dialect {
			return true
		}
	}
	return false
}

func openSubFS(fsys fs.FS, dir string) (fs.FS, bool, error) {
	if dir == "" {
		return nil, false, nil
	}
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) && errors.Is(pathErr.Err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return sub, true, nil
}

func reasonsFromDiagnostics(diags []layerDiagnostic) []string {
	var reasons []string
	for _, diag := range diags {
		if diag.Files > 0 {
			continue
		}
		if diag.Reason == "" {
			continue
		}
		label := diag.Name
		if label == "" {
			label = diag.layerName()
		}
		reasons = append(reasons, fmt.Sprintf("%s: %s", label, diag.Reason))
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "no SQL files discovered across any layer")
	}
	return reasons
}

func (d layerDiagnostic) layerName() string {
	switch d.Layer {
	case layerCommon:
		return "common"
	case layerDialect:
		return "dialect-specific"
	default:
		return "root"
	}
}

func defaultDialectValidator(_ context.Context, result DialectValidationResult) error {
	var b strings.Builder
	label := result.SourceLabel
	if label == "" {
		label = "<embedded fs>"
	}
	fmt.Fprintf(&b, "dialect migrations validation failed for %s (registration #%d)", label, result.RegistrationIdx)
	for dialect, reasons := range result.MissingDialects {
		fmt.Fprintf(&b, "\n  - %s:", dialect)
		for _, reason := range reasons {
			fmt.Fprintf(&b, " %s;", reason)
		}
	}
	panic(b.String())
}
