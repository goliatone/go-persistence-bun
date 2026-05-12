package persistence

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	ErrOrderedSourceInvalidConfig   = errors.New("invalid ordered migration source configuration")
	ErrOrderedSourceMixedIdentity   = errors.New("mixed ordered migration source identity modes")
	ErrOrderedSourceDuplicateKey    = errors.New("duplicate ordered migration source key")
	ErrOrderedSourceUnknownDep      = errors.New("unknown ordered migration source dependency")
	ErrOrderedSourceCycle           = errors.New("ordered migration source dependency cycle")
	ErrOrderedSourceOrderInversion  = errors.New("ordered migration source dependency order inversion")
	ErrOrderedSourceMissingSelected = errors.New("selected ordered migration source dependency is missing")
)

type OrderedSourceGraphError struct {
	Kind       error
	SourceName string
	SourceKey  string
	Dependency string
	Message    string
}

func (e *OrderedSourceGraphError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return e.Kind.Error()
}

func (e *OrderedSourceGraphError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Kind
}

func resolveOrderedSourceGraph(registrations []orderedSourceRegistration) ([]orderedSourceRegistration, error) {
	if len(registrations) == 0 {
		return nil, nil
	}

	hasStable, err := orderedSourceGraphUsesStableIdentity(registrations)
	if err != nil {
		return nil, err
	}
	if !hasStable {
		return resolvePositionalOrderedSourceGraph(registrations), nil
	}

	byKey, err := indexStableOrderedSources(registrations)
	if err != nil {
		return nil, err
	}
	if err := validateStableOrderedSourceDependencies(registrations, byKey); err != nil {
		return nil, err
	}

	return resolveStableOrderedSourceGraph(registrations), nil
}

func orderedSourceGraphUsesStableIdentity(registrations []orderedSourceRegistration) (bool, error) {
	hasStable := false
	hasPositional := false
	for _, registration := range registrations {
		switch registration.identityMode {
		case OrderedMigrationIdentitySourceStable:
			hasStable = true
		case OrderedMigrationIdentityPositional:
			hasPositional = true
		default:
			return false, &OrderedSourceGraphError{
				Kind:       ErrOrderedSourceInvalidConfig,
				SourceName: registration.name,
				Message:    fmt.Sprintf("ordered migration source %q has unsupported identity mode %s", registration.name, registration.identityMode.String()),
			}
		}
	}
	if hasStable && hasPositional {
		return false, &OrderedSourceGraphError{
			Kind:    ErrOrderedSourceMixedIdentity,
			Message: "ordered migration sources must be all positional or all source-stable",
		}
	}
	return hasStable, nil
}

func resolvePositionalOrderedSourceGraph(registrations []orderedSourceRegistration) []orderedSourceRegistration {
	out := append([]orderedSourceRegistration(nil), registrations...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].sequence < out[j].sequence
	})
	for idx := range out {
		out[idx].resolvedPosition = idx + 1
	}
	return out
}

func indexStableOrderedSources(registrations []orderedSourceRegistration) (map[string]orderedSourceRegistration, error) {
	byKey := make(map[string]orderedSourceRegistration, len(registrations))
	for _, registration := range registrations {
		if err := validateStableOrderedSource(registration, byKey); err != nil {
			return nil, err
		}
		byKey[registration.sourceKey] = registration
	}
	return byKey, nil
}

func validateStableOrderedSource(
	registration orderedSourceRegistration,
	byKey map[string]orderedSourceRegistration,
) error {
	if registration.sourceOrder <= 0 {
		return &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: registration.name,
			SourceKey:  registration.sourceKey,
			Message:    fmt.Sprintf("ordered migration source %q must have a positive order in source-stable mode", registration.name),
		}
	}
	if registration.sourceOrder > MaxOrderedMigrationSourceOrder {
		return &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: registration.name,
			SourceKey:  registration.sourceKey,
			Message: fmt.Sprintf(
				"ordered migration source %q order %d exceeds the source-stable maximum %d",
				registration.name,
				registration.sourceOrder,
				MaxOrderedMigrationSourceOrder,
			),
		}
	}
	if registration.sourceKey == "" {
		return &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceInvalidConfig,
			SourceName: registration.name,
			Message:    fmt.Sprintf("ordered migration source %q must have a source key in source-stable mode", registration.name),
		}
	}
	if prev, exists := byKey[registration.sourceKey]; exists {
		return &OrderedSourceGraphError{
			Kind:       ErrOrderedSourceDuplicateKey,
			SourceName: registration.name,
			SourceKey:  registration.sourceKey,
			Message:    fmt.Sprintf("duplicate ordered migration source key %q for sources %q and %q", registration.sourceKey, prev.name, registration.name),
		}
	}
	return nil
}

func validateStableOrderedSourceDependencies(
	registrations []orderedSourceRegistration,
	byKey map[string]orderedSourceRegistration,
) error {
	if err := validateKnownStableOrderedSourceDependencies(registrations, byKey); err != nil {
		return err
	}
	if cycle := detectOrderedSourceCycle(registrations, byKey); len(cycle) > 0 {
		return &OrderedSourceGraphError{
			Kind:    ErrOrderedSourceCycle,
			Message: fmt.Sprintf("ordered migration source dependency cycle: %s", strings.Join(cycle, " -> ")),
		}
	}
	return validateStableOrderedSourceDependencyOrders(registrations, byKey)
}

func validateKnownStableOrderedSourceDependencies(
	registrations []orderedSourceRegistration,
	byKey map[string]orderedSourceRegistration,
) error {
	for _, registration := range registrations {
		for _, dependencyKey := range registration.dependsOn {
			if _, exists := byKey[dependencyKey]; !exists {
				return &OrderedSourceGraphError{
					Kind:       ErrOrderedSourceUnknownDep,
					SourceName: registration.name,
					SourceKey:  registration.sourceKey,
					Dependency: dependencyKey,
					Message:    fmt.Sprintf("ordered migration source %q (%s) depends on unknown source key %q", registration.name, registration.sourceKey, dependencyKey),
				}
			}
		}
	}
	return nil
}

func validateStableOrderedSourceDependencyOrders(
	registrations []orderedSourceRegistration,
	byKey map[string]orderedSourceRegistration,
) error {
	for _, registration := range registrations {
		for _, dependencyKey := range registration.dependsOn {
			dependency := byKey[dependencyKey]
			if dependency.sourceOrder >= registration.sourceOrder {
				return &OrderedSourceGraphError{
					Kind:       ErrOrderedSourceOrderInversion,
					SourceName: registration.name,
					SourceKey:  registration.sourceKey,
					Dependency: dependencyKey,
					Message: fmt.Sprintf(
						"ordered migration source %q (%s order %d) depends on %q with non-lower order %d",
						registration.name,
						registration.sourceKey,
						registration.sourceOrder,
						dependencyKey,
						dependency.sourceOrder,
					),
				}
			}
		}
	}
	return nil
}

func resolveStableOrderedSourceGraph(registrations []orderedSourceRegistration) []orderedSourceRegistration {
	out := append([]orderedSourceRegistration(nil), registrations...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].sourceOrder == out[j].sourceOrder {
			if out[i].sourceKey == out[j].sourceKey {
				return out[i].sequence < out[j].sequence
			}
			return out[i].sourceKey < out[j].sourceKey
		}
		return out[i].sourceOrder < out[j].sourceOrder
	})
	for idx := range out {
		out[idx].resolvedPosition = idx + 1
	}
	return out
}

func detectOrderedSourceCycle(
	registrations []orderedSourceRegistration,
	byKey map[string]orderedSourceRegistration,
) []string {
	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	state := make(map[string]int, len(registrations))
	stack := make([]string, 0, len(registrations))

	var visit func(key string) []string
	visit = func(key string) []string {
		switch state[key] {
		case visiting:
			for idx, stacked := range stack {
				if stacked == key {
					return append(append([]string(nil), stack[idx:]...), key)
				}
			}
			return []string{key, key}
		case visited:
			return nil
		}

		state[key] = visiting
		stack = append(stack, key)
		registration := byKey[key]
		for _, dependencyKey := range registration.dependsOn {
			if _, exists := byKey[dependencyKey]; !exists {
				continue
			}
			if cycle := visit(dependencyKey); len(cycle) > 0 {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		state[key] = visited
		return nil
	}

	keys := make([]string, 0, len(registrations))
	for _, registration := range registrations {
		keys = append(keys, registration.sourceKey)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if cycle := visit(key); len(cycle) > 0 {
			return cycle
		}
	}
	return nil
}

func orderedSourcesUseStableIdentity(registrations []orderedSourceRegistration) bool {
	for _, registration := range registrations {
		if registration.identityMode == OrderedMigrationIdentitySourceStable {
			return true
		}
	}
	return false
}
