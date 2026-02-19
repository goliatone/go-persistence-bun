package persistence

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// JSONMap is a portable JSON object wrapper for Bun models.
// It is suitable for PostgreSQL JSONB and SQLite JSON/TEXT columns.
type JSONMap map[string]any

// Scan implements sql.Scanner.
func (m *JSONMap) Scan(src any) error {
	if m == nil {
		return fmt.Errorf("persistence: JSONMap scan target is nil")
	}

	data, err := jsonDataFromSource(src)
	if err != nil {
		return fmt.Errorf("persistence: JSONMap scan: %w", err)
	}
	if data == nil {
		*m = nil
		return nil
	}

	out := map[string]any{}
	if err := json.Unmarshal(data, &out); err != nil {
		return fmt.Errorf("persistence: JSONMap decode: %w", err)
	}

	*m = JSONMap(out)
	return nil
}

// Value implements driver.Valuer.
func (m JSONMap) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	payload, err := json.Marshal(map[string]any(m))
	if err != nil {
		return nil, fmt.Errorf("persistence: JSONMap encode: %w", err)
	}
	return payload, nil
}

// JSONStringSlice is a portable JSON string-array wrapper for Bun models.
// It is suitable for PostgreSQL JSONB and SQLite JSON/TEXT columns.
type JSONStringSlice []string

// Scan implements sql.Scanner.
func (s *JSONStringSlice) Scan(src any) error {
	if s == nil {
		return fmt.Errorf("persistence: JSONStringSlice scan target is nil")
	}

	data, err := jsonDataFromSource(src)
	if err != nil {
		return fmt.Errorf("persistence: JSONStringSlice scan: %w", err)
	}
	if data == nil {
		*s = nil
		return nil
	}

	out := []string{}
	if err := json.Unmarshal(data, &out); err != nil {
		return fmt.Errorf("persistence: JSONStringSlice decode: %w", err)
	}

	*s = JSONStringSlice(out)
	return nil
}

// Value implements driver.Valuer.
func (s JSONStringSlice) Value() (driver.Value, error) {
	if s == nil {
		return nil, nil
	}
	payload, err := json.Marshal([]string(s))
	if err != nil {
		return nil, fmt.Errorf("persistence: JSONStringSlice encode: %w", err)
	}
	return payload, nil
}

func jsonDataFromSource(src any) ([]byte, error) {
	normalize := func(data []byte) []byte {
		data = bytes.TrimSpace(data)
		if len(data) == 0 {
			return nil
		}
		return data
	}

	switch typed := src.(type) {
	case nil:
		return nil, nil
	case string:
		return normalize([]byte(typed)), nil
	case []byte:
		return normalize(typed), nil
	default:
		return nil, fmt.Errorf("unsupported source type %T", src)
	}
}
