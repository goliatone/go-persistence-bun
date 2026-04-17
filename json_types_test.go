package persistence

import (
	"encoding/json"
	"testing"
)

func TestJSONMapValueReturnsJSONString(t *testing.T) {
	value, err := (JSONMap{
		"title": "Soap Dispenser",
		"count": 3,
	}).Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	text, ok := value.(string)
	if !ok {
		t.Fatalf("Value() type = %T, want string", value)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("Value() JSON decode error = %v", err)
	}
	if decoded["title"] != "Soap Dispenser" {
		t.Fatalf("decoded title = %#v, want Soap Dispenser", decoded["title"])
	}
}

func TestJSONStringSliceValueReturnsJSONString(t *testing.T) {
	value, err := (JSONStringSlice{"blocker", "warning", "pass"}).Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	text, ok := value.(string)
	if !ok {
		t.Fatalf("Value() type = %T, want string", value)
	}

	var decoded []string
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("Value() JSON decode error = %v", err)
	}
	if len(decoded) != 3 || decoded[0] != "blocker" {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestJSONValueNilReturnsNil(t *testing.T) {
	var object JSONMap
	value, err := object.Value()
	if err != nil {
		t.Fatalf("JSONMap.Value() error = %v", err)
	}
	if value != nil {
		t.Fatalf("JSONMap.Value() = %#v, want nil", value)
	}

	var list JSONStringSlice
	value, err = list.Value()
	if err != nil {
		t.Fatalf("JSONStringSlice.Value() error = %v", err)
	}
	if value != nil {
		t.Fatalf("JSONStringSlice.Value() = %#v, want nil", value)
	}
}
