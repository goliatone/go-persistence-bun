package persistence_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing/fstest"

	persistence "github.com/goliatone/go-persistence-bun"
)

type exampleCustomerEnvelope struct {
	Data []struct {
		ExternalID  string `json:"external_id"`
		DisplayName string `json:"display_name"`
	} `json:"data"`
}

func exampleCustomerAdapter(
	ctx context.Context,
	file persistence.FixtureFile,
) (persistence.FixtureTransformResult, error) {
	if err := ctx.Err(); err != nil {
		return persistence.FixtureTransformResult{}, err
	}
	if file.Name != "customers.json" {
		return persistence.FixtureTransformResult{Skip: true}, nil
	}

	var envelope exampleCustomerEnvelope
	if err := json.Unmarshal(file.Data, &envelope); err != nil {
		return persistence.FixtureTransformResult{}, fmt.Errorf("decode customer envelope: %w", err)
	}
	rows := make([]map[string]any, 0, len(envelope.Data))
	for _, customer := range envelope.Data {
		rows = append(rows, map[string]any{
			"external_id": customer.ExternalID,
			"name":        customer.DisplayName,
		})
	}
	data, err := json.Marshal([]map[string]any{{"model": "Customer", "rows": rows}})
	if err != nil {
		return persistence.FixtureTransformResult{}, fmt.Errorf("encode customer fixture: %w", err)
	}
	return persistence.FixtureTransformResult{Data: data}, nil
}

func ExampleWithFixtureTransform() {
	source := fstest.MapFS{
		"customers.json": {Data: []byte(`{"data":[]}`)},
	}

	manager := persistence.NewSeedManager(nil,
		persistence.WithFS(source),
		persistence.WithFixtureTransform(exampleCustomerAdapter),
	)

	_ = manager // Register models and use a real *bun.DB before calling Load.

	// Output:
}
