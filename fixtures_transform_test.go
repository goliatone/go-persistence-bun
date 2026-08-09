package persistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	apierrors "github.com/goliatone/go-errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type fixtureTransformAdapter struct{}

func (fixtureTransformAdapter) Transform(
	_ context.Context,
	file FixtureFile,
) (FixtureTransformResult, error) {
	return FixtureTransformResult{Data: file.Data}, nil
}

var (
	_ FixtureTransform = func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
		return FixtureTransformResult{Data: file.Data}, nil
	}
	_ FixtureTransform = fixtureTransformAdapter{}.Transform
)

func TestFixtureTransformContract(t *testing.T) {
	t.Run("runs in registration order with stable file identity", func(t *testing.T) {
		var calls []string
		fixtures := NewSeedManager(nil,
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				calls = append(calls, "first:"+file.Path+":"+file.Name+":"+string(file.Data))
				return FixtureTransformResult{Data: append(file.Data, '1')}, nil
			}),
			WithFixtureTransform(func(_ context.Context, file FixtureFile) (FixtureTransformResult, error) {
				calls = append(calls, "second:"+file.Path+":"+file.Name+":"+string(file.Data))
				return FixtureTransformResult{Data: append(file.Data, '2')}, nil
			}),
		)
		require.NoError(t, fixtures.init())

		data, skipped, err := fixtures.transformFixtureContent(
			context.Background(),
			"nested/records.JSON",
			[]byte("input"),
		)

		require.NoError(t, err)
		assert.False(t, skipped)
		assert.Equal(t, []byte("input12"), data)
		assert.Equal(t, []string{
			"first:nested/records.JSON:records.JSON:input",
			"second:nested/records.JSON:records.JSON:input1",
		}, calls)
	})

	t.Run("error takes precedence over skip and preserves the cause", func(t *testing.T) {
		cause := errors.New("adapter failed")
		fixtures := NewSeedManager(nil, WithFixtureTransform(
			func(context.Context, FixtureFile) (FixtureTransformResult, error) {
				return FixtureTransformResult{Data: []byte("ignored"), Skip: true}, cause
			},
		))
		require.NoError(t, fixtures.init())

		_, skipped, err := fixtures.transformFixtureContent(context.Background(), "source.json", []byte("input"))

		require.ErrorIs(t, err, cause)
		assert.False(t, skipped)
		var richErr *apierrors.Error
		require.ErrorAs(t, err, &richErr)
		assert.Equal(t, apierrors.CategoryOperation, richErr.Category)
		assert.Equal(t, "source.json", richErr.Metadata["file"])
		assert.Equal(t, "transform", richErr.Metadata["stage"])
		assert.Equal(t, 0, richErr.Metadata["transform_index"])
	})

	t.Run("skip stops the chain and ignores returned data", func(t *testing.T) {
		secondCalled := false
		fixtures := NewSeedManager(nil,
			WithFixtureTransform(func(context.Context, FixtureFile) (FixtureTransformResult, error) {
				return FixtureTransformResult{Data: []byte("ignored"), Skip: true}, nil
			}),
			WithFixtureTransform(func(context.Context, FixtureFile) (FixtureTransformResult, error) {
				secondCalled = true
				return FixtureTransformResult{}, nil
			}),
		)
		require.NoError(t, fixtures.init())

		data, skipped, err := fixtures.transformFixtureContent(context.Background(), "skip.json", []byte("input"))

		require.NoError(t, err)
		assert.True(t, skipped)
		assert.Nil(t, data)
		assert.False(t, secondCalled)
	})

	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "nil output", data: nil},
		{name: "empty output", data: []byte{}},
	} {
		t.Run(test.name+" is not skip", func(t *testing.T) {
			fixtures := NewSeedManager(nil, WithFixtureTransform(
				func(context.Context, FixtureFile) (FixtureTransformResult, error) {
					return FixtureTransformResult{Data: test.data}, nil
				},
			))
			require.NoError(t, fixtures.init())

			data, skipped, err := fixtures.transformFixtureContent(context.Background(), "empty.json", []byte("input"))

			require.NoError(t, err)
			assert.False(t, skipped)
			assert.Equal(t, test.data, data)
		})
	}
}

func TestFixtureTransformConfiguration(t *testing.T) {
	t.Run("nil transform fails safely during initialization", func(t *testing.T) {
		fixtures := NewSeedManager(nil, WithFixtureTransform(nil))

		err := fixtures.Load(context.Background())

		require.Error(t, err)
		var richErr *apierrors.Error
		require.ErrorAs(t, err, &richErr)
		assert.Equal(t, apierrors.CategoryBadInput, richErr.Category)
		assert.Equal(t, "configuration", richErr.Metadata["stage"])
		assert.Equal(t, 0, richErr.Metadata["transform_index"])
		assert.Nil(t, fixtures.fixture)

		err = fixtures.Load(context.Background())
		require.Error(t, err)
		assert.Len(t, fixtures.transforms, 1)
	})

	t.Run("late options are not applied after initialization", func(t *testing.T) {
		db := bun.NewDB(new(sql.DB), pgdialect.New())
		fixtures := NewSeedManager(db)
		require.NoError(t, fixtures.init())

		fixtures.AddOptions(WithFixtureTransform(fixtureTransformAdapter{}.Transform))

		assert.Empty(t, fixtures.transforms)
	})
}
