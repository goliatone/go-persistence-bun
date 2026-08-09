package persistence

import (
	"bytes"
	"context"
	"io/fs"
	"path"
	"time"

	apierrors "github.com/goliatone/go-errors"
)

func (s *Fixtures) transformFixtureContent(
	ctx context.Context,
	filePath string,
	data []byte,
) ([]byte, bool, error) {
	for index, transform := range s.transforms {
		if err := ctx.Err(); err != nil {
			return nil, false, fixtureStageError(err, filePath, "transform", index)
		}

		result, err := transform(ctx, FixtureFile{
			Path: filePath,
			Name: path.Base(filePath),
			Data: data,
		})
		if err != nil {
			return nil, false, fixtureStageError(err, filePath, "transform", index)
		}
		if err := ctx.Err(); err != nil {
			return nil, false, fixtureStageError(err, filePath, "transform", index)
		}
		if result.Skip {
			return nil, true, nil
		}
		data = result.Data
	}

	if err := ctx.Err(); err != nil {
		return nil, false, fixtureStageError(err, filePath, "consume", -1)
	}
	return data, false, nil
}

func (s *Fixtures) loadFixtureFile(ctx context.Context, source fs.FS, filePath string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fixtureStageError(err, filePath, "read", -1)
	}

	if len(s.transforms) == 0 {
		if err := s.fixture.Load(ctx, source, filePath); err != nil {
			return false, fixtureStageError(err, filePath, "consume", -1)
		}
		return false, nil
	}

	data, err := fs.ReadFile(source, filePath)
	if err != nil {
		return false, fixtureStageError(err, filePath, "read", -1)
	}

	data, skipped, err := s.transformFixtureContent(ctx, filePath, data)
	if err != nil || skipped {
		return skipped, err
	}

	contentFS := fixtureContentFS{name: filePath, data: data}
	if err := s.fixture.Load(ctx, contentFS, filePath); err != nil {
		return false, fixtureStageError(err, filePath, "consume", -1)
	}
	return false, nil
}

func fixtureStageError(err error, filePath, stage string, transformIndex int) error {
	metadata := map[string]any{
		"file":  filePath,
		"stage": stage,
	}
	if transformIndex >= 0 {
		metadata["transform_index"] = transformIndex
	}
	return apierrors.Wrap(err, apierrors.CategoryOperation, "fixture processing failed").
		WithMetadata(metadata)
}

func collectFixtureFailures(err error, failures *[]FixtureFailure) {
	if err == nil {
		return
	}

	if richErr, ok := err.(*apierrors.Error); ok {
		if aggregated, ok := richErr.Metadata[FixtureFailuresMetadataKey].([]FixtureFailure); ok {
			*failures = append(*failures, cloneFixtureFailures(aggregated)...)
			return
		}
		if failure, ok := fixtureFailureFromMetadata(richErr.Metadata); ok {
			*failures = append(*failures, failure)
			return
		}
	}

	if joinedErr, ok := err.(interface{ Unwrap() []error }); ok {
		for _, childErr := range joinedErr.Unwrap() {
			collectFixtureFailures(childErr, failures)
		}
		return
	}
	if wrappedErr, ok := err.(interface{ Unwrap() error }); ok {
		collectFixtureFailures(wrappedErr.Unwrap(), failures)
	}
}

func fixtureFailureFromMetadata(metadata map[string]any) (FixtureFailure, bool) {
	filePath, hasFile := metadata["file"].(string)
	stage, hasStage := metadata["stage"].(string)
	if !hasFile || !hasStage {
		return FixtureFailure{}, false
	}

	failure := FixtureFailure{File: filePath, Stage: stage}
	if transformIndex, ok := metadata["transform_index"].(int); ok {
		failure.TransformIndex = &transformIndex
	}
	return failure, true
}

func cloneFixtureFailures(failures []FixtureFailure) []FixtureFailure {
	if len(failures) == 0 {
		return nil
	}
	cloned := make([]FixtureFailure, len(failures))
	for index, failure := range failures {
		cloned[index] = failure
		if failure.TransformIndex != nil {
			transformIndex := *failure.TransformIndex
			cloned[index].TransformIndex = &transformIndex
		}
	}
	return cloned
}

type fixtureContentFS struct {
	name string
	data []byte
}

func (f fixtureContentFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) || name != f.name {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &fixtureContentFile{
		reader: bytes.NewReader(f.data),
		info: fixtureContentInfo{
			name: path.Base(name),
			size: int64(len(f.data)),
		},
	}, nil
}

type fixtureContentFile struct {
	reader *bytes.Reader
	info   fixtureContentInfo
}

func (f *fixtureContentFile) Read(p []byte) (int, error) { return f.reader.Read(p) }
func (f *fixtureContentFile) Close() error               { return nil }
func (f *fixtureContentFile) Stat() (fs.FileInfo, error) { return f.info, nil }

type fixtureContentInfo struct {
	name string
	size int64
}

func (i fixtureContentInfo) Name() string       { return i.name }
func (i fixtureContentInfo) Size() int64        { return i.size }
func (i fixtureContentInfo) Mode() fs.FileMode  { return 0o444 }
func (i fixtureContentInfo) ModTime() time.Time { return time.Time{} }
func (i fixtureContentInfo) IsDir() bool        { return false }
func (i fixtureContentInfo) Sys() any           { return nil }
