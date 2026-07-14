package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
)

// ObjectStore is the subset of the storage client the runner needs. Declared
// here (rather than importing the concrete client) so the end-to-end task flow
// can be exercised with an in-memory fake and no MinIO.
type ObjectStore interface {
	ListPrefix(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Put(ctx context.Context, key string, r io.Reader, contentType string) error
}

// RunnerSpec is one shard task to execute in a sandbox.
type RunnerSpec struct {
	TaskID string
	JobID string
	Shard string
	Dataset string
	Image string
	Limits ResourceLimits
	Env map[string]string
	// IdempotencyKey is the task's deterministic side-effect key
	// (scheduler.SideEffectKey). It is stable across every re-execution of the same
	// logical work, so the algorithm can forward it to any downstream it mutates
	// (an idempotency-key header, a dedup column) to make those side effects
	// exactly-once despite retries and failover. Exposed to the container as
	// IdempotencyKeyEnv.
	IdempotencyKey string
}

// IdempotencyKeyEnv is the environment variable under which the task's
// deterministic idempotency key is exposed to untrusted algorithm code.
const IdempotencyKeyEnv = "PETABYTE_IDEMPOTENCY_KEY"

// RunResult is what the runner reports back to the coordinator.
type RunResult struct {
	ImagesProcessed int64
	BytesRead int64
	OutputKey string
	Execution *ExecutionResult
}

// Runner drives one task end to end: stage shard objects into a read-only
// input volume, run the untrusted algorithm image in the sandbox, then collect
// and upload its outputs. It holds no shard data itself — the sandbox is the
// only thing that touches the image bytes.
type Runner struct {
	Runtime Runtime
	Store ObjectStore
}

// Execute stages, runs, and collects one task. The input/output volumes are
// created under baseDir (a per-task scratch path, typically an emptyDir in the
// pod) and are the algorithm's only view of the filesystem.
func (r Runner) Execute(ctx context.Context, spec RunnerSpec, baseDir string) (*RunResult, error) {
	inputDir := filepath.Join(baseDir, "input")
	outputDir := filepath.Join(baseDir, "output")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return nil, fmt.Errorf("input dir: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("output dir: %w", err)
	}

	bytesRead, err := r.stageShard(ctx, spec, inputDir)
	if err != nil {
		return nil, err
	}

	// Expose the task's idempotency key to the algorithm without mutating the
	// caller's map. The algorithm forwards it to any downstream it writes so those
	// side effects dedupe across re-execution (see RunnerSpec.IdempotencyKey).
	env := spec.Env
	if spec.IdempotencyKey != "" {
		env = make(map[string]string, len(spec.Env)+1)
		maps.Copy(env, spec.Env)
		env[IdempotencyKeyEnv] = spec.IdempotencyKey
	}

	exec, execErr := r.Runtime.Run(ctx, ExecutionSpec{
		Image: spec.Image,
		TaskID: spec.TaskID,
		InputDir: inputDir,
		OutputDir: outputDir,
		Args: []string{"/input/manifest.json"},
		Env: env,
		Limits: spec.Limits,
	})
	result := &RunResult{BytesRead: bytesRead, Execution: exec}
	if execErr != nil {
		// Surface the sandbox failure; the caller records it against the task.
		return result, execErr
	}

	out, err := CollectOutput(outputDir)
	if err != nil {
		return result, err
	}
	result.ImagesProcessed = out.ImagesProcessed

	outputKey := fmt.Sprintf("results/%s/%s/%s.json", spec.JobID, spec.Shard, spec.TaskID)
	if err := r.uploadArtifacts(ctx, spec, outputDir, out.Artifacts); err != nil {
		return result, err
	}
	result.OutputKey = outputKey
	return result, nil
}

// stageShard downloads every object for the task's shard into inputDir and
// writes a manifest.json the algorithm reads as argv[1].
func (r Runner) stageShard(ctx context.Context, spec RunnerSpec, inputDir string) (int64, error) {
	prefix := fmt.Sprintf("%s/%s/", spec.Dataset, spec.Shard)
	keys, err := r.Store.ListPrefix(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("list shard %s: %w", spec.Shard, err)
	}

	var total int64
	var names []string
	for _, key := range keys {
		rc, size, err := r.Store.Get(ctx, key)
		if err != nil {
			return total, fmt.Errorf("get %s: %w", key, err)
		}
		name := key[strings.LastIndex(key, "/")+1:]
		dst := filepath.Join(inputDir, name)
		f, err := os.Create(dst)
		if err != nil {
			rc.Close()
			return total, fmt.Errorf("create %s: %w", name, err)
		}
		if _, err := io.Copy(f, rc); err != nil {
			f.Close()
			rc.Close()
			return total, fmt.Errorf("stage %s: %w", name, err)
		}
		f.Close()
		rc.Close()
		total += size
		names = append(names, name)
	}

	manifest, err := json.Marshal(map[string]any{
		"shard": spec.Shard,
		"dataset": spec.Dataset,
		"files": names,
	})
	if err != nil {
		return total, fmt.Errorf("marshal input manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "manifest.json"), manifest, 0o644); err != nil {
		return total, fmt.Errorf("write input manifest: %w", err)
	}
	return total, nil
}

// uploadArtifacts pushes each algorithm-declared output file to the results
// bucket. Paths were already checked for traversal by CollectOutput.
func (r Runner) uploadArtifacts(ctx context.Context, spec RunnerSpec, outputDir string, artifacts []string) error {
	for _, a := range artifacts {
		f, err := os.Open(filepath.Join(outputDir, a))
		if err != nil {
			return fmt.Errorf("open artifact %s: %w", a, err)
		}
		key := fmt.Sprintf("results/%s/%s/%s", spec.JobID, spec.Shard, a)
		err = r.Store.Put(ctx, key, f, "application/octet-stream")
		f.Close()
		if err != nil {
			return fmt.Errorf("upload artifact %s: %w", a, err)
		}
	}
	return nil
}
