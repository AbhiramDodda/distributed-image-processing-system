package sandbox

import "fmt"

// ParallelismMode declares how a user algorithm's containers relate to each
// other. The coordinator wires up the appropriate inter-container plumbing
// (shared volume, gRPC, or NCCL group) based on this value.
type ParallelismMode string

const (
	// DataParallel: one container per shard, no communication. The common
	// case for independent per-image inference.
	DataParallel ParallelismMode = "data_parallel"
	// Pipeline: containers chained A -> B -> C (e.g. preprocess -> encode -> index).
	Pipeline ParallelismMode = "pipeline"
	// AllReduce: containers share gradients via NCCL for distributed training.
	AllReduce ParallelismMode = "all_reduce"
	// MapReduce: map phase per shard, reduce phase across results.
	MapReduce ParallelismMode = "map_reduce"
)

func (m ParallelismMode) valid() bool {
	switch m {
	case DataParallel, Pipeline, AllReduce, MapReduce:
		return true
	}
	return false
}

// Manifest is the manifest.json a user ships inside their algorithm package.
// It is the contract between untrusted user code and the platform: it declares
// resource needs up front so the platform can enforce quota before ever
// building or running the image.
type Manifest struct {
	Name string `json:"name"`
	Version string `json:"version"`
	// BaseImage is the FROM line the platform expects; validated against the
	// allowlist so users cannot smuggle in an image with a known-bad shell.
	BaseImage string `json:"base_image"`
	// Entrypoint is the script invoked with the shard-manifest path as argv[1].
	Entrypoint string `json:"entrypoint"`
	GPURequired bool `json:"gpu_required"`
	GPUCount int `json:"gpu_count"`
	MemoryGB int `json:"memory_gb"`
	TimeoutSec int `json:"timeout_s"`
	Parallelism ParallelismMode `json:"parallelism"`
}

// checkShape validates internal consistency independent of any tenant quota.
// Quota enforcement lives in the registry validator; this catches malformed
// manifests early, before an image build is attempted.
func (m Manifest) checkShape() error {
	if m.Name == "" {
		return fmt.Errorf("manifest: name is required")
	}
	if m.Version == "" {
		return fmt.Errorf("manifest: version is required")
	}
	if m.Entrypoint == "" {
		return fmt.Errorf("manifest: entrypoint is required")
	}
	if m.MemoryGB <= 0 {
		return fmt.Errorf("manifest: memory_gb must be positive")
	}
	if m.TimeoutSec <= 0 {
		return fmt.Errorf("manifest: timeout_s must be positive")
	}
	if m.GPURequired && m.GPUCount < 1 {
		return fmt.Errorf("manifest: gpu_required set but gpu_count is %d", m.GPUCount)
	}
	if !m.Parallelism.valid() {
		return fmt.Errorf("manifest: unknown parallelism mode %q", m.Parallelism)
	}
	return nil
}
