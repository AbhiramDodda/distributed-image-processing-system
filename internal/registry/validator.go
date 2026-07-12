package registry

import (
	"fmt"
	"strings"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/sandbox"
)

// Quota is the per-tenant ceiling an algorithm's declared resources must fit
// within. It is enforced at registration time so a tenant cannot submit a job
// that would blow their allocation only to have it rejected mid-schedule.
type Quota struct {
	MaxCPUCores float64
	MaxMemoryGB int
	MaxGPUCount int
	MaxTimeoutSec int
}

// DefaultQuota is a conservative single-tenant ceiling used until Level 6 wires
// in real per-tenant quota lookups.
func DefaultQuota() Quota {
	return Quota{
		MaxCPUCores: 16,
		MaxMemoryGB: 128,
		MaxGPUCount: 4,
		MaxTimeoutSec: 3600,
	}
}

// allowedBaseImages is the set of vetted base-image prefixes. Users must build
// FROM one of these; an allowlist (rather than a blacklist) means an attacker
// cannot dodge the check by naming an image we never thought to ban.
var allowedBaseImages = []string{
	"nvidia/cuda:",
	"pytorch/pytorch:",
	"tensorflow/tensorflow:",
	"python:",
	"petabyte/base:",
}

// Validate applies tenant quota and platform policy to a manifest. It runs
// after ParsePackage (which already checked shape) and before any image build.
func Validate(m sandbox.Manifest, q Quota) error {
	if m.MemoryGB > q.MaxMemoryGB {
		return fmt.Errorf("memory_gb %d exceeds quota %d", m.MemoryGB, q.MaxMemoryGB)
	}
	if m.GPUCount > q.MaxGPUCount {
		return fmt.Errorf("gpu_count %d exceeds quota %d", m.GPUCount, q.MaxGPUCount)
	}
	if m.TimeoutSec > q.MaxTimeoutSec {
		return fmt.Errorf("timeout_s %d exceeds quota %d", m.TimeoutSec, q.MaxTimeoutSec)
	}
	if !baseImageAllowed(m.BaseImage) {
		return fmt.Errorf("base image %q not in allowlist", m.BaseImage)
	}
	return nil
}

func baseImageAllowed(image string) bool {
	for _, prefix := range allowedBaseImages {
		if strings.HasPrefix(image, prefix) {
			return true
		}
	}
	return false
}
