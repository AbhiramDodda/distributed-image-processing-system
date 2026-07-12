package metadata

import (
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

// DataRecord is the per-image metadata entry stored in the index.
// Image-specific dimensions live in Meta so the platform stays domain-agnostic.
type DataRecord struct {
	ID string
	Filename string
	S3Key string
	Shard string
	Dataset string
	SizeBytes int64
	Checksum string
	Labels []string
	Meta map[string]string
	Tier storage.StorageTier
	Version string
	IndexedAt time.Time
}

type ShardManifest struct {
	Shard string
	Dataset string
	Records []DataRecord
	Count int64
	SizeBytes int64
}

type ShardStats struct {
	Shard string
	Count int64
	SizeBytes int64
	Labels map[string]int64
}

type DatasetStats struct {
	Dataset string
	TotalImages int64
	TotalBytes int64
	ShardCount int
	LabelCount map[string]int64
	TierBreakdown map[storage.StorageTier]int64
}
