package storage

import (
	"crypto/sha256"
	"fmt"
)

const ShardCount = 256

// ShardKey returns the 2-character hex prefix (00–ff) for a filename.
// SHA-256's avalanche property ensures uniform distribution across 256 buckets.
// Each bucket gets its own S3 prefix → 256 × 3,500 req/s = ~896,000 req/s ceiling.
func ShardKey(filename string) string {
	h := sha256.Sum256([]byte(filename))
	return fmt.Sprintf("%02x", h[0])
}

// ObjectKey builds the full S3 object key: {dataset}/{shard}/{filename}
func ObjectKey(dataset, filename string) string {
	return fmt.Sprintf("%s/%s/%s", dataset, ShardKey(filename), filename)
}

// AllShards returns all 256 possible shard prefixes.
func AllShards() []string {
	shards := make([]string, ShardCount)
	for i := 0; i < ShardCount; i++ {
		shards[i] = fmt.Sprintf("%02x", i)
	}
	return shards
}

type StorageTier string

const (
	TierHot     StorageTier = "HOT"
	TierWarm    StorageTier = "WARM"
	TierCold    StorageTier = "COLD"
	TierArchive StorageTier = "ARCHIVE"
)

// S3StorageClass maps platform tiers to S3 storage class strings.
func (t StorageTier) S3StorageClass() string {
	switch t {
	case TierHot:
		return "STANDARD"
	case TierWarm:
		return "STANDARD_IA"
	case TierCold:
		return "GLACIER_IR"
	case TierArchive:
		return "DEEP_ARCHIVE"
	default:
		return "STANDARD"
	}
}
