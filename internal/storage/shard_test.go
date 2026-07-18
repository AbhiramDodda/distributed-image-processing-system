package storage_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

func TestShardKey_deterministic(t *testing.T) {
	first := storage.ShardKey("cat_007842.jpg")
	for i := 0; i < 100; i++ {
		if got := storage.ShardKey("cat_007842.jpg"); got != first {
			t.Fatalf("iteration %d: ShardKey returned %q, want %q", i, got, first)
		}
	}
}

func TestShardKey_format(t *testing.T) {
	cases := []string{"a.jpg", "test.png", "image_001.tiff", "", "very-long-filename-with-many-chars.jpeg"}
	for _, name := range cases {
		key := storage.ShardKey(name)
		if len(key) != 2 {
			t.Errorf("ShardKey(%q) = %q, want 2 chars", name, key)
		}
		if strings.ToLower(key) != key {
			t.Errorf("ShardKey(%q) = %q, want lowercase", name, key)
		}
		for _, c := range key {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Errorf("ShardKey(%q) = %q contains non-hex char %c", name, key, c)
			}
		}
	}
}

func TestShardKey_distribution(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 20000; i++ {
		seen[storage.ShardKey(fmt.Sprintf("image_%06d.jpg", i))] = true
	}
	// SHA-256's avalanche property should reach all 256 buckets within 20k samples
	if len(seen) < 256 {
		t.Fatalf("only %d unique shards seen; expected all 256", len(seen))
	}
}

func TestAllShards_countAndUniqueness(t *testing.T) {
	shards := storage.AllShards()
	if len(shards) != 256 {
		t.Fatalf("AllShards() returned %d shards, want 256", len(shards))
	}
	seen := make(map[string]bool, 256)
	for _, s := range shards {
		if seen[s] {
			t.Fatalf("duplicate shard %q", s)
		}
		if len(s) != 2 {
			t.Fatalf("shard %q has length %d, want 2", s, len(s))
		}
		seen[s] = true
	}
	// spot-check boundaries
	if !seen["00"] || !seen["ff"] {
		t.Fatal("AllShards() missing boundary values 00 or ff")
	}
}

func TestObjectKey_structure(t *testing.T) {
	key := storage.ObjectKey("train", "cat.jpg")
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		t.Fatalf("ObjectKey returned %q with %d parts, want 3", key, len(parts))
	}
	if parts[0] != "train" {
		t.Errorf("dataset part = %q, want train", parts[0])
	}
	if parts[1] != storage.ShardKey("cat.jpg") {
		t.Errorf("shard part = %q, want %q", parts[1], storage.ShardKey("cat.jpg"))
	}
	if parts[2] != "cat.jpg" {
		t.Errorf("filename part = %q, want cat.jpg", parts[2])
	}
}

func TestStorageTier_S3StorageClass(t *testing.T) {
	cases := []struct {
		tier storage.StorageTier
		class string
	}{
		{storage.TierHot, "STANDARD"},
		{storage.TierWarm, "STANDARD_IA"},
		{storage.TierCold, "GLACIER_IR"},
		{storage.TierArchive, "DEEP_ARCHIVE"},
	}
	for _, c := range cases {
		if got := c.tier.S3StorageClass(); got != c.class {
			t.Errorf("tier %q S3StorageClass() = %q, want %q", c.tier, got, c.class)
		}
	}
}
