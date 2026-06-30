package metadata_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/metadata"
	"github.com/abhiramd/petabyte-platform/internal/storage"
)

func openTestIndex(t *testing.T) *metadata.Index {
	t.Helper()
	dir := t.TempDir()
	idx, err := metadata.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open index: %v", err)
	}
	t.Cleanup(func() { idx.Close() })
	return idx
}

func makeRecord(id, filename, shard, dataset string, tier storage.StorageTier, age time.Duration) metadata.DataRecord {
	return metadata.DataRecord{
		ID:        id,
		Filename:  filename,
		S3Key:     dataset + "/" + shard + "/" + filename,
		Shard:     shard,
		Dataset:   dataset,
		SizeBytes: 1024,
		Checksum:  "abc123",
		Labels:    []string{"cat", "animal"},
		Meta:      map[string]string{"width": "224"},
		Tier:      tier,
		IndexedAt: time.Now().Add(-age),
	}
}

func TestIndex_insertAndGetShardManifest(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()

	r1 := makeRecord("id1", "cat001.jpg", "a3", "train", storage.TierHot, 0)
	r2 := makeRecord("id2", "cat002.jpg", "a3", "train", storage.TierHot, 0)
	r3 := makeRecord("id3", "dog001.jpg", "f0", "train", storage.TierHot, 0)

	for _, r := range []metadata.DataRecord{r1, r2, r3} {
		if err := idx.Insert(ctx, r); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	m, err := idx.GetShardManifest(ctx, "a3", "train")
	if err != nil {
		t.Fatalf("GetShardManifest: %v", err)
	}
	if m.Count != 2 {
		t.Errorf("manifest count = %d, want 2", m.Count)
	}
	if m.SizeBytes != 2048 {
		t.Errorf("manifest size = %d, want 2048", m.SizeBytes)
	}
	if m.Shard != "a3" || m.Dataset != "train" {
		t.Errorf("manifest shard/dataset = %q/%q", m.Shard, m.Dataset)
	}
}

func TestIndex_getShardManifest_emptyReturnsEmpty(t *testing.T) {
	idx := openTestIndex(t)
	m, err := idx.GetShardManifest(context.Background(), "zz", "nonexistent")
	if err != nil {
		t.Fatalf("GetShardManifest: %v", err)
	}
	if m.Count != 0 {
		t.Errorf("empty shard manifest count = %d, want 0", m.Count)
	}
}

func TestIndex_searchByLabel(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()

	r1 := makeRecord("id1", "cat.jpg", "aa", "train", storage.TierHot, 0)
	r1.Labels = []string{"cat", "animal"}
	r2 := makeRecord("id2", "dog.jpg", "bb", "train", storage.TierHot, 0)
	r2.Labels = []string{"dog", "animal"}
	r3 := makeRecord("id3", "car.jpg", "cc", "train", storage.TierHot, 0)
	r3.Labels = []string{"car", "vehicle"}

	for _, r := range []metadata.DataRecord{r1, r2, r3} {
		idx.Insert(ctx, r)
	}

	results, err := idx.SearchByLabel(ctx, "animal", "train", 10)
	if err != nil {
		t.Fatalf("SearchByLabel: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("SearchByLabel(animal) = %d results, want 2", len(results))
	}

	results, err = idx.SearchByLabel(ctx, "cat", "train", 10)
	if err != nil {
		t.Fatalf("SearchByLabel: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("SearchByLabel(cat) = %d results, want 1", len(results))
	}

	results, err = idx.SearchByLabel(ctx, "notfound", "train", 10)
	if err != nil {
		t.Fatalf("SearchByLabel: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("SearchByLabel(notfound) = %d results, want 0", len(results))
	}
}

func TestIndex_searchByLabel_limitsResults(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		r := makeRecord(
			os.DevNull+string(rune('a'+i)),
			filepath.Join("img", string(rune('a'+i))+".jpg"),
			"aa", "ds", storage.TierHot, 0,
		)
		r.ID = string(rune('a' + i))
		r.S3Key = "ds/aa/" + string(rune('a'+i)) + ".jpg"
		r.Labels = []string{"shared"}
		idx.Insert(ctx, r)
	}
	results, _ := idx.SearchByLabel(ctx, "shared", "ds", 3)
	if len(results) != 3 {
		t.Errorf("limit=3 returned %d results", len(results))
	}
}

func TestIndex_shardStats(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		r := makeRecord(
			string(rune('a'+i)), string(rune('a'+i))+".jpg",
			"aa", "train", storage.TierHot, 0,
		)
		r.S3Key = "train/aa/" + string(rune('a'+i)) + ".jpg"
		idx.Insert(ctx, r)
	}
	for i := 0; i < 3; i++ {
		r := makeRecord(
			string(rune('p'+i)), string(rune('p'+i))+".jpg",
			"bb", "train", storage.TierHot, 0,
		)
		r.S3Key = "train/bb/" + string(rune('p'+i)) + ".jpg"
		idx.Insert(ctx, r)
	}

	stats, err := idx.ShardStats(ctx, "train")
	if err != nil {
		t.Fatalf("ShardStats: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("ShardStats returned %d shards, want 2", len(stats))
	}
	counts := make(map[string]int64)
	for _, s := range stats {
		counts[s.Shard] = s.Count
	}
	if counts["aa"] != 5 {
		t.Errorf("shard aa count = %d, want 5", counts["aa"])
	}
	if counts["bb"] != 3 {
		t.Errorf("shard bb count = %d, want 3", counts["bb"])
	}
}

func TestIndex_updateTierAndRecordsByAge(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()

	old := makeRecord("old1", "old.jpg", "aa", "ds", storage.TierHot, 60*24*time.Hour) // 60 days old
	new := makeRecord("new1", "new.jpg", "bb", "ds", storage.TierHot, 0)
	idx.Insert(ctx, old)
	idx.Insert(ctx, new)

	cutoff := time.Now().Add(-30 * 24 * time.Hour) // 30 days ago
	records, err := idx.RecordsByTierAge(ctx, storage.TierHot, cutoff)
	if err != nil {
		t.Fatalf("RecordsByTierAge: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("RecordsByTierAge returned %d records, want 1", len(records))
	}
	if records[0].Filename != "old.jpg" {
		t.Errorf("expected old.jpg to be returned, got %q", records[0].Filename)
	}

	// Transition old.jpg to WARM
	if err := idx.UpdateTier(ctx, old.S3Key, storage.TierWarm); err != nil {
		t.Fatalf("UpdateTier: %v", err)
	}

	// Should no longer appear in HOT tier
	records, _ = idx.RecordsByTierAge(ctx, storage.TierHot, cutoff)
	if len(records) != 0 {
		t.Errorf("after UpdateTier, old.jpg still appears in HOT: got %d records", len(records))
	}

	// Should appear in WARM tier
	records, _ = idx.RecordsByTierAge(ctx, storage.TierWarm, cutoff)
	if len(records) != 1 || records[0].Filename != "old.jpg" {
		t.Errorf("old.jpg not found in WARM tier after UpdateTier")
	}
}

func TestIndex_datasetStats(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()

	// 3 records across 2 shards, 2 tiers
	r1 := makeRecord("id1", "a.jpg", "00", "train", storage.TierHot, 0)
	r1.SizeBytes = 100
	r2 := makeRecord("id2", "b.jpg", "00", "train", storage.TierHot, 0)
	r2.SizeBytes = 200
	r3 := makeRecord("id3", "c.jpg", "ff", "train", storage.TierWarm, 0)
	r3.SizeBytes = 300
	for _, r := range []metadata.DataRecord{r1, r2, r3} {
		if err := idx.Insert(ctx, r); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	stats, err := idx.DatasetStats(ctx, "train")
	if err != nil {
		t.Fatalf("DatasetStats: %v", err)
	}
	if stats.TotalImages != 3 {
		t.Errorf("TotalImages = %d, want 3", stats.TotalImages)
	}
	if stats.TotalBytes != 600 {
		t.Errorf("TotalBytes = %d, want 600", stats.TotalBytes)
	}
	if stats.ShardCount != 2 {
		t.Errorf("ShardCount = %d, want 2", stats.ShardCount)
	}
	if stats.TierBreakdown[storage.TierHot] != 2 {
		t.Errorf("HOT count = %d, want 2", stats.TierBreakdown[storage.TierHot])
	}
	if stats.TierBreakdown[storage.TierWarm] != 1 {
		t.Errorf("WARM count = %d, want 1", stats.TierBreakdown[storage.TierWarm])
	}
}

func TestIndex_labelCounts(t *testing.T) {
	idx := openTestIndex(t)
	ctx := context.Background()

	mk := func(id string, labels ...string) metadata.DataRecord {
		r := makeRecord(id, id+".jpg", "00", "train", storage.TierHot, 0)
		r.S3Key = "train/00/" + id + ".jpg"
		r.Labels = labels
		return r
	}
	idx.Insert(ctx, mk("a", "cat", "animal"))
	idx.Insert(ctx, mk("b", "dog", "animal"))
	idx.Insert(ctx, mk("c", "cat", "pet"))
	// record in a different dataset must not leak into counts
	other := mk("d", "cat", "animal")
	other.Dataset = "test"
	other.S3Key = "test/00/d.jpg"
	idx.Insert(ctx, other)

	counts, err := idx.LabelCounts(ctx, "train")
	if err != nil {
		t.Fatalf("LabelCounts: %v", err)
	}
	want := map[string]int64{"cat": 2, "animal": 2, "dog": 1, "pet": 1}
	for label, n := range want {
		if counts[label] != n {
			t.Errorf("label %q count = %d, want %d", label, counts[label], n)
		}
	}
	if len(counts) != len(want) {
		t.Errorf("got %d distinct labels, want %d (%v)", len(counts), len(want), counts)
	}

	// DatasetStats must carry the same counts
	stats, _ := idx.DatasetStats(ctx, "train")
	if stats.LabelCount["cat"] != 2 {
		t.Errorf("DatasetStats label cat = %d, want 2", stats.LabelCount["cat"])
	}
}

func TestIndex_labelCounts_emptyDataset(t *testing.T) {
	idx := openTestIndex(t)
	counts, err := idx.LabelCounts(context.Background(), "nope")
	if err != nil {
		t.Fatalf("LabelCounts on empty dataset: %v", err)
	}
	if len(counts) != 0 {
		t.Errorf("empty dataset returned %d labels, want 0", len(counts))
	}
}

func TestIndex_insertIsDurable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "durable.db")

	idx, _ := metadata.Open(dbPath)
	r := makeRecord("id1", "persist.jpg", "aa", "train", storage.TierHot, 0)
	idx.Insert(context.Background(), r)
	idx.Close()

	// Reopen the same file
	idx2, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen index: %v", err)
	}
	defer idx2.Close()

	m, err := idx2.GetShardManifest(context.Background(), "aa", "train")
	if err != nil {
		t.Fatalf("GetShardManifest after reopen: %v", err)
	}
	if m.Count != 1 {
		t.Errorf("after reopen: manifest count = %d, want 1", m.Count)
	}
}
