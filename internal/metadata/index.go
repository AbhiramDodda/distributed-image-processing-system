package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/storage"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS records (
    id         TEXT PRIMARY KEY,
    filename   TEXT NOT NULL,
    s3_key     TEXT NOT NULL UNIQUE,
    shard      TEXT NOT NULL,
    dataset    TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    checksum   TEXT NOT NULL DEFAULT '',
    labels     TEXT NOT NULL DEFAULT '[]',
    meta       TEXT NOT NULL DEFAULT '{}',
    tier       TEXT NOT NULL DEFAULT 'HOT',
    version    TEXT NOT NULL DEFAULT '',
    indexed_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shard_dataset ON records(shard, dataset);
CREATE INDEX IF NOT EXISTS idx_dataset        ON records(dataset);
CREATE INDEX IF NOT EXISTS idx_tier           ON records(tier);
`

type Index struct {
	db *sql.DB
}

func Open(path string) (*Index, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Index{db: db}, nil
}

func (idx *Index) Close() error { return idx.db.Close() }

func (idx *Index) Insert(ctx context.Context, r DataRecord) error {
	labels, _ := json.Marshal(r.Labels)
	meta, _ := json.Marshal(r.Meta)
	_, err := idx.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO records
		(id, filename, s3_key, shard, dataset, size_bytes, checksum, labels, meta, tier, version, indexed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.Filename, r.S3Key, r.Shard, r.Dataset,
		r.SizeBytes, r.Checksum, string(labels), string(meta),
		string(r.Tier), r.Version, r.IndexedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert record: %w", err)
	}
	return nil
}

// GetShardManifest returns all records for a shard+dataset pair.
// This is called once per worker at job start — O(log n) index lookup.
func (idx *Index) GetShardManifest(ctx context.Context, shard, dataset string) (*ShardManifest, error) {
	rows, err := idx.db.QueryContext(ctx, `
		SELECT id, filename, s3_key, shard, dataset, size_bytes, checksum, labels, meta, tier, version, indexed_at
		FROM records
		WHERE shard = ? AND dataset = ?
		ORDER BY filename`, shard, dataset)
	if err != nil {
		return nil, fmt.Errorf("query shard manifest: %w", err)
	}
	defer rows.Close()

	m := &ShardManifest{Shard: shard, Dataset: dataset}
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		m.Records = append(m.Records, r)
		m.Count++
		m.SizeBytes += r.SizeBytes
	}
	return m, rows.Err()
}

// SearchByLabel returns records matching a label substring for a dataset.
func (idx *Index) SearchByLabel(ctx context.Context, label, dataset string, limit int) ([]DataRecord, error) {
	rows, err := idx.db.QueryContext(ctx, `
		SELECT id, filename, s3_key, shard, dataset, size_bytes, checksum, labels, meta, tier, version, indexed_at
		FROM records
		WHERE dataset = ? AND labels LIKE ?
		LIMIT ?`, dataset, "%"+label+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("search by label: %w", err)
	}
	defer rows.Close()

	var records []DataRecord
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// ShardStats returns per-shard counts for a dataset.
func (idx *Index) ShardStats(ctx context.Context, dataset string) ([]ShardStats, error) {
	rows, err := idx.db.QueryContext(ctx, `
		SELECT shard, COUNT(*) as cnt, SUM(size_bytes) as total_bytes
		FROM records
		WHERE dataset = ?
		GROUP BY shard
		ORDER BY shard`, dataset)
	if err != nil {
		return nil, fmt.Errorf("shard stats: %w", err)
	}
	defer rows.Close()

	var stats []ShardStats
	for rows.Next() {
		var s ShardStats
		if err := rows.Scan(&s.Shard, &s.Count, &s.SizeBytes); err != nil {
			return nil, err
		}
		s.Labels = make(map[string]int64)
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

// DatasetStats returns aggregate stats for a dataset.
func (idx *Index) DatasetStats(ctx context.Context, dataset string) (*DatasetStats, error) {
	row := idx.db.QueryRowContext(ctx, `
		SELECT COUNT(*), SUM(size_bytes), COUNT(DISTINCT shard)
		FROM records WHERE dataset = ?`, dataset)
	s := &DatasetStats{
		Dataset:       dataset,
		LabelCount:    make(map[string]int64),
		TierBreakdown: make(map[storage.StorageTier]int64),
	}
	if err := row.Scan(&s.TotalImages, &s.TotalBytes, &s.ShardCount); err != nil {
		return nil, fmt.Errorf("dataset stats: %w", err)
	}

	trows, err := idx.db.QueryContext(ctx, `
		SELECT tier, COUNT(*) FROM records WHERE dataset = ? GROUP BY tier`, dataset)
	if err != nil {
		return nil, err
	}
	defer trows.Close()
	for trows.Next() {
		var tier string
		var cnt int64
		if err := trows.Scan(&tier, &cnt); err != nil {
			return nil, err
		}
		s.TierBreakdown[storage.StorageTier(tier)] = cnt
	}

	counts, err := idx.LabelCounts(ctx, dataset)
	if err != nil {
		return nil, err
	}
	s.LabelCount = counts
	return s, nil
}

// LabelCounts returns the frequency of each label across a dataset.
// Labels are stored as a JSON array per record, so aggregation happens in Go.
// This is an O(n) scan — acceptable for a stats endpoint, but a production
// system would maintain an inverted label->count index instead.
func (idx *Index) LabelCounts(ctx context.Context, dataset string) (map[string]int64, error) {
	rows, err := idx.db.QueryContext(ctx,
		`SELECT labels FROM records WHERE dataset = ?`, dataset)
	if err != nil {
		return nil, fmt.Errorf("label counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var labelsJSON string
		if err := rows.Scan(&labelsJSON); err != nil {
			return nil, err
		}
		var labels []string
		if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
			continue
		}
		for _, l := range labels {
			counts[l]++
		}
	}
	return counts, rows.Err()
}

// UpdateTier changes the storage tier of a record in the index.
func (idx *Index) UpdateTier(ctx context.Context, s3Key string, tier storage.StorageTier) error {
	_, err := idx.db.ExecContext(ctx,
		`UPDATE records SET tier = ? WHERE s3_key = ?`, string(tier), s3Key)
	if err != nil {
		return fmt.Errorf("update tier: %w", err)
	}
	return nil
}

// RecordsByTierAge returns records older than cutoff still on the given tier.
func (idx *Index) RecordsByTierAge(ctx context.Context, tier storage.StorageTier, before time.Time) ([]DataRecord, error) {
	rows, err := idx.db.QueryContext(ctx, `
		SELECT id, filename, s3_key, shard, dataset, size_bytes, checksum, labels, meta, tier, version, indexed_at
		FROM records
		WHERE tier = ? AND indexed_at < ?`, string(tier), before.UTC())
	if err != nil {
		return nil, fmt.Errorf("records by tier age: %w", err)
	}
	defer rows.Close()

	var records []DataRecord
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func scanRecord(rows *sql.Rows) (DataRecord, error) {
	var r DataRecord
	var labelsJSON, metaJSON, tier string
	var indexedAt string
	err := rows.Scan(
		&r.ID, &r.Filename, &r.S3Key, &r.Shard, &r.Dataset,
		&r.SizeBytes, &r.Checksum, &labelsJSON, &metaJSON,
		&tier, &r.Version, &indexedAt,
	)
	if err != nil {
		return r, fmt.Errorf("scan record: %w", err)
	}
	r.Tier = storage.StorageTier(tier)
	json.Unmarshal([]byte(labelsJSON), &r.Labels)
	json.Unmarshal([]byte(metaJSON), &r.Meta)
	r.IndexedAt, _ = time.Parse(time.RFC3339, indexedAt)
	return r, nil
}
