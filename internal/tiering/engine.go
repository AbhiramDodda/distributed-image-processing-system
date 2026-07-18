package tiering

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/metadata"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/storage"
)

type Policy struct {
	HotDays int
	WarmDays int
	ColdDays int
}

type Engine struct {
	store *storage.Client
	idx *metadata.Index
	policy Policy
	log *slog.Logger
}

func New(store *storage.Client, idx *metadata.Index, policy Policy, log *slog.Logger) *Engine {
	return &Engine{store: store, idx: idx, policy: policy, log: log}
}

// Run performs one tiering pass. Typically called nightly.
// Uses last indexed_at as a proxy for last accessed.
func (e *Engine) Run(ctx context.Context) (*Report, error) {
	now := time.Now()
	report := &Report{StartedAt: now}

	transitions := []struct {
		from storage.StorageTier
		to storage.StorageTier
		before time.Time
	}{
		{storage.TierHot, storage.TierWarm, now.AddDate(0, 0, -e.policy.HotDays)},
		{storage.TierWarm, storage.TierCold, now.AddDate(0, 0, -e.policy.WarmDays)},
		{storage.TierCold, storage.TierArchive, now.AddDate(0, 0, -e.policy.ColdDays)},
	}

	for _, t := range transitions {
		n, bytes, err := e.transition(ctx, t.from, t.to, t.before)
		if err != nil {
			return report, fmt.Errorf("transition %s->%s: %w", t.from, t.to, err)
		}
		report.Transitions = append(report.Transitions, TransitionResult{
			From: t.from,
			To: t.to,
			Count: n,
			Bytes: bytes,
		})
		report.TotalTransitioned += n
	}

	report.FinishedAt = time.Now()
	return report, nil
}

func (e *Engine) transition(ctx context.Context, from, to storage.StorageTier, before time.Time) (int64, int64, error) {
	records, err := e.idx.RecordsByTierAge(ctx, from, before)
	if err != nil {
		return 0, 0, err
	}
	var count, bytes int64
	for _, r := range records {
		if err := e.store.CopyStorageClass(ctx, r.S3Key, to.S3StorageClass()); err != nil {
			e.log.Error("copy storage class failed", "key", r.S3Key, "err", err)
			continue
		}
		if err := e.idx.UpdateTier(ctx, r.S3Key, to); err != nil {
			e.log.Error("update tier in index failed", "key", r.S3Key, "err", err)
			continue
		}
		count++
		bytes += r.SizeBytes
		e.log.Info("tiered object", "key", r.S3Key, "from", from, "to", to)
	}
	return count, bytes, nil
}

type TransitionResult struct {
	From storage.StorageTier
	To storage.StorageTier
	Count int64
	Bytes int64
}

type Report struct {
	StartedAt time.Time
	FinishedAt time.Time
	TotalTransitioned int64
	Transitions []TransitionResult
}

// CostProjection estimates monthly storage cost given bytes per tier.
// Prices are approximate US-East-1 rates per GB/month (2024).
func CostProjection(stats map[storage.StorageTier]int64) map[storage.StorageTier]float64 {
	pricePerGB := map[storage.StorageTier]float64{
		storage.TierHot: 0.023,
		storage.TierWarm: 0.0125,
		storage.TierCold: 0.004,
		storage.TierArchive: 0.00099,
	}
	out := make(map[storage.StorageTier]float64)
	for tier, bytes := range stats {
		gb := float64(bytes) / (1024 * 1024 * 1024)
		out[tier] = gb * pricePerGB[tier]
	}
	return out
}
