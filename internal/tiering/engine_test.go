package tiering_test

import (
	"testing"

	"github.com/abhiramd/petabyte-platform/internal/storage"
	"github.com/abhiramd/petabyte-platform/internal/tiering"
)

func TestCostProjection_zero(t *testing.T) {
	out := tiering.CostProjection(map[storage.StorageTier]int64{})
	if len(out) != 0 {
		t.Errorf("empty input returned %d tiers, want 0", len(out))
	}
}

func TestCostProjection_hotTierOnly(t *testing.T) {
	const oneGiB = 1024 * 1024 * 1024
	out := tiering.CostProjection(map[storage.StorageTier]int64{
		storage.TierHot: oneGiB,
	})
	// $0.023 per GiB
	want := 0.023
	if out[storage.TierHot] != want {
		t.Errorf("hot 1 GiB cost = %f, want %f", out[storage.TierHot], want)
	}
}

func TestCostProjection_archiveCheaperThanHot(t *testing.T) {
	const oneGiB = 1024 * 1024 * 1024
	out := tiering.CostProjection(map[storage.StorageTier]int64{
		storage.TierHot:     oneGiB,
		storage.TierArchive: oneGiB,
	})
	if out[storage.TierArchive] >= out[storage.TierHot] {
		t.Errorf("archive (%f) should be cheaper than hot (%f)", out[storage.TierArchive], out[storage.TierHot])
	}
}

func TestCostProjection_petabyteScale(t *testing.T) {
	const onePiB = 1024 * 1024 * 1024 * 1024 * 1024
	out := tiering.CostProjection(map[storage.StorageTier]int64{
		storage.TierHot:     onePiB,
		storage.TierWarm:    onePiB,
		storage.TierCold:    onePiB,
		storage.TierArchive: onePiB,
	})
	// Verify all four tiers produce a positive cost
	for _, tier := range []storage.StorageTier{storage.TierHot, storage.TierWarm, storage.TierCold, storage.TierArchive} {
		if out[tier] <= 0 {
			t.Errorf("tier %q cost = %f, want > 0", tier, out[tier])
		}
	}
	// Verify cost ordering: HOT > WARM > COLD > ARCHIVE
	if !(out[storage.TierHot] > out[storage.TierWarm] &&
		out[storage.TierWarm] > out[storage.TierCold] &&
		out[storage.TierCold] > out[storage.TierArchive]) {
		t.Errorf("cost ordering violated: hot=%f warm=%f cold=%f archive=%f",
			out[storage.TierHot], out[storage.TierWarm], out[storage.TierCold], out[storage.TierArchive])
	}
}

func TestCostProjection_linearScaling(t *testing.T) {
	const oneGiB = 1024 * 1024 * 1024
	one := tiering.CostProjection(map[storage.StorageTier]int64{storage.TierHot: oneGiB})
	ten := tiering.CostProjection(map[storage.StorageTier]int64{storage.TierHot: 10 * oneGiB})
	if ten[storage.TierHot] != 10*one[storage.TierHot] {
		t.Errorf("cost not linear: 10x bytes = %f, want %f", ten[storage.TierHot], 10*one[storage.TierHot])
	}
}
