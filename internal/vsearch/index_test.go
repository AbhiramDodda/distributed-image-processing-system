package vsearch

import (
	"testing"
)

// crafted 2-D corpus: points around the unit circle so nearest-neighbour order is
// obvious by angle. Magnitudes vary to prove normalisation makes ranking scale-free.
func craftedRows() []Embedding {
	return []Embedding{
		{ID: "east", Dataset: "d", Vector: []float32{1, 0}},
		{ID: "north", Dataset: "d", Vector: []float32{0, 5}}, // non-unit on purpose
		{ID: "ne", Dataset: "d", Vector: []float32{1, 1}},
		{ID: "west", Dataset: "d", Vector: []float32{-2, 0}},
		{ID: "south", Dataset: "d", Vector: []float32{0, -1}},
	}
}

func mustIndex(t *testing.T, dim int, rows []Embedding) *Index {
	t.Helper()
	ix, err := NewIndex(dim, rows)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	return ix
}

func ids(rs []Result) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

func TestSearch_ordersByCosine(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	got, err := ix.Search([]float32{1, 0}, 3) // query points east
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	// east (identical) > ne (45deg) > north/south (90deg, tie broken by ID: north<south).
	want := []string{"east", "ne", "north"}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("rank %d = %q (%v), want %q", i, got[i].ID, ids(got), want[i])
		}
	}
	if got[0].Score < got[1].Score || got[1].Score < got[2].Score {
		t.Fatalf("scores not descending: %v", got)
	}
}

func TestSearch_normalizationIsScaleFree(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	// Query north with a huge magnitude; ranking must match the unit query.
	big, err := ix.Search([]float32{0, 1000}, 5)
	if err != nil {
		t.Fatalf("Search big: %v", err)
	}
	unit, err := ix.Search([]float32{0, 1}, 5)
	if err != nil {
		t.Fatalf("Search unit: %v", err)
	}
	for i := range unit {
		if big[i].ID != unit[i].ID {
			t.Fatalf("scaled query changed ranking at %d: %v vs %v", i, ids(big), ids(unit))
		}
	}
	if big[0].ID != "north" {
		t.Fatalf("nearest to north-ish query = %q, want north", big[0].ID)
	}
}

func TestSearch_tieBreakByID(t *testing.T) {
	rows := []Embedding{
		{ID: "zebra", Vector: []float32{0, 1}},
		{ID: "apple", Vector: []float32{0, 1}}, // identical direction, same score
		{ID: "mango", Vector: []float32{0, 1}},
	}
	ix := mustIndex(t, 2, rows)
	got, err := ix.Search([]float32{0, 1}, 3)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	want := []string{"apple", "mango", "zebra"} // all tied -> ascending ID
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("tie order = %v, want %v", ids(got), want)
		}
	}
}

func TestSearch_kGreaterThanN(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	got, err := ix.Search([]float32{1, 0}, 100)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != ix.Len() {
		t.Fatalf("k>N returned %d results, want all %d", len(got), ix.Len())
	}
}

func TestSearch_exclude(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	got, err := ix.Search([]float32{1, 0}, 3, "east")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for _, r := range got {
		if r.ID == "east" {
			t.Fatalf("excluded id present: %v", ids(got))
		}
	}
	if got[0].ID != "ne" {
		t.Fatalf("with east excluded, nearest = %q, want ne", got[0].ID)
	}
}

func TestSearch_errors(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	if _, err := ix.Search([]float32{1, 0, 0}, 3); err == nil {
		t.Fatal("expected dim-mismatch error")
	}
	if _, err := ix.Search([]float32{1, 0}, 0); err == nil {
		t.Fatal("expected k<=0 error")
	}
	if _, err := ix.Search([]float32{0, 0}, 3); err == nil {
		t.Fatal("expected zero-query error")
	}
}

func TestNewIndex_rejectsBadRows(t *testing.T) {
	if _, err := NewIndex(2, []Embedding{{ID: "x", Vector: []float32{1}}}); err == nil {
		t.Fatal("expected dim-mismatch error")
	}
	if _, err := NewIndex(2, []Embedding{{ID: "z", Vector: []float32{0, 0}}}); err == nil {
		t.Fatal("expected zero-vector error")
	}
	if _, err := NewIndex(0, nil); err == nil {
		t.Fatal("expected dim<=0 error")
	}
}

func TestLookup(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	v, ok := ix.Lookup("ne")
	if !ok {
		t.Fatal("Lookup(ne) missing")
	}
	// Returned vector is unit-normalised: ne=(1,1)->(1/√2,1/√2).
	const want = float32(0.70710677)
	if abs(v[0]-want) > 1e-6 || abs(v[1]-want) > 1e-6 {
		t.Fatalf("Lookup(ne) = %v, want ~(%v,%v)", v, want, want)
	}
	if _, ok := ix.Lookup("missing"); ok {
		t.Fatal("Lookup(missing) should be absent")
	}
}

func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
