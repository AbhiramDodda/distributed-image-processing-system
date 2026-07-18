package vsearch

import (
	"container/heap"
	"fmt"
	"math"
)

// Result is one ranked neighbour: the corpus ID and its cosine similarity to the
// query, in [-1, 1] (higher is closer).
type Result struct {
	ID string `json:"id"`
	Score float32 `json:"score"`
}

// Index is an in-memory, exact cosine-similarity search structure. Vectors are
// L2-normalised at load and stored in one contiguous row-major buffer, so a query
// reduces to N dot products over a cache-friendly slice -- no per-row allocation,
// no index structure to maintain.
type Index struct {
	dim int
	ids []string
	data []float32 // len == len(ids)*dim, row i at data[i*dim:(i+1)*dim], unit-normalised
}

// NewIndex normalises and loads rows into a searchable index. Every row's vector
// must have length dim; a zero-magnitude vector cannot be normalised and is rejected.
func NewIndex(dim int, rows []Embedding) (*Index, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("vsearch: dim must be positive, got %d", dim)
	}
	ix := &Index{dim: dim, ids: make([]string, 0, len(rows)), data: make([]float32, 0, len(rows)*dim)}
	for i, r := range rows {
		if len(r.Vector) != dim {
			return nil, fmt.Errorf("vsearch: row %d (%q) has vector len %d, want dim %d", i, r.ID, len(r.Vector), dim)
		}
		unit, err := normalize(r.Vector)
		if err != nil {
			return nil, fmt.Errorf("vsearch: row %d (%q): %w", i, r.ID, err)
		}
		ix.ids = append(ix.ids, r.ID)
		ix.data = append(ix.data, unit...)
	}
	return ix, nil
}

// Len reports the number of indexed vectors.
func (ix *Index) Len() int { return len(ix.ids) }

// Dim reports the vector dimension.
func (ix *Index) Dim() int { return ix.dim }

// Lookup returns the (already unit-normalised) vector for an indexed ID. It powers
// image->image search over a corpus member without re-encoding: fetch the vector,
// search, and the query's own ID is excluded from the results by Search's caller.
func (ix *Index) Lookup(id string) ([]float32, bool) {
	for i, got := range ix.ids {
		if got == id {
			v := make([]float32, ix.dim)
			copy(v, ix.data[i*ix.dim:(i+1)*ix.dim])
			return v, true
		}
	}
	return nil, false
}

// Search returns the top-k neighbours of query by cosine similarity, most similar
// first. exclude (may be empty) drops IDs from the results -- used to omit the query
// image itself in image->image search. Ties in score break by ascending ID so the
// ranking is deterministic.
func (ix *Index) Search(query []float32, k int, exclude ...string) ([]Result, error) {
	if len(query) != ix.dim {
		return nil, fmt.Errorf("vsearch: query has len %d, want dim %d", len(query), ix.dim)
	}
	if k <= 0 {
		return nil, fmt.Errorf("vsearch: k must be positive, got %d", k)
	}
	q, err := normalize(query)
	if err != nil {
		return nil, fmt.Errorf("vsearch: query: %w", err)
	}
	skip := make(map[string]struct{}, len(exclude))
	for _, id := range exclude {
		skip[id] = struct{}{}
	}

	// A bounded min-heap of size k keeps the running top-k in O(N log k): the root is
	// the weakest kept neighbour, so a new candidate need only beat it to enter.
	h := &resultHeap{}
	for i := range ix.ids {
		if _, drop := skip[ix.ids[i]]; drop {
			continue
		}
		score := dot(q, ix.data[i*ix.dim:(i+1)*ix.dim])
		if h.Len() < k {
			heap.Push(h, Result{ID: ix.ids[i], Score: score})
		} else if less((*h)[0], Result{ID: ix.ids[i], Score: score}) {
			(*h)[0] = Result{ID: ix.ids[i], Score: score}
			heap.Fix(h, 0)
		}
	}

	// Pop drains the min-heap weakest-first; filling the slice back-to-front lands the
	// strongest neighbour at index 0. The heap's less() already encodes the tie-break.
	out := make([]Result, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(Result)
	}
	return out, nil
}

// less reports whether a ranks below b: lower score loses; equal score breaks by
// larger ID (so the min-heap root -- the element to evict -- is the weakest, and a
// final descending sort places the smaller ID first on ties).
func less(a, b Result) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.ID > b.ID
}

func normalize(v []float32) ([]float32, error) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return nil, fmt.Errorf("zero-magnitude vector cannot be normalised")
	}
	out := make([]float32, len(v))
	inv := float32(1 / norm)
	for i, x := range v {
		out[i] = x * inv
	}
	return out, nil
}

func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// resultHeap is a min-heap on the search ranking (weakest neighbour at the root).
type resultHeap []Result

func (h resultHeap) Len() int { return len(h) }
func (h resultHeap) Less(i, j int) bool { return less(h[i], h[j]) }
func (h resultHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(x any) { *h = append(*h, x.(Result)) }
func (h *resultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
